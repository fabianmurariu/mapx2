use std::hash::BuildHasher;
use std::io;
use std::marker::PhantomData;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use rustc_hash::FxBuildHasher;

use crate::byte_store::{MMapFile, VecStore};
use crate::columnar_entries::ColumnarEntries;
use crate::double_columnar_entries::{ColumnarSlotIdx, DoubleColumnarEntries};
use crate::entry::Entry;
use crate::error::Result;
use crate::heap::HeapOps;
use crate::types::{BytesActual, BytesDecode, BytesEncode, Native, Str};
use crate::{ByteStore, Heap};

// Type aliases for common use cases
pub type U64StringMap<BS = VecStore> = DiskHashMap<Native<u64>, Str, BS>;
pub type StringU64Map<BS = VecStore> = DiskHashMap<Str, Native<u64>, BS>;
pub type StringStringMap<BS = VecStore> = DiskHashMap<Str, Str, BS>;

/// Entry API for the HashMap, similar to std::collections::HashMap
pub enum MapEntry<'a, K, V, BS, S = FxBuildHasher>
where
    BS: ByteStore,
    S: BuildHasher,
{
    Occupied(OccupiedEntry<'a, K, V, BS, S>),
    Vacant(VacantEntry<'a, K, V, BS, S>),
}

impl<K, V, BS, S> MapEntry<'_, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher + Default,
    Heap<BS>: HeapOps<BS>,
{
    /// Returns true if the entry is occupied
    pub fn is_occupied(&self) -> bool {
        matches!(self, MapEntry::Occupied(_))
    }

    /// Returns true if the entry is vacant
    pub fn is_vacant(&self) -> bool {
        matches!(self, MapEntry::Vacant(_))
    }

    pub fn key(&self) -> <K as BytesDecode<'_>>::DItem
    where
        K: for<'a> BytesDecode<'a>,
    {
        let k = match self {
            MapEntry::Occupied(entry) => <K as BytesDecode>::bytes_decode(entry.key_bytes()),
            MapEntry::Vacant(entry) => <K as BytesDecode>::bytes_decode(&entry.key),
        };
        k.expect("Failed to decode key")
    }
}

/// A view into an occupied entry in the map
pub struct OccupiedEntry<'a, K, V, BS, S = FxBuildHasher>
where
    BS: ByteStore,
    S: BuildHasher,
{
    map: &'a mut DiskHashMap<K, V, BS, S>,
    slot_idx: ColumnarSlotIdx,
    entry: Entry,
}

/// A view into a vacant entry in the map
pub struct VacantEntry<'a, K, V, BS, S = FxBuildHasher>
where
    BS: ByteStore,
    S: BuildHasher,
{
    map: &'a mut DiskHashMap<K, V, BS, S>,
    key: Vec<u8>, // rethink this to be &[u8] with correct lifetime different from the map
    key_len: Option<usize>,
}

/// This is an open address hash map implementation with trait-based encoding/decoding.
/// It takes any types that implement BytesEncode/BytesDecode as key and value.
/// It is designed to be used with a backing store that implements `ByteStore` trait,
/// allowing for flexible storage options (in-memory with VecStore or persistent with MMapFile).
///
/// Uses Robin Hood hashing with columnar storage for improved cache performance
/// and supports 85% load factor (up from 50%).
#[derive(Debug)]
pub struct DiskHashMap<K, V, BS, S = FxBuildHasher>
where
    BS: ByteStore,
    S: BuildHasher,
{
    entries: DoubleColumnarEntries<BS>,
    heap: Heap<BS>,
    hasher: S,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for DiskHashMap<K, V, VecStore, FxBuildHasher> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, BS, S> DiskHashMap<K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
    Heap<BS>: HeapOps<BS>,
{
    /// Returns the number of key-value pairs in the map
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the map contains no elements
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the current capacity of the map
    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// Returns the load factor of the map (size / capacity)
    pub fn load_factor(&self) -> f64 {
        if self.capacity() == 0 {
            return f64::INFINITY;
        }
        self.len() as f64 / self.capacity() as f64
    }

    /// Returns a reference to the entries storage (for internal use by iterators)
    pub(crate) fn entries(&self) -> &DoubleColumnarEntries<BS> {
        &self.entries
    }

    /// Returns an iterator over the key-value pairs of the map.
    pub fn iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(<K as BytesDecode<'a>>::DItem, <V as BytesDecode<'a>>::DItem)>> + 'a
    where
        K: for<'b> BytesDecode<'b>,
        V: for<'b> BytesDecode<'b>,
        Heap<BS>: HeapOps<BS>,
    {
        self.entries.iter().map(|(_, entry)| {
            let key_bytes = self
                .heap
                .get(entry.key_pos())
                .expect("key must exist for occupied entry");
            let value_bytes = self
                .heap
                .get(entry.value_pos())
                .expect("value must exist for occupied entry");
            let key = K::bytes_decode(key_bytes)?;
            let value = V::bytes_decode(value_bytes)?;
            Ok((key, value))
        })
    }

    /// Returns an iterator over the keys of the map.
    pub fn keys(&self) -> impl Iterator<Item = Result<<K as BytesDecode<'_>>::DItem>> + '_
    where
        K: for<'a> BytesDecode<'a>,
        V: for<'a> BytesDecode<'a>,
        Heap<BS>: HeapOps<BS>,
    {
        self.iter().map(|res| res.map(|(k, _)| k))
    }

    /// Returns an iterator over the values of the map.
    pub fn values(&self) -> impl Iterator<Item = Result<<V as BytesDecode<'_>>::DItem>> + '_
    where
        K: for<'a> BytesDecode<'a>,
        V: for<'a> BytesDecode<'a>,
        Heap<BS>: HeapOps<BS>,
    {
        self.iter().map(|res| res.map(|(_, v)| v))
    }

    /// Check if resizing is needed based on load factor (85% threshold)
    fn should_resize(&self) -> bool {
        self.entries.should_resize()
    }

    fn insert_key_into_heap(
        &mut self,
        key_bytes: &[u8],
        key_len: Option<usize>,
    ) -> Result<crate::HeapIdx> {
        let key_idx = if let Some(key_len) = key_len {
            let mut page = self
                .heap
                .next_free_page(size_of::<usize>() + key_bytes.len());

            page.write_with_len(key_len, key_bytes)?;
            page.flush()?;
            page.pos()
        } else {
            let mut page = self.heap.next_free_page(key_bytes.len());
            page.write(key_bytes)?; // Store 0 length for empty key
            page.flush()?;
            page.pos()
        };
        Ok(key_idx)
    }

    fn insert_value_into_heap(
        &mut self,
        value_bytes: &[u8],
        value_len: Option<usize>,
    ) -> Result<crate::HeapIdx> {
        let value_idx = if let Some(value_len) = value_len {
            let mut page = self
                .heap
                .next_free_page(size_of::<usize>() + value_bytes.len());

            page.write_with_len(value_len, value_bytes)?;
            page.flush()?;
            page.pos()
        } else {
            let mut page = self.heap.next_free_page(value_bytes.len());
            page.write(value_bytes)?; // the decoder will work out the length, no need to track it
            page.flush()?;
            page.pos()
        };
        Ok(value_idx)
    }
}

impl<
    K: for<'a> BytesEncode<'a> + for<'a> BytesDecode<'a>,
    V: for<'a> BytesEncode<'a> + for<'a> BytesDecode<'a>,
    BS: ByteStore,
    S: BuildHasher,
> DiskHashMap<K, V, BS, S>
where
    Heap<BS>: HeapOps<BS>,
{
    fn grow(&mut self) -> Result<()> {
        let new_capacity = if self.capacity() == 0 {
            16
        } else {
            self.capacity() * 2
        };

        // Use incremental resizing with double array implementation
        if self.entries.has_old_entries() {
            // Already in the middle of a resize - this should not happen since we
            // double capacity and rehash 4 entries per insert, which should complete
            // before triggering another resize
            panic!("Resize triggered while already resizing - should not be reachable");
        }

        self.entries.grow(new_capacity)?;
        Ok(())
    }

    /// Compute hash for a key
    fn hash_key(&self, key_bytes: &[u8]) -> u64 {
        let mut hasher = self.hasher.build_hasher();
        <K as BytesEncode>::hash_alt(key_bytes, &mut hasher)
    }

    /// Compare two keys for equality
    fn keys_eq(&self, key1: &[u8], key2: &[u8]) -> bool {
        <K as BytesEncode>::eq_alt(key1, key2)
    }

    /// Insert a key-value pair into the map using the trait-based API
    pub fn insert<'a, 'b>(
        &'b mut self,
        key: &'a <K as BytesEncode<'a>>::EItem,
        value: &'a <V as BytesEncode<'a>>::EItem,
    ) -> Result<Option<<V as BytesDecode<'b>>::DItem>> {
        if self.should_resize() {
            self.grow()?;
        }

        let (key_len, key_bytes) = K::bytes_encode(key)?;
        let (value_len, value_bytes) = V::bytes_encode(value)?;

        self.insert_key_value_bytes(&key_bytes, key_len, &value_bytes, value_len)
    }

    /// Common insertion logic for key-value pairs using raw bytes
    fn insert_key_value_bytes(
        &mut self,
        key_bytes: &[u8],
        key_len: Option<usize>,
        value_bytes: &[u8],
        value_len: Option<usize>,
    ) -> Result<Option<<V as BytesDecode<'_>>::DItem>> {
        // Store key and value in heap first
        let key_idx = self.insert_key_into_heap(key_bytes, key_len)?;
        let value_idx = self.insert_value_into_heap(value_bytes, value_len)?;

        let hash = self.hash_key(key_bytes);
        let entry = Entry::occupied_at_pos(key_idx, value_idx);

        // Separate borrows: first get references we need, then call insert
        // We need to use raw pointers or restructure to avoid borrow issues
        let heap = &self.heap;
        let hasher = &self.hasher;

        // Create closures that capture references to heap and hasher
        // Note: eq_alt expects (raw_bytes, heap_bytes_with_len_prefix) for second arg
        // Since both e1 and e2 are from heap, we extract actual bytes from both
        let key_eq = |e1: &Entry, e2: &Entry| {
            let k1 = heap.get(e1.key_pos()).expect("key must exist");
            let k2 = heap.get(e2.key_pos()).expect("key must exist");
            // Both keys are from heap with length prefix, extract actual bytes
            <K as BytesActual>::bytes_actual(k1) == <K as BytesActual>::bytes_actual(k2)
        };

        let rehash_fn = |e: &Entry| {
            let k = heap.get(e.key_pos()).expect("key must exist");
            let mut h = hasher.build_hasher();
            <K as BytesEncode>::hash_alt(<K as BytesActual>::bytes_actual(k), &mut h)
        };

        // Insert using Robin Hood hashing
        let old_entry = self.entries.insert(hash, entry, key_eq, rehash_fn);

        // If there was an old entry, decode and return its value
        if let Some(old_entry) = old_entry {
            let old_value_bytes = self
                .heap
                .get(old_entry.value_pos())
                .expect("value must exist for occupied entry");
            let old_value = V::bytes_decode(old_value_bytes)?;
            Ok(Some(old_value))
        } else {
            Ok(None)
        }
    }

    /// Find an entry by key
    fn find_entry_inner(&self, key_bytes: &[u8]) -> Option<(ColumnarSlotIdx, Entry)> {
        let hash = self.hash_key(key_bytes);

        self.entries.find(hash, |entry| {
            let entry_key_bytes = self.heap.get(entry.key_pos()).expect("key must exist");
            // eq_alt expects (raw_key, heap_key_with_len_prefix) - it extracts actual bytes from second arg
            self.keys_eq(key_bytes, entry_key_bytes)
        })
    }

    /// Get a value by key using the trait-based API
    pub fn get<'a>(
        &self,
        key: &'a <K as BytesEncode<'a>>::EItem,
    ) -> Result<Option<<V as BytesDecode<'_>>::DItem>> {
        self.find_entry(key)?.map_or(Ok(None), |entry| {
            let value_bytes = self
                .heap
                .get(entry.value_pos())
                .expect("value must exist for occupied entry");
            V::bytes_decode(value_bytes).map(Some)
        })
    }

    pub fn get_key(&self, e: &Entry) -> Result<<K as BytesDecode<'_>>::DItem> {
        let key_bytes = self
            .heap
            .get(e.key_pos())
            .expect("key must exist for occupied entry");
        let key = K::bytes_decode(key_bytes)?;

        Ok(key)
    }

    pub fn get_value(&self, e: &Entry) -> Result<<V as BytesDecode<'_>>::DItem> {
        let value_bytes = self
            .heap
            .get(e.value_pos())
            .expect("value must exist for occupied entry");
        let value = V::bytes_decode(value_bytes)?;

        Ok(value)
    }

    pub fn find_entry<'a>(&self, key: &'a <K as BytesEncode<'a>>::EItem) -> Result<Option<Entry>> {
        if self.is_empty() {
            return Ok(None);
        }

        let (_, key_bytes) = K::bytes_encode(key)?;
        Ok(self.find_entry_inner(&key_bytes).map(|(_, entry)| entry))
    }

    /// Get an entry for the given key using trait-based API
    pub fn entry<'a>(
        &'a mut self,
        key: &'a <K as BytesEncode<'a>>::EItem,
    ) -> Result<MapEntry<'a, K, V, BS, S>>
    where
        for<'b> K: BytesEncode<'b>,
        for<'b> V: BytesDecode<'b>,
    {
        let (key_len, key_bytes) = K::bytes_encode(key)?;
        Ok(self.entry_raw(key_len, key_bytes.as_ref()))
    }

    /// Get an entry for the given key, allowing for efficient insertion/access patterns
    fn entry_raw<Q: AsRef<[u8]>>(
        &mut self,
        key_len: Option<usize>,
        key: Q,
    ) -> MapEntry<'_, K, V, BS, S>
    where
        for<'a> K: BytesEncode<'a>,
        for<'b> V: BytesDecode<'b>,
    {
        if self.should_resize() {
            let _ = self.grow();
        }

        let key_bytes = key.as_ref();
        match self.find_entry_inner(key_bytes) {
            Some((slot_idx, entry)) => MapEntry::Occupied(OccupiedEntry {
                map: self,
                entry,
                slot_idx,
            }),
            None => MapEntry::Vacant(VacantEntry {
                map: self,
                key: key_bytes.to_vec(),
                key_len,
            }),
        }
    }
}

impl<K, V, S: BuildHasher + Default> DiskHashMap<K, V, VecStore, S> {
    /// Creates a new in-memory HashMap
    pub fn new() -> Self {
        const DEFAULT_CAP: usize = 16;

        let heap = Heap::new_in_memory();

        // Create columnar entries with default capacity
        let bytes_needed = ColumnarEntries::<VecStore>::bytes_needed(DEFAULT_CAP);
        let mut store = VecStore::with_capacity(bytes_needed);
        store.grow(bytes_needed);
        let entries = ColumnarEntries::new(store, DEFAULT_CAP);

        Self {
            heap,
            entries: DoubleColumnarEntries::new(entries),
            hasher: S::default(),
            _marker: PhantomData,
        }
    }
}

impl<K, V, S> DiskHashMap<K, V, MMapFile, S>
where
    S: BuildHasher + Default,
{
    pub fn new_in(path: impl AsRef<Path>) -> io::Result<Self> {
        const DEFAULT_ENTRIES_CAP: usize = 16;

        let path = path.as_ref();
        let heap = Heap::new(path.join("heap"))?;

        // Create columnar entries
        let bytes_needed = ColumnarEntries::<MMapFile>::bytes_needed(DEFAULT_ENTRIES_CAP);
        let store = MMapFile::new(path.join("entries"), bytes_needed)?;
        let entries = ColumnarEntries::new(store, DEFAULT_ENTRIES_CAP);

        Ok(Self {
            heap,
            entries: DoubleColumnarEntries::new(entries),
            hasher: S::default(),
            _marker: PhantomData,
        })
    }

    /// Creates a new HashMap with specified capacities, rounding up to nearest power of 2
    pub fn with_capacity(
        path: impl AsRef<Path>,
        num_entries: usize,
        slots_per_slab: usize,
        max_bytes: Option<usize>,
    ) -> io::Result<Self> {
        let path = path.as_ref();

        // Ensure none of the capacities are zero
        if num_entries == 0 || slots_per_slab == 0 || max_bytes == Some(0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Capacities must be greater than zero",
            ));
        }

        // Round up to nearest power of 2
        let capacity = num_entries.next_power_of_two();
        let heap = Heap::new_with_capacity(path.join("heap"), slots_per_slab, max_bytes)?;

        // Create columnar entries
        let bytes_needed = ColumnarEntries::<MMapFile>::bytes_needed(capacity);
        let store = MMapFile::new(path.join("entries"), bytes_needed)?;
        let entries = ColumnarEntries::new(store, capacity);

        Ok(Self {
            heap,
            entries: DoubleColumnarEntries::new(entries),
            hasher: S::default(),
            _marker: PhantomData,
        })
    }

    pub fn load_from(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let heap = Heap::load_from(path.join("heap"))?;

        // Try to find all entries files to detect if we were in the middle of a resize
        let mut entries_files = Self::find_all_entries_files(path)?;

        if entries_files.len() == 1 {
            // Single entries file - normal case
            let entries_path = &entries_files[0];
            let store = MMapFile::from_file(entries_path)?;
            let entries = ColumnarEntries::from_existing(store);

            Ok(Self {
                heap,
                entries: DoubleColumnarEntries::new(entries),
                hasher: S::default(),
                _marker: PhantomData,
            })
        } else {
            // sort by size then take 2, assert there are exactly 2 files
            assert_eq!(entries_files.len(), 2, "Expected exactly 2 entries files");
            entries_files.sort_by_key(|path| {
                let meta = std::fs::metadata(path)
                    .unwrap_or_else(|_| panic!("File {path:?} does not exist"));
                meta.size()
            });

            let oldest_entries_path = &entries_files[0];
            let oldest_store = MMapFile::from_file(oldest_entries_path)?;
            let oldest_entries = ColumnarEntries::from_existing(oldest_store);

            let latest_entries_path = &entries_files[1];
            let latest_store = MMapFile::from_file(latest_entries_path)?;
            let latest_entries = ColumnarEntries::from_existing(latest_store);

            Ok(Self {
                heap,
                entries: DoubleColumnarEntries::new_with_old(oldest_entries, latest_entries),
                hasher: S::default(),
                _marker: PhantomData,
            })
        }
    }

    /// Helper function to find all entries files in a directory
    fn find_all_entries_files(path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut entries_files = Vec::new();

        // Check for the original "entries" file
        let entries_path = path.join("entries");
        if entries_path.exists() {
            entries_files.push(entries_path);
        }

        // Check for numbered entries files (entries_1.bin, entries_2.bin, etc.)
        if let Ok(dir) = std::fs::read_dir(path) {
            for entry in dir {
                let entry = entry?;
                let file_path = entry.path();
                if let Some(filename) = file_path.file_name().and_then(|s| s.to_str()) {
                    if filename.starts_with("entries_") && filename.ends_with(".bin") {
                        entries_files.push(file_path);
                    }
                }
            }
        }

        // Sort by file size (smaller = older entries file)
        entries_files.sort_by_key(|path| {
            std::fs::metadata(path).map(|m| m.size()).unwrap_or(0)
        });

        if entries_files.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "No entries files found",
            ));
        }

        Ok(entries_files)
    }
}

#[allow(clippy::needless_lifetimes)]
impl<'a, K, V, BS, S> OccupiedEntry<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher + Default,
    Heap<BS>: HeapOps<BS>,
{
    /// Get a reference to the key in the entry
    fn key_bytes(&self) -> &[u8] {
        self.map
            .heap
            .get(self.entry.key_pos())
            .expect("key must exist for occupied entry")
    }

    /// Get a reference to the value in the entry
    fn value_bytes(&self) -> &[u8] {
        self.map
            .heap
            .get(self.entry.value_pos())
            .expect("value must exist for occupied entry")
    }
}

// Trait-based extensions for OccupiedEntry
#[allow(clippy::needless_lifetimes)]
impl<'a, K, V, BS, S> OccupiedEntry<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher + Default,
    K: for<'b> BytesEncode<'b> + for<'b> BytesDecode<'b>,
    V: for<'b> BytesEncode<'b> + for<'b> BytesDecode<'b>,
    Heap<BS>: HeapOps<BS>,
{
    /// Get the value in the entry using the trait-based API
    pub fn value(&self) -> Result<<V as BytesDecode<'_>>::DItem> {
        let value_bytes = self.value_bytes();
        V::bytes_decode(value_bytes)
    }

    pub fn key(&self) -> Result<<K as BytesDecode<'_>>::DItem> {
        let key_bytes = self.key_bytes();
        K::bytes_decode(key_bytes)
    }

    /// Insert the value into the occupied entry using the trait-based API
    /// Returns the old value
    pub fn insert(
        self,
        value: &'a <V as BytesEncode<'a>>::EItem,
    ) -> Result<<V as BytesDecode<'a>>::DItem> {
        // Get the old value position first
        let old_value_pos = self.entry.value_pos();

        // Insert new value into heap
        let (value_len, value_bytes) = V::bytes_encode(value)?;
        let new_value_idx = self.map.insert_value_into_heap(&value_bytes, value_len)?;

        // Update the entry with new value position
        let new_entry = self.entry.with_v_pos(new_value_idx);
        self.map.entries.set_entry(self.slot_idx, new_entry);

        // Now decode the old value
        let old_value_bytes = self.map.heap.get(old_value_pos).expect("old value must exist");
        V::bytes_decode(old_value_bytes)
    }

    /// Insert the value into the entry using trait-based API if occupied
    pub fn or_insert(
        self,
        value: &'a <V as BytesEncode<'a>>::EItem,
    ) -> Result<<V as BytesDecode<'a>>::DItem> {
        self.insert(value)
    }

    /// Insert the value returned by the closure if the entry is occupied using trait-based API
    pub fn or_insert_with<F>(self, f: F) -> Result<<V as BytesDecode<'a>>::DItem>
    where
        F: FnOnce() -> &'a <V as BytesEncode<'a>>::EItem,
    {
        self.insert(f())
    }
}

// Trait-based extensions for VacantEntry
impl<'a, K, V, BS, S> VacantEntry<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher + Default,
    K: for<'b> BytesEncode<'b> + for<'b> BytesDecode<'b>,
    V: for<'b> BytesEncode<'b> + for<'b> BytesDecode<'b>,
    Heap<BS>: HeapOps<BS>,
{
    /// Insert the value into the vacant entry using the trait-based API
    /// Returns the inserted value
    pub fn insert(
        self,
        value: &'a <V as BytesEncode<'a>>::EItem,
    ) -> Result<<V as BytesDecode<'a>>::DItem> {
        let (value_len, value_bytes) = V::bytes_encode(value)?;
        let key_bytes = self.key.clone();

        // Use the map's insert method which handles everything
        self.map.insert_key_value_bytes(&key_bytes, self.key_len, &value_bytes, value_len)?;

        // Find the entry we just inserted to get the value from heap
        let (_, entry) = self.map.find_entry_inner(&key_bytes).expect("just inserted entry must exist");
        let heap_value_bytes = self.map.heap.get(entry.value_pos()).expect("value must exist");
        V::bytes_decode(heap_value_bytes)
    }

    /// Insert the value into the vacant entry using trait-based API if vacant
    pub fn or_insert(
        self,
        value: &'a <V as BytesEncode<'a>>::EItem,
    ) -> Result<<V as BytesDecode<'a>>::DItem> {
        self.insert(value)
    }

    /// Insert the value returned by the closure if the entry is vacant using trait-based API
    pub fn or_insert_with<F>(self, f: F) -> Result<<V as BytesDecode<'a>>::DItem>
    where
        F: FnOnce() -> &'a <V as BytesEncode<'a>>::EItem,
    {
        self.insert(f())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "rkyv")]
    use crate::types::rkyv::Arch;
    use crate::types::{Native, Str};
    use crate::{Bytes, VecStore};
    use proptest::prelude::*;
    #[cfg(feature = "rkyv")]
    use rkyv::{Archive, Deserialize, Serialize};
    use rustc_hash::FxBuildHasher;
    use std::collections::HashMap as StdHashMap;
    use tempfile::tempdir;

    type BytesHM = DiskHashMap<Bytes, Bytes, VecStore, FxBuildHasher>;
    type DiskBytesHM = DiskHashMap<Bytes, Bytes, MMapFile, FxBuildHasher>;

    // Legacy tests using raw byte API for backward compatibility
    #[test]
    fn test_insert_and_get_raw() {
        let mut map: BytesHM = DiskHashMap::new();

        // Insert a key-value pair using raw API
        map.insert(b"hello", b"world").unwrap();

        // Get the value using raw API
        let value = map.get(b"hello").unwrap();
        assert_eq!(value, Some(b"world".as_ref()));

        // Test non-existent key
        let value = map.get(b"not_found").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_update_value_raw() {
        let mut map: BytesHM = DiskHashMap::new();

        // Insert a key-value pair
        map.insert(b"key", b"value1").unwrap();
        assert_eq!(map.get(b"key").unwrap(), Some(b"value1".as_ref()));

        // Update the value
        let old_value = map.insert(b"key", b"value2").unwrap();
        assert_eq!(old_value, Some(b"value1".as_ref()));

        // Get the updated value
        let value = map.get(b"key").unwrap();
        assert_eq!(value, Some(b"value2".as_ref()));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_multiple_entries_raw() {
        let mut map: BytesHM = DiskHashMap::new();

        // Insert multiple key-value pairs
        map.insert(b"key1", b"value1").unwrap();
        map.insert(b"key2", b"value2").unwrap();
        map.insert(b"key3", b"value3").unwrap();

        // Get values
        assert_eq!(map.get(b"key1").unwrap(), Some(b"value1".as_ref()));
        assert_eq!(map.get(b"key2").unwrap(), Some(b"value2".as_ref()));
        assert_eq!(map.get(b"key3").unwrap(), Some(b"value3".as_ref()));
    }

    #[test]
    fn test_empty_map() {
        let map: BytesHM = DiskHashMap::new();

        // Map should be empty
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());

        // Get on empty map
        assert_eq!(map.get(b"key").unwrap(), None);
    }

    fn check_prop(hm: StdHashMap<Vec<u8>, Vec<u8>>) {
        println!("Length of input hashmap: {}", hm.len());
        let temp_dir = tempdir().unwrap();
        let mut map: DiskBytesHM = DiskHashMap::new_in(&temp_dir).unwrap();

        // Insert all key-value pairs from the StdHashMap
        let mut already_inserted = vec![];
        for (k, v) in hm.iter() {
            map.insert(k, v).unwrap();
            already_inserted.push((k.clone(), v.clone()));
            for (k, v) in &already_inserted {
                assert_eq!(map.get(k).unwrap(), Some(v.as_slice()), "key: {k:?}");

                let entry = map.entry(k).unwrap();
                assert!(entry.is_occupied(), "Expected occupied entry {k:?}:{v:?}");
                assert_eq!(entry.key(), k);
                match entry {
                    MapEntry::Occupied(occupied) => {
                        assert_eq!(occupied.value().unwrap(), v);
                        assert_eq!(occupied.key().unwrap(), k);
                    }
                    MapEntry::Vacant(_) => panic!("Expected occupied entry"),
                }
            }

            // Verify iterator returns exactly the inserted items at this point
            let mut iter_items = std::collections::HashSet::new();
            for result in map.iter() {
                let (key_bytes, value_bytes) = result.unwrap();
                iter_items.insert((key_bytes.to_vec(), value_bytes.to_vec()));
            }

            // Convert already_inserted to HashSet for comparison
            let expected_items: std::collections::HashSet<_> =
                already_inserted.iter().cloned().collect();

            // Verify iterator has exactly the same items as inserted so far
            assert_eq!(
                iter_items.len(),
                expected_items.len(),
                "Iterator count mismatch after inserting {} items",
                already_inserted.len()
            );

            // Check every item from iterator exists in expected
            for (key, value) in &iter_items {
                assert!(
                    expected_items.contains(&(key.clone(), value.clone())),
                    "Iterator returned unexpected item: key={:?}, value={:?} after {} insertions",
                    key,
                    value,
                    already_inserted.len()
                );
            }

            // Check every expected item exists in iterator results
            for (key, value) in &expected_items {
                assert!(
                    iter_items.contains(&(key.clone(), value.clone())),
                    "Iterator missing expected item: key={:?}, value={:?} after {} insertions",
                    key,
                    value,
                    already_inserted.len()
                );
            }
        }

        let check_fn = |map: &DiskBytesHM,
                        hm: &StdHashMap<Vec<u8>, Vec<u8>>,
                        already_inserted: &Vec<(Vec<u8>, Vec<u8>)>| {
            // Check the size of the map
            assert_eq!(map.len(), hm.len());

            // Check that all values can be retrieved
            for (k, v) in hm.iter() {
                assert_eq!(
                    map.get(k.as_slice()).unwrap(),
                    Some(v.as_slice()),
                    "key: {k:?}"
                );
            }

            // Verify iterator returns exactly the inserted items
            let mut iter_items = std::collections::HashSet::new();
            for result in map.iter() {
                let (key_bytes, value_bytes) = result.unwrap();
                iter_items.insert((key_bytes.to_vec(), value_bytes.to_vec()));
            }

            // Convert already_inserted to HashSet for comparison
            let expected_items: std::collections::HashSet<_> =
                already_inserted.into_iter().collect();

            // Verify iterator has exactly the same items as inserted
            assert_eq!(
                iter_items.len(),
                expected_items.len(),
                "Iterator count mismatch"
            );

            // Check every item from iterator exists in expected
            for (key, value) in &iter_items {
                assert!(
                    expected_items.contains(&(key.clone(), value.clone())),
                    "Iterator returned unexpected item: key={:?}, value={:?}",
                    key,
                    value
                );
            }

            // Check every expected item exists in iterator results
            for (key, value) in &expected_items {
                assert!(
                    iter_items.contains(&(key.clone(), value.clone())),
                    "Iterator missing expected item: key={:?}, value={:?}",
                    key,
                    value
                );
            }
        };

        check_fn(&map, &hm, &already_inserted);
        drop(map);

        let map = DiskHashMap::load_from(&temp_dir).unwrap();
        check_fn(&map, &hm, &already_inserted);
    }

    fn check_prop_native(hm: StdHashMap<u64, u64>) {
        let mut map: DiskHashMap<Native<u64>, Native<u64>, VecStore, FxBuildHasher> =
            DiskHashMap::new();

        // Insert all key-value pairs from the StdHashMap
        let mut already_inserted = vec![];
        for (k, v) in hm.iter() {
            map.insert(k, v).unwrap();
            already_inserted.push((*k, *v));
            for (k, v) in &already_inserted {
                assert_eq!(map.get(k).unwrap(), Some(*v), "key: {k:?}");

                let entry = map.entry(k).unwrap();
                assert!(entry.is_occupied());
                assert_eq!(entry.key(), *k);
                match entry {
                    MapEntry::Occupied(occupied) => {
                        assert_eq!(occupied.value().unwrap(), *v);
                        assert_eq!(occupied.key().unwrap(), *k)
                    }
                    MapEntry::Vacant(_) => panic!("Expected occupied entry"),
                }
            }

            // Verify iterator returns exactly the inserted items at this point
            let mut iter_items = std::collections::HashSet::new();
            for result in map.iter() {
                let (key, value) = result.unwrap();
                iter_items.insert((key, value));
            }

            // Convert already_inserted to HashSet for comparison
            let expected_items: std::collections::HashSet<_> =
                already_inserted.iter().cloned().collect();

            // Verify iterator has exactly the same items as inserted so far
            assert_eq!(
                iter_items.len(),
                expected_items.len(),
                "Iterator count mismatch after inserting {} items",
                already_inserted.len()
            );

            // Check every item from iterator exists in expected
            for (key, value) in &iter_items {
                assert!(
                    expected_items.contains(&(*key, *value)),
                    "Iterator returned unexpected item: key={:?}, value={:?} after {} insertions",
                    key,
                    value,
                    already_inserted.len()
                );
            }

            // Check every expected item exists in iterator results
            for (key, value) in &expected_items {
                assert!(
                    iter_items.contains(&(*key, *value)),
                    "Iterator missing expected item: key={:?}, value={:?} after {} insertions",
                    key,
                    value,
                    already_inserted.len()
                );
            }
        }

        // Check the size of the map
        assert_eq!(map.len(), hm.len());

        // Check that all values can be retrieved
        for (k, v) in hm.iter() {
            assert_eq!(map.get(k).unwrap(), Some(*v), "key: {k:?}");
        }

        // Verify iterator returns exactly the inserted items
        let mut iter_items = std::collections::HashSet::new();
        for result in map.iter() {
            let (key, value) = result.unwrap();
            iter_items.insert((key, value));
        }

        // Convert already_inserted to HashSet for comparison
        let expected_items: std::collections::HashSet<_> = already_inserted.into_iter().collect();

        // Verify iterator has exactly the same items as inserted
        assert_eq!(
            iter_items.len(),
            expected_items.len(),
            "Iterator count mismatch"
        );

        // Check every item from iterator exists in expected
        for (key, value) in &iter_items {
            assert!(
                expected_items.contains(&(*key, *value)),
                "Iterator returned unexpected item: key={:?}, value={:?}",
                key,
                value
            );
        }

        // Check every expected item exists in iterator results
        for (key, value) in &expected_items {
            assert!(
                iter_items.contains(&(*key, *value)),
                "Iterator missing expected item: key={:?}, value={:?}",
                key,
                value
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]
        #[test]
        fn it_s_a_hash_disk_map(
                small_hash_map_prop in proptest::collection::hash_map(
                    proptest::collection::vec(0u8..255, 1..32),
                    proptest::collection::vec(0u8..255, 1..32),
                    10..500,

            )){ check_prop(small_hash_map_prop); }
    }

    #[test]
    fn it_s_a_hash_disk_map_0() {
        let mut expected = StdHashMap::new();
        expected.insert(vec![2], vec![0]);
        expected.insert(vec![6], vec![0]);
        expected.insert(vec![7], vec![0]);
        expected.insert(vec![4], vec![0]);
        expected.insert(vec![9], vec![0]);
        expected.insert(vec![10], vec![0]);
        expected.insert(vec![11], vec![0]);
        expected.insert(vec![3], vec![0]);
        expected.insert(vec![12], vec![0]);
        expected.insert(vec![5], vec![0]);
        expected.insert(vec![0], vec![0]);
        expected.insert(vec![8], vec![0]);
        expected.insert(vec![1], vec![0]);
        check_prop(expected);
    }

    #[test]
    fn it_s_a_hash_map_native() {
        let small_hash_map_prop = proptest::collection::hash_map(
            proptest::num::u64::ANY,
            proptest::num::u64::ANY,
            1..250,
        );

        proptest!(|(values in small_hash_map_prop)|{
            check_prop_native(values);
        });
    }

    #[test]
    fn it_s_a_hash_map_native_0() {
        let mut hm = StdHashMap::new();
        hm.insert(0u64, 1u64);
        check_prop_native(hm);
    }

    #[test]
    fn it_s_a_hash_map_0() {
        let mut hm = StdHashMap::new();
        hm.insert(vec![0u8], vec![0u8]);
        check_prop(hm);
    }

    #[test]
    fn it_s_a_hash_map_1() {
        let mut expected = StdHashMap::new();
        expected.insert(vec![225, 211, 10, 64, 102, 152], vec![173, 231, 92]);
        expected.insert(vec![227, 209, 20, 158, 58, 22, 107, 62], vec![77]);
        expected.insert(
            vec![140, 134, 67, 127, 34, 190],
            vec![144, 189, 239, 135, 30],
        );
        expected.insert(vec![206, 143, 221], vec![253, 107, 93, 29, 207]);
        expected.insert(vec![182, 46, 63, 120], vec![110, 233, 124, 103]);
        check_prop(expected);
    }

    #[test]
    fn it_s_a_hash_map_3() {
        let mut expected = StdHashMap::new();
        expected.insert(vec![0], vec![0]);
        expected.insert(vec![1], vec![0]);
        expected.insert(vec![2], vec![0]);
        expected.insert(vec![3], vec![0]);
        expected.insert(vec![4], vec![0]);
        expected.insert(vec![5], vec![0]);
        expected.insert(vec![6], vec![0]);
        check_prop(expected);
    }

    #[test]
    fn it_s_a_hash_map_2() {
        let mut expected = StdHashMap::new();
        let kvs = vec![
            (vec![6], vec![0]),
            (vec![214], vec![252]),
            (vec![44], vec![0]),
            (vec![113], vec![160]),
            (vec![116], vec![15]),
            (vec![67], vec![42]),
            (vec![12], vec![0]),
            (vec![191], vec![172]),
            (vec![209], vec![119]),
            (vec![11], vec![0]),
            (vec![254], vec![104]),
            (vec![121], vec![0]),
            (vec![117], vec![174]),
            (vec![38], vec![79]),
            (vec![94], vec![66]),
            (vec![16], vec![0]),
            (vec![89], vec![167]),
            (vec![112], vec![195]),
            (vec![91], vec![18]),
            (vec![23], vec![0]),
            (vec![58], vec![0]),
            (vec![32], vec![118]),
            (vec![198], vec![47]),
            (vec![18], vec![0]),
            (vec![120], vec![0]),
            (vec![0], vec![0]),
            (vec![24], vec![0]),
            (vec![7], vec![0]),
            (vec![15], vec![0]),
            (vec![22], vec![0]),
            (vec![13], vec![0]),
            (vec![102], vec![182]),
            (vec![253], vec![68]),
            (vec![139], vec![250]),
            (vec![43], vec![0]),
            (vec![14], vec![0]),
            (vec![8], vec![0]),
            (vec![88], vec![175]),
            (vec![195], vec![150]),
            (vec![41], vec![0]),
            (vec![5], vec![46]),
            (vec![10], vec![0]),
            (vec![119], vec![0]),
            (vec![239], vec![34]),
            (vec![17], vec![0]),
            (vec![42], vec![0]),
            (vec![40], vec![213]),
            (vec![1], vec![0]),
            (vec![9], vec![0]),
            (vec![140], vec![14]),
            (vec![31], vec![51]),
            (vec![57], vec![154]),
            (vec![19], vec![102]),
            (vec![238], vec![198]),
            (vec![129], vec![15]),
            (vec![141], vec![0]),
            (vec![33], vec![0]),
            (vec![95], vec![74]),
            (vec![21], vec![162]),
        ];

        for (k, v) in kvs {
            expected.insert(k, v);
        }

        check_prop(expected);
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        type FileMap = DiskHashMap<Bytes, Bytes, MMapFile, FxBuildHasher>;

        // 1. Create a new map and add some data
        {
            let mut map: FileMap = FileMap::new_in(path).unwrap();
            map.insert(b"key1", b"value1").unwrap();
            map.insert(b"key2", b"value2").unwrap();
            assert_eq!(map.len(), 2);
            assert_eq!(map.get(b"key1").unwrap(), Some(b"value1".as_ref()));
            assert_eq!(map.get(b"key2").unwrap(), Some(b"value2".as_ref()));
        } // map is dropped, files should be persisted

        // 2. Load the map from disk
        {
            let map: FileMap = FileMap::load_from(path).unwrap();
            assert_eq!(map.len(), 2);
            assert_eq!(map.get(b"key1").unwrap(), Some(b"value1".as_ref()));
            assert_eq!(map.get(b"key2").unwrap(), Some(b"value2".as_ref()));
            assert_eq!(map.get(b"key3").unwrap(), None);
        }

        // 3. Load again, and add more data
        {
            let mut map: FileMap = FileMap::load_from(path).unwrap();
            map.insert(b"key3", b"value3").unwrap();
            assert_eq!(map.len(), 3);
            assert_eq!(map.get(b"key3").unwrap(), Some(b"value3".as_ref()));
        }

        // 4. Load one more time to check the new data is there
        {
            let map: FileMap = FileMap::load_from(path).unwrap();
            assert_eq!(map.len(), 3);
            assert_eq!(map.get(b"key1").unwrap(), Some(b"value1".as_ref()));
            assert_eq!(map.get(b"key2").unwrap(), Some(b"value2".as_ref()));
            assert_eq!(map.get(b"key3").unwrap(), Some(b"value3".as_ref()));
        }
    }

    // #[test]
    // fn test_no_resize_with_preallocation() {
    //     let mut entry_store = VecStore::new();
    //     entry_store.grow(256 * std::mem::size_of::<Entry>());
    //     let mut key_store = VecStore::new();
    //     key_store.grow(20 * 1024);
    //     let mut value_store = VecStore::new();
    //     value_store.grow(20 * 1024);

    //     // The stores have been resized once to pre-allocate space.
    //     assert_eq!(entry_store.stats(), 1);
    //     assert_eq!(key_store.stats(), 1);
    //     assert_eq!(value_store.stats(), 1);

    //     let mut map: DiskHashMap<Bytes, Bytes, _, FxBuildHasher> =
    //         DiskHashMap::with_stores(entry_store, key_store, value_store);

    //     let initial_stats = map.stats();
    //     assert_eq!(initial_stats, (1, 1, 1));

    //     // Insert 100 elements. Should not trigger any more resizes.
    //     for i in 0..100 {
    //         let s = i.to_string();
    //         map.insert(s.clone().into_bytes().as_slice(), s.into_bytes().as_slice())
    //             .unwrap();
    //     }
    //     assert_eq!(
    //         map.stats(),
    //         initial_stats,
    //         "No resize should happen with pre-allocation"
    //     );

    //     // Insert more elements to trigger a resize of the entries container.
    //     for i in 100..150 {
    //         let s = i.to_string();
    //         map.insert(s.clone().into_bytes().as_slice(), s.into_bytes().as_slice())
    //             .unwrap();
    //     }

    //     let (entries_resizes, keys_resizes, values_resizes) = map.stats();
    //     assert_eq!(
    //         entries_resizes, 0,
    //         "entries store is replaced, so stats are reset"
    //     );
    //     assert_eq!(
    //         keys_resizes, initial_stats.1,
    //         "keys store should not resize"
    //     );
    //     assert_eq!(
    //         values_resizes, initial_stats.2,
    //         "values store should not resize"
    //     );
    // }

    #[test]
    fn test_entry_api_vacant() {
        let mut map: BytesHM = DiskHashMap::new();

        // Test vacant entry insertion
        match map.entry(b"key1").unwrap() {
            MapEntry::Vacant(entry) => {
                let value_ref = entry.insert(b"value1").unwrap();
                assert_eq!(value_ref, b"value1");
            }
            MapEntry::Occupied(_) => panic!("Expected vacant entry"),
        }

        assert_eq!(map.len(), 1);
        assert_eq!(map.get(b"key1").unwrap(), Some(b"value1".as_ref()));
    }

    #[test]
    fn test_entry_api_occupied() {
        let mut map: BytesHM = DiskHashMap::new();

        // Insert initial value
        map.insert(b"key1", b"value1").unwrap();

        // Test occupied entry access and update
        match map.entry(b"key1").unwrap() {
            MapEntry::Occupied(entry) => {
                assert_eq!(entry.value().unwrap(), b"value1");
                let old_value = entry.insert(b"value2").unwrap();
                assert_eq!(old_value, b"value1");
            }
            MapEntry::Vacant(_) => panic!("Expected occupied entry"),
        }

        assert_eq!(map.len(), 1);
        assert_eq!(map.get(b"key1").unwrap(), Some(b"value2".as_ref()));
    }

    #[test]
    fn test_entry_api_or_insert() {
        let mut map: BytesHM = DiskHashMap::new();

        // Test or_insert with vacant entry
        match map.entry(b"key1").unwrap() {
            MapEntry::Vacant(entry) => {
                let value_ref = entry.or_insert(b"value1").unwrap();
                assert_eq!(value_ref, b"value1");
            }
            MapEntry::Occupied(_) => panic!("Expected vacant entry"),
        }

        // Test entry with existing key (should not create occupied entry in this test)
        assert_eq!(map.get(b"key1").unwrap(), Some(b"value1".as_ref()));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_entry_api_or_insert_with() {
        let mut map: BytesHM = DiskHashMap::new();

        // Test or_insert_with with vacant entry
        match map.entry(b"key1").unwrap() {
            MapEntry::Vacant(entry) => {
                let value_ref = entry.or_insert_with(|| b"computed_value").unwrap();
                assert_eq!(value_ref, b"computed_value");
            }
            MapEntry::Occupied(_) => panic!("Expected vacant entry"),
        }

        assert_eq!(map.get(b"key1").unwrap(), Some(b"computed_value".as_ref()));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_insert_returns_previous_value() {
        let mut map: BytesHM = DiskHashMap::new();

        // First insert should return None
        let previous = map.insert(b"key1", b"value1").unwrap();
        assert_eq!(previous, None);

        // Second insert should return previous value
        let previous = map.insert(b"key1", b"value2").unwrap();
        assert_eq!(previous, Some(b"value1".as_ref()));

        // Verify current value
        assert_eq!(map.get(b"key1").unwrap(), Some(b"value2".as_ref()));
        assert_eq!(map.len(), 1);
    }

    // New trait-based API tests
    #[test]
    fn test_native_u64_str_string() {
        let mut map: DiskHashMap<Native<u64>, Str, VecStore> = DiskHashMap::new();

        // Insert a key-value pair
        let result = map.insert(&42, "hello");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        // Get the value
        let result = map.get(&42u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("hello"));

        // Update the value
        let result = map.insert(&42u64, "world");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("hello"));

        // Verify updated value
        let result = map.get(&42u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("world"));

        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_str_string_native_u32() {
        let mut map: DiskHashMap<Str, Native<u32>, VecStore> = DiskHashMap::new();

        // Insert multiple pairs
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();

        let result = map.insert(&key1, &100u32);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        let result = map.insert(&key2, &200u32);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        // Verify both values
        let result = map.get(&key1);
        assert!(result.is_ok(), "Failed to get key1 {result:?}");
        assert_eq!(result.unwrap(), Some(100u32));

        let result = map.get(&key2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(200u32));

        assert_eq!(map.len(), 2);
    }

    #[test]
    #[ignore = "not supporting size 1 byte for now"]
    fn test_capacity_and_growth() {
        let mut map: DiskHashMap<Native<u8>, Native<u8>, VecStore> = DiskHashMap::new();

        // Insert enough items to trigger growth
        for i in 0u8..20 {
            let result = map.insert(&i, &(i * 2));
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), None);
        }

        assert_eq!(map.len(), 20);

        // Verify all values are still accessible
        for i in 0u8..20 {
            let result = map.get(&i);
            assert!(result.is_ok(), "Failed to get key {i}");
            assert_eq!(result.unwrap(), Some(i * 2));
        }
    }

    #[test]
    fn test_convenience_methods() {
        // Test U64StringMap
        let mut map: DiskHashMap<Native<u64>, Str, VecStore> = U64StringMap::new();

        let result = map.insert(&42, "hello");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        let result = map.get(&42u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("hello"));

        // Test StringU64Map
        let mut map2: StringU64Map = StringU64Map::new();

        let result = map2.insert("key", &100);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        let result = map2.get("key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(100));

        // Test StringStringMap
        let mut map3: StringStringMap = StringStringMap::new();

        let result = map3.insert("key", "value");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        let result = map3.get("key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("value"));
    }

    /// A complex data structure that we want to store and retrieve efficiently
    #[cfg(feature = "rkyv")]
    #[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq)]
    pub struct UserProfile {
        pub id: u32,
        pub name: String,
        pub tags: Vec<String>,
        pub scores: Vec<f64>,
        pub metadata: Vec<(String, String)>,
    }

    #[cfg(feature = "rkyv")]
    impl UserProfile {
        fn new(id: u32, name: &str) -> Self {
            Self {
                id,
                name: name.to_string(),
                tags: vec!["user".to_string(), "active".to_string()],
                scores: vec![85.5, 92.1, 78.3],
                metadata: vec![
                    ("created".to_string(), "2024-01-15".to_string()),
                    ("last_login".to_string(), "2024-01-20".to_string()),
                ],
            }
        }

        /// Serialize this UserProfile to bytes using rkyv
        fn to_bytes(&self) -> Vec<u8> {
            rkyv::to_bytes::<rkyv::rancor::Error>(self)
                .unwrap()
                .to_vec()
        }

        /// Deserialize from bytes without copying (zero-copy)
        fn from_bytes(bytes: &[u8]) -> Result<&rkyv::Archived<UserProfile>> {
            Ok(rkyv::access::<
                rkyv::Archived<UserProfile>,
                rkyv::rancor::Error,
            >(bytes)?)
        }
    }

    #[cfg(feature = "rkyv")]
    #[test]
    fn simple_mmap_only_no_hash_map_rkyv_zerocopy() {
        let tmp_file = tempfile::NamedTempFile::new().expect("Failed to create temp dir");

        // Create a UserProfile instance
        let check_alignment = |offset: usize| {
            let user = UserProfile::new(1, "Integration Test");

            // Serialize to bytes using rkyv
            let user_bytes = user.to_bytes();

            // Create a memory-mapped file and write the bytes
            let mut mmap_file = MMapFile::new(&tmp_file, 1024).expect("Failed to create mmap file");
            let aligned_range = offset..user_bytes.len() + offset;
            let items = &mut mmap_file.as_mut()[aligned_range.clone()];
            dbg!(align_of_val(items));
            dbg!(align_of_val(&user_bytes));
            items.copy_from_slice(&user_bytes);

            // Read back the bytes from the mmap file
            let read_bytes = mmap_file.as_ref();

            // Deserialize without copying (zero-copy)
            let archived_user = UserProfile::from_bytes(&read_bytes[aligned_range])
                .expect("Failed to deserialize UserProfile from bytes");

            assert_eq!(archived_user.id, 1);
            assert_eq!(archived_user.name, "Integration Test");
        };

        check_alignment(0);
        check_alignment(8); // Check with 8-byte alignment
        check_alignment(16); // Check with 16-byte alignment
        check_alignment(32); // Check with 16-byte alignment
        // check_alignment(1) this will fail, as it is not aligned
    }

    #[cfg(feature = "rkyv")]
    #[test]
    fn archived_map() {
        let tempdir = tempfile::tempdir().expect("Failed to create temp dir");

        let mut map: DiskHashMap<Native<u64>, Arch<UserProfile>, MMapFile, FxBuildHasher> =
            DiskHashMap::new_in(tempdir.path()).unwrap();

        let user = UserProfile::new(1, "Integration Test");

        map.insert(&3, &user)
            .expect("Failed to insert user profile into the map");

        let user = map
            .get(&3)
            .expect("Failed to retrieve user profile from the map")
            .expect("User profile not found in the map");

        assert_eq!(user.id, 1);
        assert_eq!(user.name, "Integration Test");
    }

    #[test]
    fn test_with_capacity() {
        let tempdir = tempfile::tempdir().expect("Failed to create temp dir");

        // Test with valid capacities
        let map_result: io::Result<DiskHashMap<Bytes, Bytes, MMapFile, FxBuildHasher>> =
            DiskHashMap::with_capacity(tempdir.path(), 8, 16, Some(4096));
        assert!(map_result.is_ok());

        let map = map_result.unwrap();
        // Capacity should be rounded up to power of 2: 8 -> 8 (already power of 2)
        assert_eq!(map.capacity(), 8);

        // Test that we can actually use the map
        drop(map);
        let mut map: DiskHashMap<Bytes, Bytes, MMapFile, FxBuildHasher> =
            DiskHashMap::load_from(tempdir.path()).unwrap();

        map.insert(b"test_key", b"test_value").unwrap();
        assert_eq!(map.get(b"test_key").unwrap(), Some(b"test_value".as_ref()));
    }

    #[test]
    fn test_with_capacity_rounds_up_to_power_of_2() {
        let tempdir = tempfile::tempdir().expect("Failed to create temp dir");

        // Test with non-power-of-2 capacities
        let map: DiskHashMap<Bytes, Bytes, MMapFile, FxBuildHasher> =
            DiskHashMap::with_capacity(tempdir.path(), 15, 32, Some(4096)).unwrap();

        // 15 -> 16 (next power of 2)
        assert_eq!(map.capacity(), 16);
    }

    #[test]
    fn test_with_capacity_zero_values_error() {
        let tempdir = tempfile::tempdir().expect("Failed to create temp dir");

        // Test zero num_entries
        let result: io::Result<DiskHashMap<Bytes, Bytes, MMapFile, FxBuildHasher>> =
            DiskHashMap::with_capacity(tempdir.path().join("zero_entries"), 0, 512, Some(1024));
        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }

        // Test zero keys_bytes
        let result: io::Result<DiskHashMap<Bytes, Bytes, MMapFile, FxBuildHasher>> =
            DiskHashMap::with_capacity(tempdir.path().join("zero_keys"), 8, 0, Some(1024));
        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }

        // Test zero values_bytes
        let result: io::Result<DiskHashMap<Bytes, Bytes, MMapFile, FxBuildHasher>> =
            DiskHashMap::with_capacity(tempdir.path().join("zero_values"), 8, 512, Some(0));
        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn test_iterators() {
        let mut map: DiskHashMap<Native<u64>, Str, VecStore> = DiskHashMap::new();

        // Insert test data
        let test_data = vec![
            (1u64, "one"),
            (2u64, "two"),
            (3u64, "three"),
            (4u64, "four"),
            (5u64, "five"),
        ];

        for (key, value) in &test_data {
            map.insert(key, value).unwrap();
        }

        // Test iter() - collect all key-value pairs
        let mut collected: Vec<_> = map.iter().map(|result| result.unwrap()).collect();
        collected.sort_by_key(|(k, _)| *k);

        assert_eq!(collected.len(), test_data.len());
        for (i, (key, value)) in collected.iter().enumerate() {
            assert_eq!(*key, test_data[i].0);
            assert_eq!(*value, test_data[i].1);
        }

        // Test keys() - collect all keys
        let mut keys: Vec<_> = map.keys().map(|result| result.unwrap()).collect();
        keys.sort();

        assert_eq!(keys.len(), test_data.len());
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(*key, test_data[i].0);
        }

        // Test values() - collect all values
        let mut values: Vec<_> = map.values().map(|result| result.unwrap()).collect();
        values.sort();

        let mut expected_values: Vec<_> = test_data.iter().map(|(_, v)| *v).collect();
        expected_values.sort();

        assert_eq!(values.len(), test_data.len());
        for (actual, expected) in values.iter().zip(expected_values.iter()) {
            assert_eq!(actual, expected);
        }

        assert_eq!(map.iter().count(), test_data.len());
        assert_eq!(map.keys().count(), test_data.len());
        assert_eq!(map.values().count(), test_data.len());

        // Test empty map
        let empty_map: DiskHashMap<Native<u64>, Str, VecStore> = DiskHashMap::new();
        assert_eq!(empty_map.iter().count(), 0);
        assert_eq!(empty_map.keys().count(), 0);
        assert_eq!(empty_map.values().count(), 0);
        assert_eq!(empty_map.iter().count(), 0);
    }
}
