use std::hash::BuildHasher;
use std::io;
use std::marker::PhantomData;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use rustc_hash::FxBuildHasher;

use crate::byte_store::{MMapFile, VecStore};
use crate::entries::{DoubleArrayEntries, EntriesState, SlotIdx};
use crate::entry::Entry;
use crate::error::Result;
use crate::fixed_buffers::FixedVec;
use crate::heap::HeapOps;
use crate::types::{BytesDecode, BytesEncode, KVPair, Native, Str};
use crate::{ByteStore, Heap, HeapIdx};

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
    K: for<'a> BytesDecode<'a>,
{
    /// Returns true if the entry is occupied
    pub fn is_occupied(&self) -> bool {
        matches!(self, MapEntry::Occupied(_))
    }

    /// Returns true if the entry is vacant
    pub fn is_vacant(&self) -> bool {
        matches!(self, MapEntry::Vacant(_))
    }

    pub fn key(&self) -> <K as BytesDecode<'_>>::DItem {
        let k = match self {
            MapEntry::Occupied(entry) => {
                let (key, _) = KVPair::<K, ()>::decode_key(entry.kv_bytes()).unwrap();
                return key;
            }
            MapEntry::Vacant(entry) => <K as BytesDecode>::bytes_decode(&entry.key),
        };
        k.expect("Failed to decode key").0
    }
}

/// A view into an occupied entry in the map
pub struct OccupiedEntry<'a, K, V, BS, S = FxBuildHasher>
where
    BS: ByteStore,
    S: BuildHasher,
{
    map: &'a mut DiskHashMap<K, V, BS, S>,
    slot_idx: SlotIdx,
    entry: Entry,
    hash: u64,
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
    slot_idx: SlotIdx,
    hash: u64,
}

/// This is an open address hash map implementation with trait-based encoding/decoding.
/// It takes any types that implement BytesEncode/BytesDecode as key and value.
/// It is designed to be used with a backing store that implements `ByteStore` trait,
/// allowing for flexible storage options (in-memory with VecStore or persistent with MMapFile).
/// The `ByteStore` is not used directly; instead we rely on `Buffers`
/// which is technically a `Vec<Box<[u8]>>` but backed by a `ByteStore` trait.
#[derive(Debug)]
pub struct DiskHashMap<K, V, BS, S = FxBuildHasher>
where
    BS: ByteStore,
    S: BuildHasher,
{
    entries: DoubleArrayEntries<BS>,
    heap: Heap<BS>,
    size: usize,
    hasher: S,
    entries_size_category: usize,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for DiskHashMap<K, V, VecStore, FxBuildHasher> {
    fn default() -> Self {
        Self::new()
    }
}

static EMPTY_ENTRY: Entry = Entry::new();

impl<K, V, BS, S> DiskHashMap<K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
    Heap<BS>: HeapOps<BS>,
{
    /// Returns the number of key-value pairs in the map
    pub fn len(&self) -> usize {
        self.size
    }

    /// Returns true if the map contains no elements
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Returns the current capacity of the map
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    /// Returns the load factor of the map (size / capacity)
    pub fn load_factor(&self) -> f64 {
        if self.capacity() == 0 {
            return f64::INFINITY;
        }
        self.size as f64 / self.capacity() as f64
    }

    /// Returns a reference to the entries storage (for internal use by iterators)
    pub(crate) fn entries(&self) -> &DoubleArrayEntries<BS> {
        &self.entries
    }

    /// Check if resizing is needed based on load factor
    fn should_resize(&self) -> bool {
        if self.capacity() == 0 {
            return true;
        }
        // Resize when load factor exceeds 50% to reduce resize frequency.
        // Trade-off: slightly longer probe distances at peak, but fewer resize cycles.
        self.load_factor() > 0.5
    }

    pub(crate) fn entries_state(&self) -> &EntriesState {
        let es_bytes = self
            .heap
            .get(
                HeapIdx::new()
                    .with_category(self.entries_size_category as u8)
                    .with_offset(0),
            )
            .expect("EntriesState must exist in heap");
        bytemuck::from_bytes::<EntriesState>(&es_bytes[0..size_of::<EntriesState>()])
    }

    fn entries_state_mut(heap: &mut Heap<BS>, esg: u8) -> &mut EntriesState {
        let es_bytes = heap
            .get_mut(HeapIdx::new().with_category(esg).with_offset(0))
            .expect("EntriesState must exist in heap");
        bytemuck::from_bytes_mut::<EntriesState>(&mut es_bytes[0..size_of::<EntriesState>()])
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
    /// Returns an iterator over the key-value pairs of the map.
    pub fn iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(<K as BytesDecode<'a>>::DItem, <V as BytesDecode<'a>>::DItem)>> + 'a
    {
        let EntriesState {
            reindex_offset,
            occupied_count,
            ..
        } = *self.entries_state();
        self.entries()
            .iter(reindex_offset, occupied_count as usize)
            .map(|(_, entry)| {
                let kv_bytes = self.get_kv_bytes(&entry);
                let (key, value, _) = KVPair::<K, V>::decode_key_value(kv_bytes)?;
                Ok((key, value))
            })
    }

    /// Returns an iterator over the keys of the map.
    pub fn keys(&self) -> impl Iterator<Item = Result<<K as BytesDecode<'_>>::DItem>> + '_ {
        self.iter().map(|res| res.map(|(k, _)| k))
    }

    /// Returns an iterator over the values of the map.
    pub fn values(&self) -> impl Iterator<Item = Result<<V as BytesDecode<'_>>::DItem>> + '_ {
        self.iter().map(|res| res.map(|(_, v)| v))
    }

    /// Get the KV bytes for an entry
    fn get_kv_bytes(&self, entry: &Entry) -> &[u8] {
        let pos = entry.pos();
        let heap_idx = HeapIdx::from_u64(pos);
        self.heap
            .get(heap_idx)
            .expect("kv data must exist for occupied entry")
    }

    /// Find the slot index for a key
    /// if the key is found, returns Ok(index, entry, hash),
    /// if the key is not found returns Err((first_empty_slot_index, entry, hash))
    fn find_slot(
        &self,
        key: &[u8],
        mut eq_fn: impl FnMut(&[u8], &[u8]) -> bool,
        hash_fn: impl Fn(&[u8]) -> u64,
    ) -> std::result::Result<(SlotIdx, &Entry, u64), (SlotIdx, &Entry, u64)> {
        let hash = hash_fn(key);
        let (new_entries, old_entries) =
            self.entries.find_entry(hash as usize, self.entries_state());

        let entries_state = self.entries_state();
        if entries_state.reindex_offset < 0 {
            // we are not resizing, just search in the current entries array
            // find the first entry that equals the key or is empty

            for pair @ (_, entry) in new_entries {
                if entry.is_empty() {
                    return Err((pair.0, pair.1, hash));
                }
                if entry.is_occupied() {
                    // Quick rejection via hash prefix
                    if !entry.hash_prefix_matches(hash) {
                        continue;
                    }
                    let kv_bytes = self.get_kv_bytes(entry);
                    let (key_bytes, _) = KVPair::<K, V>::get_key_bytes(kv_bytes)
                        .expect("key must exist for occupied entry");
                    if eq_fn(key, key_bytes) {
                        return Ok((pair.0, pair.1, hash));
                    }
                }
            }
            unreachable!("should have found an empty slot");
        } else {
            let mut candidate_entry = Err((SlotIdx::max(), &EMPTY_ENTRY, hash));
            // new entries must only be returned on the new_entries array
            for pair @ (_, entry) in new_entries {
                if entry.is_empty() {
                    candidate_entry = Err((pair.0, pair.1, hash));
                    break;
                }
                if entry.is_occupied() {
                    // Quick rejection via hash prefix
                    if !entry.hash_prefix_matches(hash) {
                        continue;
                    }
                    let kv_bytes = self.get_kv_bytes(entry);
                    let (key_bytes, _) = KVPair::<K, V>::get_key_bytes(kv_bytes)
                        .expect("key must exist for occupied entry");
                    if eq_fn(key, key_bytes) {
                        candidate_entry = Ok((pair.0, pair.1, hash));
                        break;
                    }
                }
            }

            // need to check old entries maybe the entry exists in the old array
            if candidate_entry.is_err() {
                for pair @ (_, entry) in old_entries {
                    if entry.is_empty() {
                        break; // no need to continue, we found an empty slot
                    }
                    if entry.is_occupied() {
                        // Quick rejection via hash prefix
                        if !entry.hash_prefix_matches(hash) {
                            continue;
                        }
                        let kv_bytes = self.get_kv_bytes(entry);
                        let (key_bytes, _) = KVPair::<K, V>::get_key_bytes(kv_bytes)
                            .expect("key must exist for occupied entry");
                        if eq_fn(key, key_bytes) {
                            candidate_entry = Ok((pair.0, pair.1, hash));
                            break;
                        }
                    }
                }
            }
            candidate_entry
        }
    }

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

        let entries_size_category = self.entries_size_category;
        let current_state = Self::entries_state_mut(&mut self.heap, entries_size_category as u8);
        let current_occupied_count = current_state.occupied_count;
        let DiskHashMap { heap, .. } = self;

        let new_state = self.entries.grow(new_capacity, current_occupied_count)?;
        let state = Self::entries_state_mut(heap, entries_size_category as u8);
        *state = new_state;

        Ok(())
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

        // Compute hash from actual key bytes
        let hash = {
            let mut hasher = self.hasher.build_hasher();
            <K as BytesEncode>::hash_alt(&key_bytes, &mut hasher)
        };

        match self.find_slot_inner(&key_bytes) {
            Err((slot_idx, _, _)) => {
                // Found an empty slot, insert new key-value pair
                self.insert_new_entry(slot_idx, hash, key, value)?;
                Ok(None)
            }
            Ok((slot_idx, entry, _)) => {
                // Key already exists, update value
                let entry = *entry;
                self.update_existing_entry(slot_idx, &entry, hash, key, value)
            }
        }
    }

    /// Insert a new entry at the given slot index
    fn insert_new_entry<'a>(
        &mut self,
        slot_idx: SlotIdx,
        hash: u64,
        key: &'a <K as BytesEncode<'a>>::EItem,
        value: &'a <V as BytesEncode<'a>>::EItem,
    ) -> Result<Entry> {
        // Encode KV pair with hash
        let kv_idx = {
            let mut kv_bytes =
                KVPair::<K, V>::encode(hash, key, value, |size| self.heap.next_free_page(size))?;
            kv_bytes.flush()?;
            kv_bytes.pos()
        };

        // Store in heap and get position before releasing page borrow
        // let kv_idx = {
        //     let mut page = self.heap.next_free_page(kv_bytes.len());
        //     page.write(&kv_bytes)?;
        //     page.flush()?;
        //     page.pos()
        // };

        // Convert HeapIdx to u64 for storage in Entry (48-bit pos)
        let pos = kv_idx.as_u64();

        let mut entries_state = *(self.entries_state());
        let entries_size_category = self.entries_size_category;
        let DiskHashMap { entries, heap, .. } = self;

        let entry = Entry::occupied_at(pos, hash);
        entries.set_entry(slot_idx, entry, &mut entries_state, |entry| {
            let kv_bytes = {
                let pos = entry.pos();
                let heap_idx = HeapIdx::from_u64(pos);
                heap.get(heap_idx)
                    .expect("kv data must exist for occupied entry")
            };
            KVPair::<K, V>::decode_hash(kv_bytes) as usize
        });

        entries_state.occupied_count += 1;
        // Update the entries state in the heap
        *Self::entries_state_mut(heap, entries_size_category as u8) = entries_state;
        self.size += 1;
        Ok(entry)
    }

    /// Update an existing entry at the given slot index
    fn update_existing_entry<'a, 'b>(
        &'b mut self,
        slot_idx: SlotIdx,
        entry: &Entry,
        hash: u64,
        key: &'a <K as BytesEncode<'a>>::EItem,
        value: &'a <V as BytesEncode<'a>>::EItem,
    ) -> Result<Option<<V as BytesDecode<'b>>::DItem>> {
        // Get position of old entry - we'll decode old value after updating
        let old_pos = entry.pos();

        // Encode new KV pair with hash

        // Store in heap and get position before releasing page borrow
        let kv_idx = {
            let mut page =
                KVPair::<K, V>::encode(hash, key, value, |size| self.heap.next_free_page(size))?;
            // let mut page = self.heap.next_free_page(kv_bytes.len());
            // page.write(&kv_bytes)?;
            page.flush()?;
            page.pos()
        };

        // Convert HeapIdx to u64 for storage in Entry (48-bit pos)
        let pos = kv_idx.as_u64();

        let mut entries_state = *(self.entries_state());
        let entries_size_category = self.entries_size_category;
        let DiskHashMap { heap, entries, .. } = self;

        let new_entry = Entry::occupied_at(pos, hash);
        // Use update_entry instead of set_entry for direct overwrite (no probing)
        entries.update_entry(slot_idx, new_entry, &mut entries_state, |entry| {
            let kv_bytes = {
                let pos = entry.pos();
                let heap_idx = HeapIdx::from_u64(pos);
                heap.get(heap_idx)
                    .expect("kv data must exist for occupied entry")
            };
            KVPair::<K, V>::decode_hash(kv_bytes) as usize
        });
        // Update the entries state in the heap
        *Self::entries_state_mut(heap, entries_size_category as u8) = entries_state;

        // Decode old value from old position (still in heap)
        let old_heap_idx = HeapIdx::from_u64(old_pos);
        let old_kv_bytes = self.heap.get(old_heap_idx).expect("old kv data must exist");
        let (_, key_end) = KVPair::<K, V>::decode_key(old_kv_bytes)?;
        let (old_value, _) = KVPair::<K, V>::decode_value(old_kv_bytes, key_end)?;

        Ok(Some(old_value))
    }

    fn find_slot_inner(
        &self,
        key: &[u8],
    ) -> std::result::Result<(SlotIdx, &Entry, u64), (SlotIdx, &Entry, u64)> {
        self.find_slot(
            key,
            |l, r| <K as BytesEncode>::eq_alt(l, r),
            |k| {
                let mut hasher = self.hasher.build_hasher();
                <K as BytesEncode>::hash_alt(k, &mut hasher)
            },
        )
    }

    /// Get a value by key using the trait-based API
    pub fn get<'a>(
        &self,
        key: &'a <K as BytesEncode<'a>>::EItem,
    ) -> Result<Option<<V as BytesDecode<'_>>::DItem>> {
        self.find_entry(key)?.map_or(Ok(None), |entry| {
            let kv_bytes = self.get_kv_bytes(&entry);
            let (_, key_end) = KVPair::<K, V>::decode_key(kv_bytes)?;
            let (value, _) = KVPair::<K, V>::decode_value(kv_bytes, key_end)?;
            Ok(Some(value))
        })
    }

    pub fn get_key(&self, e: &Entry) -> Result<<K as BytesDecode<'_>>::DItem> {
        let kv_bytes = self.get_kv_bytes(e);
        let (key, _) = KVPair::<K, V>::decode_key(kv_bytes)?;
        Ok(key)
    }

    pub fn get_value(&self, e: &Entry) -> Result<<V as BytesDecode<'_>>::DItem> {
        let kv_bytes = self.get_kv_bytes(e);
        let (_, key_end) = KVPair::<K, V>::decode_key(kv_bytes)?;
        let (value, _) = KVPair::<K, V>::decode_value(kv_bytes, key_end)?;
        Ok(value)
    }

    pub fn find_entry<'a>(&self, key: &'a <K as BytesEncode<'a>>::EItem) -> Result<Option<Entry>> {
        if self.is_empty() {
            return Ok(None);
        }

        let (_, key_bytes) = K::bytes_encode(key)?;
        match self.find_slot_inner(&key_bytes) {
            Ok((_, entry, _)) => {
                if entry.is_occupied() {
                    Ok(Some(*entry))
                } else {
                    Ok(None)
                }
            }
            Err(_) => Ok(None),
        }
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
        match self
            .find_slot_inner(key_bytes)
            .map(|(idx, entry, hash)| (idx, *entry, hash))
        {
            Ok((slot_idx, entry, hash)) => MapEntry::Occupied(OccupiedEntry {
                map: self,
                entry,
                slot_idx,
                hash,
            }),
            Err((slot_idx, _, hash)) => MapEntry::Vacant(VacantEntry {
                map: self,
                key: key_bytes.to_vec(),
                key_len,
                slot_idx,
                hash,
            }),
        }
    }
}

impl<K, V, S: BuildHasher + Default> DiskHashMap<K, V, VecStore, S> {
    /// Creates a new in-memory HashMap
    pub fn new() -> Self {
        let mut heap = Heap::new_in_memory();
        let entries = FixedVec::<Entry, _>::new(VecStore::new());
        let entries_size_category = heap.find_size_category(size_of::<EntriesState>());

        let es = EntriesState {
            reindex_offset: -1,
            reindex_batch: 4,
            occupied_count: 0,
        };
        let idx = heap.append(bytemuck::bytes_of(&es));
        assert_eq!(idx.offset(), 0); // should be at start of heap
        Self {
            heap,
            entries: DoubleArrayEntries::new(entries),
            size: 0,
            entries_size_category,
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
        let length_bytes = DEFAULT_ENTRIES_CAP * std::mem::size_of::<Entry>();
        let mut heap = Heap::new(path.join("heap"))?;
        let entries = FixedVec::<Entry, _>::new(MMapFile::new(path.join("entries"), length_bytes)?);
        let es = EntriesState {
            reindex_offset: -1,
            reindex_batch: 4,
            occupied_count: 0,
        };
        let idx = heap.append(bytemuck::bytes_of(&es));
        assert_eq!(idx.offset(), 0); // should be at start of heap
        let entries_size_category = heap.find_size_category(size_of::<EntriesState>());

        Ok(Self {
            heap,
            entries: DoubleArrayEntries::new(entries),
            size: 0,
            entries_size_category,
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

        let length_bytes = num_entries * size_of::<Entry>();
        // Round up to nearest power of 2
        let mut heap = Heap::new_with_capacity(path.join("heap"), slots_per_slab, max_bytes)?;
        let entries = FixedVec::<Entry, _>::new(MMapFile::new(path.join("entries"), length_bytes)?);
        let es = EntriesState {
            reindex_offset: -1,
            reindex_batch: 4,
            occupied_count: 0,
        };
        let idx = heap.append(bytemuck::bytes_of(&es));
        assert_eq!(idx.offset(), 0); // should be at start of heap
        let entries_size_category = heap.find_size_category(size_of::<EntriesState>());

        Ok(Self {
            heap,
            entries: DoubleArrayEntries::new(entries),
            size: 0,
            entries_size_category,
            hasher: S::default(),
            _marker: PhantomData,
        })
    }

    pub fn load_from(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let heap = Heap::load_from(path.join("heap"))?;
        let entries_size_category = heap.find_size_category(size_of::<EntriesState>());
        let es_bytes = heap
            .get(
                HeapIdx::new()
                    .with_category(entries_size_category as u8)
                    .with_offset(0),
            )
            .expect("EntriesState must exist in heap");
        let es = bytemuck::from_bytes::<EntriesState>(&es_bytes[0..size_of::<EntriesState>()]);
        let size = es.occupied_count as usize;

        // Try to find all entries files to detect if we were in the middle of a resize
        let mut entries_files = Self::find_all_entries_files(path)?;

        if entries_files.len() == 1 {
            // Single entries file - normal case
            let entries_path = &entries_files[0];
            let entries = FixedVec::<Entry, _>::new(MMapFile::from_file(entries_path)?);

            Ok(Self {
                heap,
                entries: DoubleArrayEntries::new(entries),
                size,
                entries_size_category,
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
            let latest_entries_path = &entries_files[1];
            let latest_entries =
                FixedVec::<Entry, _>::new(MMapFile::from_file(latest_entries_path)?);
            let oldest_entries_path = &entries_files[0];
            let oldest_entries =
                FixedVec::<Entry, _>::new(MMapFile::from_file(oldest_entries_path)?);

            Ok(Self {
                heap,
                entries: DoubleArrayEntries::new_with_old(oldest_entries, latest_entries),
                size,
                entries_size_category,
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

        // Sort by capacity (as a proxy for creation order) since file naming is broken
        entries_files.sort_by(|a, b| {
            let get_capacity = |path: &PathBuf| -> usize {
                if let Ok(mmap) = MMapFile::from_file(path) {
                    let entries = FixedVec::<Entry, _>::new(mmap);
                    entries.capacity()
                } else {
                    0
                }
            };
            get_capacity(a).cmp(&get_capacity(b))
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
    /// Get a reference to the KV bytes in the entry
    fn kv_bytes(&self) -> &[u8] {
        let pos = self.entry.pos();
        let heap_idx = HeapIdx::from_u64(pos);
        self.map
            .heap
            .get(heap_idx)
            .expect("kv data must exist for occupied entry")
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
        let kv_bytes = self.kv_bytes();
        let (_, key_end) = KVPair::<K, V>::decode_key(kv_bytes)?;
        let (value, _) = KVPair::<K, V>::decode_value(kv_bytes, key_end)?;
        Ok(value)
    }

    pub fn key(&self) -> Result<<K as BytesDecode<'_>>::DItem> {
        let kv_bytes = self.kv_bytes();
        let (key, _) = KVPair::<K, V>::decode_key(kv_bytes)?;
        Ok(key)
    }

    /// Insert a new value into the entry, returning the old value
    pub fn insert(
        self,
        value: &'a <V as BytesEncode<'a>>::EItem,
    ) -> Result<<V as BytesDecode<'a>>::DItem> {
        // Get old position and copy key bytes before mutation
        let old_pos = self.entry.pos();
        let hash = self.hash;
        let slot_idx = self.slot_idx;

        // Copy key bytes to avoid borrow conflicts
        let key_bytes_copy: Vec<u8> = {
            let kv_bytes = self.kv_bytes();
            let (key_bytes, _) = KVPair::<K, V>::get_key_bytes(kv_bytes)?;
            key_bytes.to_vec()
        };

        // Encode value
        let (value_len, value_bytes) = V::bytes_encode(value)?;

        // Build new KV bytes manually: [hash][key_bytes][value]
        let value_stored_size = if value_len.is_some() {
            std::mem::size_of::<usize>() + value_bytes.len()
        } else {
            value_bytes.len()
        };

        let total_size = KVPair::<K, V>::HASH_SIZE + key_bytes_copy.len() + value_stored_size;
        let mut new_kv_bytes = vec![0u8; total_size];

        // Write hash
        new_kv_bytes[0..KVPair::<K, V>::HASH_SIZE].copy_from_slice(&hash.to_le_bytes());
        let mut offset = KVPair::<K, V>::HASH_SIZE;

        // Write key (already has length prefix if needed)
        new_kv_bytes[offset..offset + key_bytes_copy.len()].copy_from_slice(&key_bytes_copy);
        offset += key_bytes_copy.len();

        // Write value
        if let Some(len) = value_len {
            new_kv_bytes[offset..offset + std::mem::size_of::<usize>()]
                .copy_from_slice(&len.to_le_bytes());
            offset += std::mem::size_of::<usize>();
        }
        new_kv_bytes[offset..offset + value_bytes.len()].copy_from_slice(&value_bytes);

        // Store in heap and get position
        let kv_idx = {
            let mut page = self.map.heap.next_free_page(new_kv_bytes.len());
            page.write(&new_kv_bytes)?;
            page.flush()?;
            page.pos()
        };

        // Convert HeapIdx to u64 for storage in Entry (48-bit pos)
        let pos = kv_idx.as_u64();

        let mut entries_state = *(self.map.entries_state());
        let entries_size_category = self.map.entries_size_category;
        let DiskHashMap { heap, entries, .. } = self.map;

        let new_entry = Entry::occupied_at(pos, hash);
        // Use update_entry instead of set_entry for direct overwrite (no probing)
        entries.update_entry(slot_idx, new_entry, &mut entries_state, |entry| {
            let kv_bytes = {
                let pos = entry.pos();
                let heap_idx = HeapIdx::from_u64(pos);
                heap.get(heap_idx)
                    .expect("kv data must exist for occupied entry")
            };
            KVPair::<K, V>::decode_hash(kv_bytes) as usize
        });
        // Update the entries state in the heap
        *DiskHashMap::<K, V, BS, S>::entries_state_mut(heap, entries_size_category as u8) =
            entries_state;

        // Decode old value from old position (still in heap)
        let old_heap_idx = HeapIdx::from_u64(old_pos);
        let old_kv_bytes = heap.get(old_heap_idx).expect("old kv data must exist");
        let (_, key_end) = KVPair::<K, V>::decode_key(old_kv_bytes)?;
        let (old_value, _) = KVPair::<K, V>::decode_value(old_kv_bytes, key_end)?;

        Ok(old_value)
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

impl<'a, K, V, BS, S> VacantEntry<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher + Default,
    Heap<BS>: HeapOps<BS>,
    K: for<'b> BytesEncode<'b> + for<'b> BytesDecode<'b>,
    V: for<'b> BytesEncode<'b> + for<'b> BytesDecode<'b>,
{
    /// Insert the value into the vacant entry using the trait-based API
    pub fn insert(
        self,
        value: &'a <V as BytesEncode<'a>>::EItem,
    ) -> Result<<V as BytesDecode<'a>>::DItem> {
        let hash = self.hash;
        let slot_idx = self.slot_idx;

        // Encode value
        let (value_len, value_bytes) = V::bytes_encode(value)?;

        // Build KV bytes manually: [hash][key_bytes][value]
        // Note: self.key already has the key bytes (with length prefix if needed)
        let value_stored_size = if value_len.is_some() {
            std::mem::size_of::<usize>() + value_bytes.len()
        } else {
            value_bytes.len()
        };

        let key_stored_size = if self.key_len.is_some() {
            std::mem::size_of::<usize>() + self.key.len()
        } else {
            self.key.len()
        };

        let total_size = KVPair::<K, V>::HASH_SIZE + key_stored_size + value_stored_size;
        let mut kv_bytes = vec![0u8; total_size];

        // Write hash
        kv_bytes[0..KVPair::<K, V>::HASH_SIZE].copy_from_slice(&hash.to_le_bytes());
        let mut offset = KVPair::<K, V>::HASH_SIZE;

        // Write key (with length prefix if needed)
        if let Some(len) = self.key_len {
            kv_bytes[offset..offset + std::mem::size_of::<usize>()]
                .copy_from_slice(&len.to_le_bytes());
            offset += std::mem::size_of::<usize>();
        }
        kv_bytes[offset..offset + self.key.len()].copy_from_slice(&self.key);
        offset += self.key.len();

        // Write value (with length prefix if needed)
        if let Some(len) = value_len {
            kv_bytes[offset..offset + std::mem::size_of::<usize>()]
                .copy_from_slice(&len.to_le_bytes());
            offset += std::mem::size_of::<usize>();
        }
        kv_bytes[offset..offset + value_bytes.len()].copy_from_slice(&value_bytes);

        // Store in heap and get position
        let kv_idx = {
            let mut page = self.map.heap.next_free_page(kv_bytes.len());
            page.write(&kv_bytes)?;
            page.flush()?;
            page.pos()
        };

        // Convert HeapIdx to u64 for storage in Entry (48-bit pos)
        let pos = kv_idx.as_u64();

        let mut entries_state = *(self.map.entries_state());
        let entries_size_category = self.map.entries_size_category;
        let DiskHashMap { entries, heap, .. } = self.map;

        let entry = Entry::occupied_at(pos, hash);
        entries.set_entry(slot_idx, entry, &mut entries_state, |entry| {
            let kv_bytes = {
                let pos = entry.pos();
                let heap_idx = HeapIdx::from_u64(pos);
                heap.get(heap_idx)
                    .expect("kv data must exist for occupied entry")
            };
            KVPair::<K, V>::decode_hash(kv_bytes) as usize
        });

        entries_state.occupied_count += 1;
        // Update the entries state in the heap
        *DiskHashMap::<K, V, BS, S>::entries_state_mut(heap, entries_size_category as u8) =
            entries_state;
        self.map.size += 1;

        // Get the value from the heap we just wrote to
        let kv_data = heap.get(kv_idx).expect("just wrote this kv data");
        let (_, key_end) = KVPair::<K, V>::decode_key(kv_data)?;
        let (value, _) = KVPair::<K, V>::decode_value(kv_data, key_end)?;
        Ok(value)
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
        // #![proptest_config(ProptestConfig::with_cases(10)]
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
