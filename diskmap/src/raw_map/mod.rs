use std::hash::{BuildHasher, RandomState};
use std::io;
use std::marker::PhantomData;
use std::path::Path;

use rustc_hash::FxBuildHasher;

use crate::byte_store::{MMapFile, VecStore};
use crate::fixed_buffers::FixedVec;
use crate::raw_map::entry::Entry;
use crate::raw_map::storage::MapStorage;
use crate::{Buffers, ByteStore};

pub mod entry;
pub mod storage;

/// Entry API for the OpenHashMap, similar to std::collections::HashMap
pub enum MapEntry<'a, K, V, EBs, KBs, VBs, S = RandomState>
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
    EBs: ByteStore,
    KBs: ByteStore,
    VBs: ByteStore,
    S: BuildHasher,
{
    Occupied(OccupiedEntry<'a, K, V, EBs, KBs, VBs, S>),
    Vacant(VacantEntry<'a, K, V, EBs, KBs, VBs, S>),
}

/// A view into an occupied entry in the map
pub struct OccupiedEntry<'a, K, V, EBs, KBs, VBs, S = RandomState>
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
    EBs: ByteStore,
    KBs: ByteStore,
    VBs: ByteStore,
    S: BuildHasher,
{
    map: &'a mut OpenHashMap<K, V, EBs, KBs, VBs, S>,
    slot_idx: usize,
}

/// A view into a vacant entry in the map
pub struct VacantEntry<'a, K, V, EBs, KBs, VBs, S = RandomState>
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
    EBs: ByteStore,
    KBs: ByteStore,
    VBs: ByteStore,
    S: BuildHasher,
{
    map: &'a mut OpenHashMap<K, V, EBs, KBs, VBs, S>,
    key: Vec<u8>,
    slot_idx: usize,
}

/// This is a open address hash map implementation,
/// it takes any &[u8] as key and value.
/// It is designed to be used with a backing store that implements
/// `ByteStore` trait, allowing for flexible storage options.
/// the `ByteStore` is not used directly instead we rely on `Buffers`
/// which is technically a `Vec<Box<[u8]>>` but backed by a `ByteStore` trait.
/// The hash function and equality function are provided as closures
pub struct OpenHashMap<
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
    EBs: ByteStore,
    KBs: ByteStore,
    VBs: ByteStore,
    S = RandomState,
> {
    entries: FixedVec<Entry, EBs>,
    keys: Buffers<KBs>,
    values: Buffers<VBs>,
    capacity: usize,
    size: usize,
    hasher: S,
    _marker: PhantomData<(K, V)>,
}

impl<K: AsRef<[u8]>, V: AsRef<[u8]>> Default
    for OpenHashMap<K, V, VecStore, VecStore, VecStore, FxBuildHasher>
{
    fn default() -> Self {
        Self::new(VecStore::new(), VecStore::new(), VecStore::new())
    }
}

impl<K, V, EBs, KBs, VBs, S> OpenHashMap<K, V, EBs, KBs, VBs, S>
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
    EBs: ByteStore,
    KBs: ByteStore,
    VBs: ByteStore,
    S: BuildHasher + Default,
{
    /// Creates a new OpenHashMap with the given initial capacity and hash/equality functions
    pub fn new(entry_store: EBs, keys_store: KBs, values_store: VBs) -> Self {
        let keys = Buffers::new(keys_store);
        let values = Buffers::new(values_store);
        let entries = FixedVec::new(entry_store);
        let capacity = entries.capacity();

        Self {
            keys,
            values,
            entries,
            capacity,
            size: 0,
            hasher: S::default(),
            _marker: PhantomData,
        }
    }

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
        self.capacity
    }

    /// Returns the load factor of the map (size / capacity)
    pub fn load_factor(&self) -> f64 {
        if self.capacity == 0 {
            return f64::INFINITY; // Avoid division by zero
        }
        self.size as f64 / self.capacity as f64
    }

    /// Check if resizing is needed based on load factor
    fn should_resize(&self) -> bool {
        if self.capacity == 0 {
            return true;
        }
        // Resize when load factor exceeds 40% for better performance.
        self.load_factor() > 0.4
    }

    fn hash_key(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// Find the slot index for a key
    /// if the key is found, returns Some(index),
    /// if the key is not found return the first empty slot index
    fn find_slot(&self, key: &[u8]) -> Result<usize, usize> {
        if self.capacity == 0 {
            return Err(0);
        }

        let hash = self.hash_key(key);
        let mut index = hash as usize % self.capacity;

        // Linear probing
        for _ in 0..self.capacity {
            let entry = &self.entries[index];
            if entry.is_empty() {
                // Empty slot, key not found
                return Err(index);
            }

            if !entry.is_deleted() {
                // Check if this is our key
                if let Some(stored_key) = self.keys.get(entry.key_pos()) {
                    if key == stored_key {
                        return Ok(index);
                    }
                }
            }
            // continue probing for deleted slots or non-matching keys
            index = (index + 1) % self.capacity;
        }

        Err(self.capacity)
    }

    /// Insert a key-value pair into the map, returning reference to previous value if it existed
    pub fn insert(&mut self, k: K, v: V) -> Option<&[u8]> {
        if self.should_resize() {
            self.grow();
        }

        let key_bytes = k.as_ref();
        let value_bytes = v.as_ref();

        match self.find_slot(key_bytes) {
            Err(slot_idx) => {
                // Found an empty slot, insert new key-value pair
                let key_idx = self.keys.append(key_bytes);
                let value_idx = self.values.append(value_bytes);

                self.entries[slot_idx] = Entry::occupied_at_pos(key_idx, value_idx);
                self.size += 1;

                None
            }
            Ok(slot_idx) => {
                // Key already exists, update value
                let entry = &mut self.entries[slot_idx];
                let old_value_idx = entry.value_pos();
                let new_value_idx = self.values.append(value_bytes);

                // Update the value index in the entry, keeping the key pos
                entry.set_new_kv(entry.key_pos(), new_value_idx);

                // Return reference to the old value
                Some(self.values.get(old_value_idx).expect("value must exist for occupied entry"))
            }
        }
    }

    fn grow(&mut self) {
        let new_capacity = if self.capacity == 0 {
            16
        } else {
            self.capacity * 2
        };
        let mut new_entries = self.entries.new_empty(new_capacity);

        // Re-hash all existing entries into the new larger array
        for i in 0..self.capacity {
            let entry = self.entries[i];
            if entry.is_occupied() {
                let key_data = self
                    .keys
                    .get(entry.key_pos())
                    .expect("key must exist for occupied entry");
                let hash = self.hash_key(key_data);
                let mut index = hash as usize % new_capacity;

                // Linear probing in the new_entries array
                loop {
                    if new_entries[index].is_empty() {
                        new_entries[index] = entry;
                        break;
                    }
                    index = (index + 1) % new_capacity;
                }
            }
        }
        self.entries = new_entries;
        self.capacity = new_capacity;
    }

    /// Get a value by key
    pub fn get<Q: AsRef<[u8]>>(&self, k: Q) -> Option<&[u8]> {
        if self.is_empty() {
            return None;
        }
        match self.find_slot(k.as_ref()) {
            Ok(slot_idx) => {
                let entry = &self.entries[slot_idx];
                self.values.get(entry.value_pos())
            }
            Err(_) => None,
        }
    }

    /// Get an entry for the given key, allowing for efficient insertion/access patterns
    pub fn entry<Q: AsRef<[u8]>>(&mut self, key: Q) -> MapEntry<'_, K, V, EBs, KBs, VBs, S> {
        if self.should_resize() {
            self.grow();
        }

        let key_bytes = key.as_ref();
        match self.find_slot(key_bytes) {
            Ok(slot_idx) => MapEntry::Occupied(OccupiedEntry {
                map: self,
                slot_idx,
            }),
            Err(slot_idx) => MapEntry::Vacant(VacantEntry {
                map: self,
                key: key_bytes.to_vec(),
                slot_idx,
            }),
        }
    }

    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.entries.store().stats(),
            self.keys.store().stats(),
            self.values.store().stats(),
        )
    }
}

impl<K, V, S> OpenHashMap<K, V, MMapFile, MMapFile, MMapFile, S>
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
    S: BuildHasher + Default,
{
    pub fn new_in(path: &Path) -> io::Result<Self> {
        const DEFAULT_ENTRIES_CAP: usize = 16;
        const DEFAULT_KV_CAP: usize = 1024;

        let storage = MapStorage::new_in(
            path,
            DEFAULT_ENTRIES_CAP * std::mem::size_of::<Entry>(),
            DEFAULT_KV_CAP,
            DEFAULT_KV_CAP,
        )?;

        let keys = Buffers::new(storage.keys);
        let values = Buffers::new(storage.values);
        let entries = FixedVec::<Entry, _>::new(storage.entries);
        let capacity = entries.capacity();

        Ok(Self {
            keys,
            values,
            entries,
            capacity,
            size: 0,
            hasher: S::default(),
            _marker: PhantomData,
        })
    }

    pub fn load_from(path: &Path) -> io::Result<Self> {
        let storage = MapStorage::load_from(path)?;

        let keys = Buffers::load(storage.keys);
        let values = Buffers::load(storage.values);
        let entries = FixedVec::<Entry, _>::new(storage.entries);
        let capacity = entries.capacity();

        let mut size = 0;
        for i in 0..capacity {
            if entries[i].is_occupied() {
                size += 1;
            }
        }

        Ok(Self {
            keys,
            values,
            entries,
            capacity,
            size,
            hasher: S::default(),
            _marker: PhantomData,
        })
    }
}

impl<'a, K, V, EBs, KBs, VBs, S> OccupiedEntry<'a, K, V, EBs, KBs, VBs, S>
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
    EBs: ByteStore,
    KBs: ByteStore,
    VBs: ByteStore,
    S: BuildHasher,
{
    /// Get a reference to the value in the entry
    pub fn get(&self) -> &[u8] {
        let entry = &self.map.entries[self.slot_idx];
        self.map.values.get(entry.value_pos()).expect("value must exist for occupied entry")
    }

    /// Insert a new value into the entry, returning the old value
    pub fn insert(&mut self, value: V) -> &[u8] {
        let entry = &mut self.map.entries[self.slot_idx];
        let old_value_idx = entry.value_pos();
        let new_value_idx = self.map.values.append(value.as_ref());
        
        // Update the value index in the entry, keeping the key pos
        entry.set_new_kv(entry.key_pos(), new_value_idx);
        
        self.map.values.get(old_value_idx).expect("value must exist for occupied entry")
    }
}

impl<'a, K, V, EBs, KBs, VBs, S> VacantEntry<'a, K, V, EBs, KBs, VBs, S>
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
    EBs: ByteStore,
    KBs: ByteStore,
    VBs: ByteStore,
    S: BuildHasher,
{
    /// Insert the value into the vacant entry, returning a reference to the inserted value
    pub fn insert(self, value: V) -> &'a [u8] {
        let key_idx = self.map.keys.append(&self.key);
        let value_idx = self.map.values.append(value.as_ref());

        self.map.entries[self.slot_idx] = Entry::occupied_at_pos(key_idx, value_idx);
        self.map.size += 1;

        self.map.values.get(value_idx).expect("value was just inserted")
    }

    /// Insert the value into the vacant entry if it's vacant, or return reference to existing value
    pub fn or_insert(self, value: V) -> &'a [u8] {
        self.insert(value)
    }

    /// Insert the value returned by the closure if the entry is vacant
    pub fn or_insert_with<F>(self, f: F) -> &'a [u8]
    where
        F: FnOnce() -> V,
    {
        self.insert(f())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rustc_hash::FxBuildHasher;
    use std::collections::HashMap;
    use tempfile::tempdir;

    type OpenHM<K, V> = OpenHashMap<K, V, VecStore, VecStore, VecStore, FxBuildHasher>;

    // Basic functionality tests
    #[test]
    fn test_insert_and_get() {
        let mut map: OpenHM<Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Insert a key-value pair
        map.insert(b"hello".to_vec(), b"world".to_vec());

        // Get the value
        let value = map.get(b"hello");
        assert_eq!(value, Some(b"world".as_ref()));

        // Test non-existent key
        let value = map.get(b"not_found");
        assert_eq!(value, None);
    }

    #[test]
    fn test_update_value() {
        let mut map: OpenHM<Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Insert a key-value pair
        map.insert(b"key".to_vec(), b"value1".to_vec());
        assert_eq!(map.get(b"key"), Some(b"value1".as_ref()));

        // Update the value
        map.insert(b"key".to_vec(), b"value2".to_vec());

        // Get the updated value
        let value = map.get(b"key");
        assert_eq!(value, Some(b"value2".as_ref()));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_multiple_entries() {
        let mut map: OpenHM<Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Insert multiple key-value pairs
        map.insert(b"key1".to_vec(), b"value1".to_vec());
        map.insert(b"key2".to_vec(), b"value2".to_vec());
        map.insert(b"key3".to_vec(), b"value3".to_vec());

        // Get values
        assert_eq!(map.get(b"key1"), Some(b"value1".as_ref()));
        assert_eq!(map.get(b"key2"), Some(b"value2".as_ref()));
        assert_eq!(map.get(b"key3"), Some(b"value3".as_ref()));
    }

    #[test]
    fn test_empty_map() {
        let map: OpenHM<Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Map should be empty
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());

        // Get on empty map
        assert_eq!(map.get(b"key"), None);
    }

    fn check_prop(hm: HashMap<Vec<u8>, Vec<u8>>) {
        let mut map: OpenHM<Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Insert all key-value pairs from the HashMap
        for (k, v) in hm.iter() {
            map.insert(k.clone(), v.clone());
        }

        // Check the size of the map
        assert_eq!(map.len(), hm.len());

        // Check that all values can be retrieved
        for (k, v) in hm.iter() {
            assert_eq!(map.get(k), Some(v.as_ref()), "key: {k:?}");
        }
    }
    #[test]
    fn it_s_a_hash_map() {
        let small_hash_map_prop = proptest::collection::hash_map(
            proptest::collection::vec(0u8..255, 1..32),
            proptest::collection::vec(0u8..255, 1..32),
            1..250,
        );

        proptest!(|(values in small_hash_map_prop)|{
            check_prop(values);
        });
    }

    #[test]
    fn it_s_a_hash_map_1() {
        let mut expected = HashMap::new();
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
    fn it_s_a_hash_map_2() {
        let mut expected = HashMap::new();
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

        type FileMap<K, V> = OpenHashMap<K, V, MMapFile, MMapFile, MMapFile, FxBuildHasher>;

        // 1. Create a new map and add some data
        {
            let mut map: FileMap<Vec<u8>, Vec<u8>> = FileMap::new_in(path).unwrap();
            map.insert(b"key1".to_vec(), b"value1".to_vec());
            map.insert(b"key2".to_vec(), b"value2".to_vec());
            assert_eq!(map.len(), 2);
            assert_eq!(map.get(b"key1"), Some(b"value1".as_ref()));
            assert_eq!(map.get(b"key2"), Some(b"value2".as_ref()));
        } // map is dropped, files should be persisted

        // 2. Load the map from disk
        {
            let map: FileMap<Vec<u8>, Vec<u8>> = FileMap::load_from(path).unwrap();
            assert_eq!(map.len(), 2);
            assert_eq!(map.get(b"key1"), Some(b"value1".as_ref()));
            assert_eq!(map.get(b"key2"), Some(b"value2".as_ref()));
            assert_eq!(map.get(b"key3"), None);
        }

        // 3. Load again, and add more data
        {
            let mut map: FileMap<Vec<u8>, Vec<u8>> = FileMap::load_from(path).unwrap();
            map.insert(b"key3".to_vec(), b"value3".to_vec());
            assert_eq!(map.len(), 3);
            assert_eq!(map.get(b"key3"), Some(b"value3".as_ref()));
        }

        // 4. Load one more time to check the new data is there
        {
            let map: FileMap<Vec<u8>, Vec<u8>> = FileMap::load_from(path).unwrap();
            assert_eq!(map.len(), 3);
            assert_eq!(map.get(b"key1"), Some(b"value1".as_ref()));
            assert_eq!(map.get(b"key2"), Some(b"value2".as_ref()));
            assert_eq!(map.get(b"key3"), Some(b"value3".as_ref()));
        }
    }

    #[test]
    fn test_no_resize_with_preallocation() {
        let mut entry_store = VecStore::new();
        entry_store.grow(256 * std::mem::size_of::<entry::Entry>());
        let mut key_store = VecStore::new();
        key_store.grow(20 * 1024);
        let mut value_store = VecStore::new();
        value_store.grow(20 * 1024);

        // The stores have been resized once to pre-allocate space.
        assert_eq!(entry_store.stats(), 1);
        assert_eq!(key_store.stats(), 1);
        assert_eq!(value_store.stats(), 1);

        let mut map: OpenHashMap<Vec<u8>, Vec<u8>, _, _, _, FxBuildHasher> =
            OpenHashMap::new(entry_store, key_store, value_store);

        let initial_stats = map.stats();
        assert_eq!(initial_stats, (1, 1, 1));

        // Insert 100 elements. Should not trigger any more resizes.
        for i in 0..100 {
            let s = i.to_string();
            map.insert(s.clone().into_bytes(), s.into_bytes());
        }
        assert_eq!(
            map.stats(),
            initial_stats,
            "No resize should happen with pre-allocation"
        );

        // Insert more elements to trigger a resize of the entries container.
        for i in 100..150 {
            let s = i.to_string();
            map.insert(s.clone().into_bytes(), s.into_bytes());
        }

        let (entries_resizes, keys_resizes, values_resizes) = map.stats();
        assert_eq!(
            entries_resizes, 0,
            "entries store is replaced, so stats are reset"
        );
        assert_eq!(
            keys_resizes, initial_stats.1,
            "keys store should not resize"
        );
        assert_eq!(
            values_resizes, initial_stats.2,
            "values store should not resize"
        );
    }

    #[test]
    fn test_entry_api_vacant() {
        let mut map: OpenHM<Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Test vacant entry insertion
        match map.entry(b"key1".to_vec()) {
            MapEntry::Vacant(entry) => {
                let value_ref = entry.insert(b"value1".to_vec());
                assert_eq!(value_ref, b"value1");
            }
            MapEntry::Occupied(_) => panic!("Expected vacant entry"),
        }

        assert_eq!(map.len(), 1);
        assert_eq!(map.get(b"key1"), Some(b"value1".as_ref()));
    }

    #[test]
    fn test_entry_api_occupied() {
        let mut map: OpenHM<Vec<u8>, Vec<u8>> = OpenHashMap::default();
        
        // Insert initial value
        map.insert(b"key1".to_vec(), b"value1".to_vec());

        // Test occupied entry access and update
        match map.entry(b"key1".to_vec()) {
            MapEntry::Occupied(mut entry) => {
                assert_eq!(entry.get(), b"value1");
                let old_value = entry.insert(b"value2".to_vec());
                assert_eq!(old_value, b"value1");
            }
            MapEntry::Vacant(_) => panic!("Expected occupied entry"),
        }

        assert_eq!(map.len(), 1);
        assert_eq!(map.get(b"key1"), Some(b"value2".as_ref()));
    }

    #[test]
    fn test_entry_api_or_insert() {
        let mut map: OpenHM<Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Test or_insert with vacant entry
        match map.entry(b"key1".to_vec()) {
            MapEntry::Vacant(entry) => {
                let value_ref = entry.or_insert(b"value1".to_vec());
                assert_eq!(value_ref, b"value1");
            }
            MapEntry::Occupied(_) => panic!("Expected vacant entry"),
        }

        // Test entry with existing key (should not create occupied entry in this test)
        assert_eq!(map.get(b"key1"), Some(b"value1".as_ref()));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_entry_api_or_insert_with() {
        let mut map: OpenHM<Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Test or_insert_with with vacant entry
        match map.entry(b"key1".to_vec()) {
            MapEntry::Vacant(entry) => {
                let value_ref = entry.or_insert_with(|| b"computed_value".to_vec());
                assert_eq!(value_ref, b"computed_value");
            }
            MapEntry::Occupied(_) => panic!("Expected vacant entry"),
        }

        assert_eq!(map.get(b"key1"), Some(b"computed_value".as_ref()));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_insert_returns_previous_value() {
        let mut map: OpenHM<Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // First insert should return None
        let previous = map.insert(b"key1".to_vec(), b"value1".to_vec());
        assert_eq!(previous, None);

        // Second insert should return previous value
        let previous = map.insert(b"key1".to_vec(), b"value2".to_vec());
        assert_eq!(previous, Some(b"value1".as_ref()));

        // Verify current value
        assert_eq!(map.get(b"key1"), Some(b"value2".as_ref()));
        assert_eq!(map.len(), 1);
    }
}
