use std::hash::BuildHasher;
use std::io;
use std::marker::PhantomData;
use std::path::Path;

use rustc_hash::FxBuildHasher;

use crate::byte_store::{MMapFile, VecStore};
use crate::fixed_buffers::FixedVec;
use crate::raw_map::entry::Entry;
use crate::raw_map::storage::MapStorage;
use crate::types::{BytesDecode, BytesEncode, Native, Str};
use crate::{Buffers, ByteStore};

// Type aliases for common use cases
pub type U64StringMap<BS = VecStore> = HashMap<Native<u64>, Str<String>, BS>;
pub type StringU64Map<BS = VecStore> = HashMap<Str<String>, Native<u64>, BS>;
pub type StringStringMap<BS = VecStore> = HashMap<Str<String>, Str<String>, BS>;

/// A simplified hash map implementation with trait-based encoding/decoding
pub struct HashMap<K, V, BS, S = FxBuildHasher>
where
    BS: ByteStore,
    S: BuildHasher + Default,
{
    entries: FixedVec<Entry, BS>,
    keys: Buffers<BS>,
    values: Buffers<BS>,
    capacity: usize,
    size: usize,
    hasher: S,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for HashMap<K, V, VecStore, FxBuildHasher> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, BS, S> HashMap<K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher + Default,
{
    /// Creates a new HashMap with the given backing stores
    pub fn with_stores(entry_store: BS, keys_store: BS, values_store: BS) -> Self {
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
            return f64::INFINITY;
        }
        self.size as f64 / self.capacity as f64
    }

    /// Check if resizing is needed based on load factor
    fn should_resize(&self) -> bool {
        if self.capacity == 0 {
            return true;
        }
        self.load_factor() > 0.4
    }

    fn hash_key(&self, key_bytes: &[u8]) -> u64 {
        self.hasher.hash_one(key_bytes)
    }

    /// Find the slot index for a key
    fn find_slot(&self, key_bytes: &[u8]) -> Result<usize, usize> {
        if self.capacity == 0 {
            return Err(0);
        }

        let hash = self.hash_key(key_bytes);
        let mut index = hash as usize % self.capacity;

        // Linear probing
        for _ in 0..self.capacity {
            let entry = &self.entries[index];
            if entry.is_empty() {
                return Err(index);
            }

            if !entry.is_deleted() {
                if let Some(stored_key) = self.keys.get(entry.key_pos()) {
                    if key_bytes == stored_key {
                        return Ok(index);
                    }
                }
            }
            index = (index + 1) % self.capacity;
        }

        Err(self.capacity)
    }

    /// Insert a key-value pair into the map, returning the previous value if it existed
    pub fn insert<'a, KEnc, VEnc, VDec>(&mut self, key: &'a KEnc::EItem, value: &'a VEnc::EItem) -> Result<Option<<VDec as BytesDecode<'_>>::DItem>, Box<dyn std::error::Error + Sync + Send>>
    where
        KEnc: BytesEncode<'a>,
        VEnc: BytesEncode<'a>,
        for<'b> VDec: BytesDecode<'b>,
    {
        if self.should_resize() {
            self.grow()?;
        }

        let key_bytes = KEnc::bytes_encode(key)?;
        let value_bytes = VEnc::bytes_encode(value)?;

        match self.find_slot(&key_bytes) {
            Err(slot_idx) => {
                // Found an empty slot, insert new key-value pair
                let key_idx = self.keys.append(&key_bytes);
                let value_idx = self.values.append(&value_bytes);

                self.entries[slot_idx] = Entry::occupied_at_pos(key_idx, value_idx);
                self.size += 1;

                Ok(None)
            }
            Ok(slot_idx) => {
                // Key already exists, update value
                let entry = &mut self.entries[slot_idx];
                let old_value_idx = entry.value_pos();
                let new_value_idx = self.values.append(&value_bytes);
                entry.set_new_kv(entry.key_pos(), new_value_idx);

                // Get the old value after the mutation
                let old_value_bytes = self.values.get(old_value_idx).expect("value must exist for occupied entry");
                let old_value = VDec::bytes_decode(old_value_bytes)?;

                Ok(Some(old_value))
            }
        }
    }

    fn grow(&mut self) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
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
        Ok(())
    }

    /// Get a value by key
    pub fn get<'a, KEnc, VDec>(&self, key: &'a KEnc::EItem) -> Result<Option<<VDec as BytesDecode<'_>>::DItem>, Box<dyn std::error::Error + Sync + Send>>
    where
        KEnc: BytesEncode<'a>,
        for<'b> VDec: BytesDecode<'b>,
    {
        if self.is_empty() {
            return Ok(None);
        }
        
        let key_bytes = KEnc::bytes_encode(key)?;
        match self.find_slot(&key_bytes) {
            Ok(slot_idx) => {
                let entry = &self.entries[slot_idx];
                let value_bytes = self.values.get(entry.value_pos()).expect("value must exist for occupied entry");
                let value = VDec::bytes_decode(value_bytes)?;
                Ok(Some(value))
            }
            Err(_) => Ok(None),
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

impl<K, V> HashMap<K, V, VecStore, FxBuildHasher> {
    /// Creates a new in-memory HashMap
    pub fn new() -> Self {
        Self::with_stores(VecStore::new(), VecStore::new(), VecStore::new())
    }
}

impl<K, V, S> HashMap<K, V, MMapFile, S>
where
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

// Convenience implementations for common type combinations
impl<BS, S> HashMap<Native<u64>, Str<String>, BS, S>
where
    BS: ByteStore,
    S: BuildHasher + Default,
{
    /// Insert a u64 key with String value
    pub fn insert_u64_string(&mut self, key: u64, value: String) -> Result<Option<String>, Box<dyn std::error::Error + Sync + Send>> {
        self.insert::<Native<u64>, Str<String>, Str<String>>(&key, &value)
    }

    /// Get a String value by u64 key
    pub fn get_u64(&self, key: u64) -> Result<Option<String>, Box<dyn std::error::Error + Sync + Send>> {
        self.get::<Native<u64>, Str<String>>(&key)
    }
}

impl<BS, S> HashMap<Str<String>, Native<u64>, BS, S>
where
    BS: ByteStore,
    S: BuildHasher + Default,
{
    /// Insert a String key with u64 value
    pub fn insert_string_u64(&mut self, key: String, value: u64) -> Result<Option<u64>, Box<dyn std::error::Error + Sync + Send>> {
        self.insert::<Str<String>, Native<u64>, Native<u64>>(&key, &value)
    }

    /// Get a u64 value by String key
    pub fn get_string(&self, key: &str) -> Result<Option<u64>, Box<dyn std::error::Error + Sync + Send>> {
        let key_owned = key.to_string();
        self.get::<Str<String>, Native<u64>>(&key_owned)
    }
}

impl<BS, S> HashMap<Str<String>, Str<String>, BS, S>
where
    BS: ByteStore,
    S: BuildHasher + Default,
{
    /// Insert a String key with String value
    pub fn insert_string_string(&mut self, key: String, value: String) -> Result<Option<String>, Box<dyn std::error::Error + Sync + Send>> {
        self.insert::<Str<String>, Str<String>, Str<String>>(&key, &value)
    }

    /// Get a String value by String key
    pub fn get_str(&self, key: &str) -> Result<Option<String>, Box<dyn std::error::Error + Sync + Send>> {
        let key_owned = key.to_string();
        self.get::<Str<String>, Str<String>>(&key_owned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Native, Str};
    use crate::VecStore;

    #[test]
    fn test_native_u64_str_string() {
        let mut map: HashMap<Native<u64>, Str<String>, VecStore> = HashMap::new();

        // Insert a key-value pair
        let result = map.insert::<Native<u64>, Str<String>, Str<String>>(&42u64, &"hello".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        // Get the value
        let result = map.get::<Native<u64>, Str<String>>(&42u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("hello".to_string()));

        // Update the value
        let result = map.insert::<Native<u64>, Str<String>, Str<String>>(&42u64, &"world".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("hello".to_string()));

        // Verify updated value
        let result = map.get::<Native<u64>, Str<String>>(&42u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("world".to_string()));

        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_str_string_native_u32() {
        let mut map: HashMap<Str<String>, Native<u32>, VecStore> = HashMap::new();

        // Insert multiple pairs
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();

        let result = map.insert::<Str<String>, Native<u32>, Native<u32>>(&key1, &100u32);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        let result = map.insert::<Str<String>, Native<u32>, Native<u32>>(&key2, &200u32);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        // Verify both values
        let result = map.get::<Str<String>, Native<u32>>(&key1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(100u32));

        let result = map.get::<Str<String>, Native<u32>>(&key2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(200u32));

        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_empty_map() {
        let map: HashMap<Native<u64>, Str<String>, VecStore> = HashMap::new();

        assert_eq!(map.len(), 0);
        assert!(map.is_empty());

        let result = map.get::<Native<u64>, Str<String>>(&42u64);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_capacity_and_growth() {
        let mut map: HashMap<Native<u8>, Native<u8>, VecStore> = HashMap::new();
        
        // Insert enough items to trigger growth
        for i in 0u8..20 {
            let result = map.insert::<Native<u8>, Native<u8>, Native<u8>>(&i, &(i * 2));
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), None);
        }

        assert_eq!(map.len(), 20);

        // Verify all values are still accessible
        for i in 0u8..20 {
            let result = map.get::<Native<u8>, Native<u8>>(&i);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), Some(i * 2));
        }
    }

    #[test]
    fn test_convenience_methods() {
        // Test U64StringMap
        let mut map: U64StringMap = U64StringMap::new();
        
        let result = map.insert_u64_string(42, "hello".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        let result = map.get_u64(42);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("hello".to_string()));

        // Test StringU64Map
        let mut map2: StringU64Map = StringU64Map::new();
        
        let result = map2.insert_string_u64("key".to_string(), 100);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        let result = map2.get_string("key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(100));

        // Test StringStringMap
        let mut map3: StringStringMap = StringStringMap::new();
        
        let result = map3.insert_string_string("key".to_string(), "value".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        let result = map3.get_str("key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("value".to_string()));
    }
}