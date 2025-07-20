use std::hash::{BuildHasher, Hash, Hasher, RandomState};
use std::marker::PhantomData;

use crate::fixed_buffers::FixedVec;
use crate::raw_map::entry::Entry;
use crate::{Buffers, ByteStore};

mod entry;

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
    for OpenHashMap<K, V, Vec<u8>, Vec<u8>, Vec<u8>, RandomState>
{
    fn default() -> Self {
        let entry_cap = 16 * std::mem::size_of::<Entry>();
        Self::new(Vec::with_capacity(entry_cap), Vec::new(), Vec::new(), 16)
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
    pub fn new(
        entry_store: EBs,
        keys_store: KBs,
        values_store: VBs,
        initial_capacity: usize,
    ) -> Self {
        let keys = Buffers::new(keys_store);
        let values = Buffers::new(values_store);
        let entries = FixedVec::new(entry_store);
        let capacity = initial_capacity.max(16).next_power_of_two();

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

    /// Creates a new OpenHashMap with default hash and equality functions
    pub fn with_capacity(
        entry_store: EBs,
        keys_store: KBs,
        values_store: VBs,
        initial_capacity: usize,
    ) -> Self {
        Self::new(entry_store, keys_store, values_store, initial_capacity)
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
        self.size as f64 / self.capacity as f64
    }

    /// Check if resizing is needed based on load factor
    fn should_resize(&self) -> bool {
        // Resize when load factor exceeds 70%
        self.load_factor() > 0.7
    }

    fn hash_key(&self, key: &[u8]) -> usize {
        self.hasher.hash_one(key) as usize
    }

    /// Find the slot index for a key
    /// if the key is found, returns Some(index),
    /// if the key is not found return the first empty slot index
    fn find_slot(&self, key: &[u8]) -> Result<usize, usize> {
        if self.is_empty() {
            return Err(self.len());
        }

        let hash = self.hash_key(key);
        let index = hash % self.capacity;

        // Linear probing
        for entry in &self.entries[index..self.capacity()] {
            if entry.is_empty() {
                // Empty slot, key not found
                return Err(entry.key_pos());
            }

            if entry.is_deleted() {
                // Deleted slot, continue probing
                continue;
            }
            // Check if this is our key
            if let Some(stored_key) = self.keys.get(entry.key_pos()) {
                if key == stored_key {
                    return Ok(index);
                }
            }
        }

        Err(self.len())
    }

    /// Insert a key-value pair into the map
    pub fn insert(&mut self, k: K, v: V) -> Option<usize> {
        // Check if we need to resize before insert
        if self.should_resize() {
            self.grow();
        }

        let key_bytes = k.as_ref();
        let value_bytes = v.as_ref();

        match self.find_slot(key_bytes) {
            Err(slot_idx) if slot_idx == self.len() => {
                // need to resize, we double the capacity of the entries array
                self.grow();
                // insert the new key-value pair
                self.insert(k, v)
            }
            Err(slot_idx) => {
                // Found an empty slot, insert new key-value pair
                let key_idx = self.keys.append(key_bytes);
                let value_idx = self.values.append(value_bytes);

                // Create a new entry
                self.entries[slot_idx] = Entry::occupied_at_pos(key_idx, value_idx);
                self.size += 1;

                Some(slot_idx)
            }
            Ok(slot_idx) => {
                // Key already exists, update value
                let entry = &mut self.entries[slot_idx];
                let old_value_idx = entry.key_pos();
                let new_value_idx = self.values.append(value_bytes);

                // Update the value index in the entry
                entry.set_new_kv(old_value_idx, new_value_idx);

                // If the value was updated, return the old value index
                Some(old_value_idx)
            }
        }
    }

    fn grow(&mut self) {
        let new_capacity = self.capacity * 2;
        let mut new_entries = self.entries.new_empty(new_capacity);
        // need a way to rehash all existing entries
        for entry in self.entries.iter() {
            if entry.is_occupied() {
                // Reinsert existing entries into the new entries array
                let key_data = self.keys.get(entry.key_pos()).unwrap_or_default();
                let hash = self.hash_key(key_data);
                let new_index = hash % new_capacity;
                let new_entry = Entry::occupied_at_pos(entry.key_pos(), entry.value_pos());
                // Find an empty slot in the new entries array
                for entry in &mut new_entries[new_index..new_capacity] {
                    if entry.is_empty() {
                        // Found an empty slot, insert the entry
                        *entry = new_entry;
                        break;
                    }
                }
            }
        }
        // Update the entries to the new entries array
        self.entries = new_entries;
        self.capacity = new_capacity;
    }

    /// Get a value by key
    pub fn get(&self, k: &K) -> Option<&[u8]> {
        let slot_idx = self.find_slot(k.as_ref()).ok()?;
        let entry = &self.entries[slot_idx];

        if entry.is_occupied() {
            self.values.get(entry.value_pos())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    type OHM<K, V> = OpenHashMap<K, V, Vec<u8>, Vec<u8>, Vec<u8>, RandomState>;

    // Basic functionality tests
    #[test]
    fn test_insert_and_get() {
        let mut map: OHM<&[u8], &[u8]> = OpenHashMap::default();

        // Insert a key-value pair
        map.insert(b"hello", b"world");

        // Get the value
        let value = map.get(&b"hello".as_ref());
        assert_eq!(value, Some(b"world".as_ref()));

        // Test non-existent key
        let value = map.get(&b"not_found".as_ref());
        assert_eq!(value, None);
    }

    // #[test]
    // fn test_update_value() {
    //     let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();

    //     // Insert a key-value pair
    //     map.insert(b"key".to_vec(), b"value1".to_vec());

    //     // Update the value
    //     map.insert(b"key".to_vec(), b"value2".to_vec());

    //     // Get the updated value
    //     let value = map.get(&b"key".to_vec());
    //     assert_eq!(value, Some(b"value2".as_ref()));
    // }

    // #[test]
    // fn test_multiple_entries() {
    //     let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();

    //     // Insert multiple key-value pairs
    //     map.insert(b"key1".to_vec(), b"value1".to_vec());
    //     map.insert(b"key2".to_vec(), b"value2".to_vec());
    //     map.insert(b"key3".to_vec(), b"value3".to_vec());

    //     // Get values
    //     assert_eq!(map.get(&b"key1".to_vec()), Some(b"value1".as_ref()));
    //     assert_eq!(map.get(&b"key2".to_vec()), Some(b"value2".as_ref()));
    //     assert_eq!(map.get(&b"key3".to_vec()), Some(b"value3".as_ref()));
    // }

    // #[test]
    // fn test_empty_map() {
    //     let map: OpenHashMap<Vec<u8>, Vec<u8>, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();

    //     // Map should be empty
    //     assert_eq!(map.len(), 0);
    //     assert!(map.is_empty());

    //     // Get on empty map
    //     assert_eq!(map.get(&b"key".to_vec()), None);
    // }

    // #[test]
    // fn test_resize() {
    //     let hash_fn: Box<dyn Fn(&[u8]) -> usize> = Box::new(|b| {
    //         // Simple hash function for testing
    //         if b.is_empty() { 0 } else { b[0] as usize }
    //     });
    //     let eq_fn: Box<dyn Fn(&[u8], &[u8]) -> bool> = Box::new(|a, b| a == b);
    //     let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> =
    //         OpenHashMap::new(Vec::new(), Vec::new(), 4, hash_fn, eq_fn);

    //     // Insert enough items to trigger resize
    //     for i in 0..10 {
    //         let key = vec![i];
    //         let value = vec![i * 2];
    //         map.insert(key, value);
    //     }

    //     // Check all values after resize
    //     for i in 0..10 {
    //         let key = vec![i];
    //         let expected = vec![i * 2];
    //         assert_eq!(map.get(&key), Some(expected.as_ref()));
    //     }

    //     // Capacity should have increased
    //     assert!(map.capacity() > 4);
    // }

    // #[test]
    // fn test_collision_handling() {
    //     // Create a map with a hash function that always returns the same value
    //     let hash_fn: Box<dyn Fn(&[u8]) -> usize> = Box::new(|_| 42);
    //     let eq_fn: Box<dyn Fn(&[u8], &[u8]) -> bool> = Box::new(|a, b| a == b);
    //     let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> =
    //         OpenHashMap::new(Vec::new(), Vec::new(), 16, hash_fn, eq_fn);

    //     // Insert multiple key-value pairs (all will hash to the same bucket)
    //     map.insert(b"key1".to_vec(), b"value1".to_vec());
    //     map.insert(b"key2".to_vec(), b"value2".to_vec());
    //     map.insert(b"key3".to_vec(), b"value3".to_vec());

    //     // Check all values
    //     assert_eq!(map.get(&b"key1".to_vec()), Some(b"value1".as_ref()));
    //     assert_eq!(map.get(&b"key2".to_vec()), Some(b"value2".as_ref()));
    //     assert_eq!(map.get(&b"key3".to_vec()), Some(b"value3".as_ref()));
    // }

    // // Property-based tests

    // // Helper to convert a Vec<u8> to a human-readable debug string
    // fn debug_bytes(bytes: &[u8]) -> String {
    //     format!("{:?}", bytes)
    // }

    // proptest! {
    //     // Test that inserting and retrieving works for arbitrary data
    //     #[test]
    //     fn prop_insert_get(key: Vec<u8>, value: Vec<u8>) {
    //         let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();

    //         map.insert(key.clone(), value.clone());
    //         let result = map.get(&key);

    //         prop_assert_eq!(
    //             result,
    //             Some(value.as_ref()),
    //             "Failed with key: {}, value: {}",
    //             debug_bytes(&key),
    //             debug_bytes(&value)
    //         );
    //     }

    //     // Test that the map behaves like a standard HashMap
    //     #[test]
    //     fn prop_matches_std_hashmap(
    //         operations: Vec<(Vec<u8>, Vec<u8>)>,
    //         lookups: Vec<Vec<u8>>
    //     ) {
    //         // Limit test size for performance
    //         let operations = operations.into_iter().take(50).collect::<Vec<_>>();
    //         let lookups = lookups.into_iter().take(20).collect::<Vec<_>>();

    //         let mut our_map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();
    //         let mut std_map = HashMap::new();

    //         // Perform operations
    //         for (key, value) in operations {
    //             our_map.insert(key.clone(), value.clone());
    //             std_map.insert(key, value);
    //         }

    //         // Test lookups
    //         for key in lookups {
    //             let our_result = our_map.get(&key);
    //             let std_result = std_map.get(&key).map(|v| v.as_slice());

    //             prop_assert_eq!(
    //                 our_result,
    //                 std_result,
    //                 "Results differ for key: {}",
    //                 debug_bytes(&key)
    //             );
    //         }
    //     }

    //     // Test that the map correctly handles updates to existing keys
    //     #[test]
    //     fn prop_update_values(
    //         key: Vec<u8>,
    //         value1: Vec<u8>,
    //         value2: Vec<u8>
    //     ) {
    //         let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();

    //         map.insert(key.clone(), value1);
    //         map.insert(key.clone(), value2.clone());

    //         let result = map.get(&key);
    //         prop_assert_eq!(
    //             result,
    //             Some(value2.as_ref()),
    //             "Failed to update key: {}",
    //             debug_bytes(&key)
    //         );
    //     }
    // }
}
