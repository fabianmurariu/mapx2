use std::hash::{BuildHasher, Hasher, RandomState};
use std::marker::PhantomData;

use crate::{Buffers, ByteStore};

// Simple Entry for the hash map, containing key and value indices
struct Entry {
    key_idx: usize,
    value_idx: usize,
    occupied: bool,
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
    HFn: Fn(&[u8]) -> usize,
    EqFn: Fn(&[u8], &[u8]) -> bool,
    KBs: ByteStore,
    VBs: ByteStore,
    S = RandomState,
> {
    keys: Buffers<KBs>,
    values: Buffers<VBs>,
    hash_fn: HFn,
    eq_fn: EqFn,
    entries: Vec<Entry>,
    capacity: usize,
    size: usize,
    _marker: PhantomData<(K, V, S)>,
}

impl<K, V, HFn, EqFn, KBs, VBs, S> OpenHashMap<K, V, HFn, EqFn, KBs, VBs, S>
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
    HFn: Fn(&[u8]) -> usize,
    EqFn: Fn(&[u8], &[u8]) -> bool,
    KBs: ByteStore,
    VBs: ByteStore,
    S: Default,
{
    /// Creates a new OpenHashMap with the given initial capacity and hash/equality functions
    pub fn new(
        keys_store: KBs,
        values_store: VBs,
        initial_capacity: usize,
        hash_fn: HFn,
        eq_fn: EqFn,
    ) -> Self {
        let keys = Buffers::new(keys_store);
        let values = Buffers::new(values_store);
        let capacity = initial_capacity.max(16).next_power_of_two();

        // Initialize entries with empty slots
        let mut entries = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            entries.push(Entry {
                key_idx: 0,
                value_idx: 0,
                occupied: false,
            });
        }

        Self {
            keys,
            values,
            hash_fn,
            eq_fn,
            entries,
            capacity,
            size: 0,
            _marker: PhantomData,
        }
    }

    /// Creates a new OpenHashMap with default hash and equality functions
    pub fn with_capacity(keys_store: KBs, values_store: VBs, initial_capacity: usize) -> Self
    where
        HFn: Default,
        EqFn: Default,
    {
        Self::new(
            keys_store,
            values_store,
            initial_capacity,
            Default::default(),
            Default::default(),
        )
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

    /// Find the slot index for a key
    fn find_slot(&self, key: &[u8]) -> Option<usize> {
        if self.is_empty() {
            return None;
        }

        let hash = (self.hash_fn)(key);
        let mut index = hash % self.capacity;

        // Linear probing
        for _ in 0..self.capacity {
            let entry = &self.entries[index];

            if !entry.occupied {
                // Empty slot, key not found
                return None;
            }

            // Check if this is our key
            if let Some(stored_key) = self.keys.get(entry.key_idx) {
                if (self.eq_fn)(key, stored_key) {
                    return Some(index);
                }
            }

            // Continue probing
            index = (index + 1) % self.capacity;
        }

        None
    }

    /// Insert a key-value pair into the map
    pub fn insert(&mut self, k: K, v: V) -> Option<usize> {
        // Check if we need to resize before insert
        if self.should_resize() {
            self.resize();
        }

        let key_bytes = k.as_ref();
        let value_bytes = v.as_ref();
        let hash = (self.hash_fn)(key_bytes);
        let mut index = hash % self.capacity;

        // Try to find the key or an empty slot
        for _ in 0..self.capacity {
            let entry = &self.entries[index];

            if !entry.occupied {
                // Found an empty slot, insert here
                let key_idx = self.keys.append(key_bytes);
                let value_idx = self.values.append(value_bytes);

                self.entries[index] = Entry {
                    key_idx,
                    value_idx,
                    occupied: true,
                };

                self.size += 1;
                return Some(value_idx);
            }

            // Check if this is the same key
            if let Some(stored_key) = self.keys.get(entry.key_idx) {
                if (self.eq_fn)(key_bytes, stored_key) {
                    // Update the value for existing key
                    let value_idx = self.values.append(value_bytes);
                    self.entries[index].value_idx = value_idx;
                    return Some(value_idx);
                }
            }

            // Continue probing
            index = (index + 1) % self.capacity;
        }

        // Map is full, shouldn't happen with proper resizing
        None
    }

    /// Get a value by key
    pub fn get(&self, k: &K) -> Option<&[u8]> {
        let slot_idx = self.find_slot(k.as_ref())?;
        let entry = &self.entries[slot_idx];

        if entry.occupied {
            self.values.get(entry.value_idx)
        } else {
            None
        }
    }

    /// Resize the hash map to double the capacity
    fn resize(&mut self) {
        // Double the capacity
        let new_capacity = self.capacity * 2;

        // Save old entries
        let old_entries = std::mem::replace(&mut self.entries, Vec::with_capacity(new_capacity));

        // Initialize new entries with empty slots
        for _ in 0..new_capacity {
            self.entries.push(Entry {
                key_idx: 0,
                value_idx: 0,
                occupied: false,
            });
        }

        // Remember old capacity and update
        let old_capacity = self.capacity;
        self.capacity = new_capacity;

        // Reset size since we'll reinsert everything
        let old_size = self.size;
        self.size = 0;

        // Reinsert all entries
        for i in 0..old_capacity {
            let entry = &old_entries[i];
            if entry.occupied {
                // Collect all key indices and value indices to reinsert
                let key_idx = entry.key_idx;
                let value_idx = entry.value_idx;

                // Clone the key for rehashing
                let key_data = self
                    .keys
                    .get(key_idx)
                    .map(|k| k.to_vec())
                    .unwrap_or_default();

                // Reinsert with the key data and indices
                self.reinsert(key_idx, value_idx, &key_data);
            }
        }

        // Ensure size is preserved
        assert_eq!(self.size, old_size, "Size mismatch after resize");
    }

    /// Helper method to reinsert an existing key-value pair during resize
    fn reinsert(&mut self, key_idx: usize, value_idx: usize, key: &[u8]) {
        let hash = (self.hash_fn)(key);
        let mut index = hash % self.capacity;

        // Find an empty slot
        for _ in 0..self.capacity {
            if !self.entries[index].occupied {
                // Found an empty slot
                self.entries[index] = Entry {
                    key_idx,
                    value_idx,
                    occupied: true,
                };
                self.size += 1;
                return;
            }

            // Continue probing
            index = (index + 1) % self.capacity;
        }

        panic!("Failed to reinsert during resize - no empty slots found");
    }
}

// Default implementations for the OpenHashMap
impl<K, V, KBs, VBs, S> Default
    for OpenHashMap<
        K,
        V,
        Box<dyn Fn(&[u8]) -> usize>,
        Box<dyn Fn(&[u8], &[u8]) -> bool>,
        KBs,
        VBs,
        S,
    >
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
    KBs: ByteStore + Default,
    VBs: ByteStore + Default,
    S: Default,
{
    fn default() -> Self {
        let random_state = RandomState::new();

        // Box the closures to make them work with the trait bounds
        let hash_fn: Box<dyn Fn(&[u8]) -> usize> = Box::new(move |bytes: &[u8]| {
            let mut hasher = random_state.build_hasher();
            hasher.write(bytes);
            hasher.finish() as usize
        });

        let eq_fn: Box<dyn Fn(&[u8], &[u8]) -> bool> = Box::new(|a: &[u8], b: &[u8]| a == b);

        Self::new(KBs::default(), VBs::default(), 16, hash_fn, eq_fn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    // Basic functionality tests
    #[test]
    fn test_insert_and_get() {
        let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Insert a key-value pair
        map.insert(b"hello".to_vec(), b"world".to_vec());

        // Get the value
        let value = map.get(&b"hello".to_vec());
        assert_eq!(value, Some(b"world".as_ref()));

        // Test non-existent key
        let value = map.get(&b"not_found".to_vec());
        assert_eq!(value, None);
    }

    #[test]
    fn test_update_value() {
        let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Insert a key-value pair
        map.insert(b"key".to_vec(), b"value1".to_vec());

        // Update the value
        map.insert(b"key".to_vec(), b"value2".to_vec());

        // Get the updated value
        let value = map.get(&b"key".to_vec());
        assert_eq!(value, Some(b"value2".as_ref()));
    }

    #[test]
    fn test_multiple_entries() {
        let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Insert multiple key-value pairs
        map.insert(b"key1".to_vec(), b"value1".to_vec());
        map.insert(b"key2".to_vec(), b"value2".to_vec());
        map.insert(b"key3".to_vec(), b"value3".to_vec());

        // Get values
        assert_eq!(map.get(&b"key1".to_vec()), Some(b"value1".as_ref()));
        assert_eq!(map.get(&b"key2".to_vec()), Some(b"value2".as_ref()));
        assert_eq!(map.get(&b"key3".to_vec()), Some(b"value3".as_ref()));
    }

    #[test]
    fn test_empty_map() {
        let map: OpenHashMap<Vec<u8>, Vec<u8>, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();

        // Map should be empty
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());

        // Get on empty map
        assert_eq!(map.get(&b"key".to_vec()), None);
    }

    #[test]
    fn test_resize() {
        let hash_fn: Box<dyn Fn(&[u8]) -> usize> = Box::new(|b| {
            // Simple hash function for testing
            if b.is_empty() { 0 } else { b[0] as usize }
        });
        let eq_fn: Box<dyn Fn(&[u8], &[u8]) -> bool> = Box::new(|a, b| a == b);
        let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> =
            OpenHashMap::new(Vec::new(), Vec::new(), 4, hash_fn, eq_fn);

        // Insert enough items to trigger resize
        for i in 0..10 {
            let key = vec![i];
            let value = vec![i * 2];
            map.insert(key, value);
        }

        // Check all values after resize
        for i in 0..10 {
            let key = vec![i];
            let expected = vec![i * 2];
            assert_eq!(map.get(&key), Some(expected.as_ref()));
        }

        // Capacity should have increased
        assert!(map.capacity() > 4);
    }

    #[test]
    fn test_collision_handling() {
        // Create a map with a hash function that always returns the same value
        let hash_fn: Box<dyn Fn(&[u8]) -> usize> = Box::new(|_| 42);
        let eq_fn: Box<dyn Fn(&[u8], &[u8]) -> bool> = Box::new(|a, b| a == b);
        let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> =
            OpenHashMap::new(Vec::new(), Vec::new(), 16, hash_fn, eq_fn);

        // Insert multiple key-value pairs (all will hash to the same bucket)
        map.insert(b"key1".to_vec(), b"value1".to_vec());
        map.insert(b"key2".to_vec(), b"value2".to_vec());
        map.insert(b"key3".to_vec(), b"value3".to_vec());

        // Check all values
        assert_eq!(map.get(&b"key1".to_vec()), Some(b"value1".as_ref()));
        assert_eq!(map.get(&b"key2".to_vec()), Some(b"value2".as_ref()));
        assert_eq!(map.get(&b"key3".to_vec()), Some(b"value3".as_ref()));
    }

    // Property-based tests

    // Helper to convert a Vec<u8> to a human-readable debug string
    fn debug_bytes(bytes: &[u8]) -> String {
        format!("{:?}", bytes)
    }

    proptest! {
        // Test that inserting and retrieving works for arbitrary data
        #[test]
        fn prop_insert_get(key: Vec<u8>, value: Vec<u8>) {
            let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();

            map.insert(key.clone(), value.clone());
            let result = map.get(&key);

            prop_assert_eq!(
                result,
                Some(value.as_ref()),
                "Failed with key: {}, value: {}",
                debug_bytes(&key),
                debug_bytes(&value)
            );
        }

        // Test that the map behaves like a standard HashMap
        #[test]
        fn prop_matches_std_hashmap(
            operations: Vec<(Vec<u8>, Vec<u8>)>,
            lookups: Vec<Vec<u8>>
        ) {
            // Limit test size for performance
            let operations = operations.into_iter().take(50).collect::<Vec<_>>();
            let lookups = lookups.into_iter().take(20).collect::<Vec<_>>();

            let mut our_map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();
            let mut std_map = HashMap::new();

            // Perform operations
            for (key, value) in operations {
                our_map.insert(key.clone(), value.clone());
                std_map.insert(key, value);
            }

            // Test lookups
            for key in lookups {
                let our_result = our_map.get(&key);
                let std_result = std_map.get(&key).map(|v| v.as_slice());

                prop_assert_eq!(
                    our_result,
                    std_result,
                    "Results differ for key: {}",
                    debug_bytes(&key)
                );
            }
        }

        // Test that the map correctly handles updates to existing keys
        #[test]
        fn prop_update_values(
            key: Vec<u8>,
            value1: Vec<u8>,
            value2: Vec<u8>
        ) {
            let mut map: OpenHashMap<_, _, _, _, Vec<u8>, Vec<u8>> = OpenHashMap::default();

            map.insert(key.clone(), value1);
            map.insert(key.clone(), value2.clone());

            let result = map.get(&key);
            prop_assert_eq!(
                result,
                Some(value2.as_ref()),
                "Failed to update key: {}",
                debug_bytes(&key)
            );
        }
    }
}
