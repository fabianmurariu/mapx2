use std::hash::BuildHasher;
use std::io;
use std::path::Path;
use std::sync::OnceLock;

pub mod refs;

use crossbeam_utils::CachePadded;
use opendiskmap::{
    ByteStore, DiskHashMap as OpenDiskHM, MMapFile, Result,
    types::{BytesDecode, BytesEncode},
};
use parking_lot::RwLock;
use rustc_hash::FxBuildHasher;

use crate::refs::Ref;

fn default_shard_amount() -> usize {
    static DEFAULT_SHARD_AMOUNT: OnceLock<usize> = OnceLock::new();
    *DEFAULT_SHARD_AMOUNT.get_or_init(|| {
        (std::thread::available_parallelism().map_or(1, usize::from) * 4).next_power_of_two()
    })
}

/// A concurrent, sharded disk-backed hash map.
/// Each shard is a separate disk-backed hash map stored in its own subdirectory.
/// Generic over key type K, value type V, backing store BS, and hasher S.
pub struct DiskDashMap<K, V, BS, S = FxBuildHasher>
where
    BS: ByteStore,
    S: BuildHasher,
{
    shift: usize,
    shards: Box<[CachePadded<RwLock<OpenDiskHM<K, V, BS, S>>>]>,
    hasher: S,
}

// For MMapFile backing store with default hasher
impl<K, V> DiskDashMap<K, V, opendiskmap::MMapFile, FxBuildHasher>
where
    K: for<'a> BytesEncode<'a> + for<'a> BytesDecode<'a>,
    V: for<'a> BytesEncode<'a> + for<'a> BytesDecode<'a>,
{
    /// Create a new concurrent disk hash map in the given directory with default number of shards.
    pub fn new_in<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        Self::with_shards_in(dir, default_shard_amount())
    }

    /// Load an existing concurrent disk hash map from the given directory.
    pub fn load_from<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        Self::load_with_hasher_from(dir, FxBuildHasher)
    }

    /// Create a new concurrent disk hash map with specified number of shards.
    pub fn with_shards_in<P: AsRef<Path>>(dir: P, shard_count: usize) -> io::Result<Self> {
        Self::with_hasher_and_shards_in(dir, FxBuildHasher, shard_count, |path| {
            OpenDiskHM::new_in(path)
        })
    }

    /// Create a new concurrent disk hash map with capacity pre-allocated for keys and values and entries
    /// the num_keys, key_capacity, and values_capacity parameters are per each shard
    pub fn new_with_capacity<P: AsRef<Path>>(
        dir: P,
        num_keys: usize,
        keys_capacity: usize,
        values_capacity: usize,
    ) -> io::Result<Self> {
        Self::with_hasher_and_shards_in(dir, FxBuildHasher, default_shard_amount(), |path| {
            OpenDiskHM::with_capacity(path, num_keys, keys_capacity, values_capacity)
        })
    }
}

impl<K, V, S> DiskDashMap<K, V, opendiskmap::MMapFile, S>
where
    S: BuildHasher + Clone + Default,
    K: for<'a> BytesEncode<'a> + for<'a> BytesDecode<'a>,
    V: for<'a> BytesEncode<'a> + for<'a> BytesDecode<'a>,
{
    /// Create a new concurrent disk hash map with a custom hasher and number of shards.
    pub fn with_hasher_and_shards_in<P: AsRef<Path>>(
        dir: P,
        hasher: S,
        shard_count: usize,
        builder: impl Fn(&Path) -> io::Result<OpenDiskHM<K, V, MMapFile, S>>,
    ) -> io::Result<Self> {
        let shard_count = shard_count.next_power_of_two();
        let shift = shard_count.trailing_zeros() as usize;
        let dir = dir.as_ref();

        // Create directory if it doesn't exist
        std::fs::create_dir_all(dir)?;

        let mut shards = Vec::with_capacity(shard_count);

        // Create each shard
        for i in 0..shard_count {
            let shard_dir = dir.join(i.to_string());
            std::fs::create_dir_all(&shard_dir)?;

            // Create a hash map for this shard
            let map = builder(&shard_dir)?;
            shards.push(CachePadded::new(RwLock::new(map)));
        }

        Ok(Self {
            shift,
            shards: shards.into_boxed_slice(),
            hasher,
        })
    }

    /// Load an existing concurrent disk hash map with a custom hasher.
    pub fn load_with_hasher_from<P: AsRef<Path>>(dir: P, hasher: S) -> io::Result<Self> {
        let dir = dir.as_ref();

        if !dir.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Directory does not exist",
            ));
        }

        // Find all numeric subdirectories
        let mut shard_indices = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Ok(index) = name.parse::<usize>() {
                        shard_indices.push(index);
                    }
                }
            }
        }

        if shard_indices.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No shard directories found",
            ));
        }

        // Determine shard count (next power of 2 that fits all indices)
        let max_index = *shard_indices.iter().max().unwrap();
        let shard_count = (max_index + 1).next_power_of_two();
        let shift = shard_count.trailing_zeros() as usize;

        let mut shards = Vec::with_capacity(shard_count);

        // Load or create each shard
        for i in 0..shard_count {
            let shard_dir = dir.join(i.to_string());

            let map = if shard_indices.contains(&i) {
                // Load existing shard
                OpenDiskHM::load_from(&shard_dir)?
            } else {
                // Create empty shard for missing indices
                std::fs::create_dir_all(&shard_dir)?;
                OpenDiskHM::new_in(&shard_dir)?
            };

            shards.push(CachePadded::new(RwLock::new(map)));
        }

        Ok(Self {
            shift,
            shards: shards.into_boxed_slice(),
            hasher,
        })
    }
}

impl<K, V, BS, S> DiskDashMap<K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
    K: for<'a> BytesEncode<'a> + for<'a> BytesDecode<'a>,
    V: for<'a> BytesEncode<'a> + for<'a> BytesDecode<'a>,
{
    pub fn shards(&self) -> &[CachePadded<RwLock<OpenDiskHM<K, V, BS, S>>>] {
        &self.shards
    }

    /// Get the shard index for a given key.
    /// Uses a different hash calculation than the individual shard's find_slot to ensure
    /// better distribution and avoid hash collisions between shard selection and slot selection.
    fn shard_for_key<'a>(&self, key: &'a <K as BytesEncode<'a>>::EItem) -> Result<usize> {
        let key_bytes = K::bytes_encode(key)?;
        let mut hasher = self.hasher.build_hasher();
        let base_hash = K::hash_alt(&key_bytes, &mut hasher);

        // Ok(base_hash as usize % self.shards.len())
        Ok((base_hash as usize) & ((1 << self.shift) - 1))
    }

    /// Get a value from the map.
    pub fn get<'a>(
        &'a self,
        key: &'a <K as BytesEncode<'a>>::EItem,
    ) -> Result<Option<Ref<'a, K, V, BS, S>>> {
        let shard_idx = self.shard_for_key(key)?;
        let shard = self.shards[shard_idx].read();
        Ref::new_from_key(key, shard)
    }

    /// Insert a key-value pair into the map.
    pub fn insert<'a>(
        &'a self,
        key: &'a <K as BytesEncode<'a>>::EItem,
        value: &'a <V as BytesEncode<'a>>::EItem,
    ) -> Result<()> {
        let shard_idx = self.shard_for_key(key)?;
        let mut shard = self.shards[shard_idx].write();

        shard.insert(key, value)?;
        Ok(())
    }

    /// Remove a key from the map.
    /// Note: Current opendiskmap doesn't support remove operation
    pub fn remove<'a>(
        &self,
        _key: &'a <K as BytesEncode<'a>>::EItem,
    ) -> Result<Option<<V as BytesDecode<'a>>::DItem>> {
        // TODO: Implement when opendiskmap supports remove
        Ok(None)
    }

    /// Check if the map contains a key.
    pub fn contains_key<'a>(&self, key: &'a <K as BytesEncode<'a>>::EItem) -> Result<bool> {
        let shard_idx = self.shard_for_key(key)?;
        let shard = self.shards[shard_idx].read();
        Ok(shard.get(key)?.is_some())
    }

    /// Get the number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Get an approximate length of the map (sum of all shard lengths).
    /// This is approximate because shards can be modified concurrently.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.read().len()).sum()
    }

    /// Check if the map is empty.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.read().is_empty())
    }

    /// Clear all entries from the map.
    /// Note: Current opendiskmap doesn't support clear operation
    pub fn clear(&self) {
        // TODO: Implement when opendiskmap supports clear
        // For now, this is a no-op
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendiskmap::{MMapFile, types::Str};
    use rustc_hash::FxBuildHasher;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_basic_operations() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let map: DiskDashMap<Str, Str, MMapFile, FxBuildHasher> =
            DiskDashMap::new_in(temp_dir.path()).unwrap();

        // Test insert and get
        let key1 = "key1";
        let value1 = "value1";
        map.insert(key1, value1)?;
        assert_eq!(map.get(key1)?.unwrap().value()?, "value1");

        // Test contains_key
        assert!(map.contains_key(key1)?);
        let key2 = "key2";
        assert!(!map.contains_key(key2)?);

        // Test update
        let value2 = "value2";
        map.insert(key1, value2)?;
        assert_eq!(map.get(key1)?.unwrap().value()?, "value2");

        // Test remove (not supported yet)
        let removed = map.remove(key1)?;
        assert!(removed.is_none()); // Remove not implemented yet
        assert!(map.contains_key(key1)?); // Should still be there
        Ok(())
    }

    #[test]
    fn test_concurrent_access() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let map: Arc<DiskDashMap<Str, Str, MMapFile, FxBuildHasher>> =
            Arc::new(DiskDashMap::new_in(temp_dir.path()).unwrap());

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let map = Arc::clone(&map);
                thread::spawn(move || {
                    for j in 0..100 {
                        let key = format!("key_{i}_{j}");
                        let value = format!("value_{i}_{j}");
                        map.insert(&key, &value)?;
                        assert_eq!(map.get(&key)?.unwrap().value()?, value);
                    }
                    Ok::<_, opendiskmap::DiskMapError>(())
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        // Verify all entries exist
        for i in 0..10 {
            for j in 0..100 {
                let key = format!("key_{i}_{j}");
                let expected_value = format!("value_{i}_{j}");
                assert_eq!(map.get(&key)?.unwrap().value()?, expected_value);
            }
        }

        assert_eq!(map.len(), 1000);
        Ok(())
    }

    #[test]
    fn test_load_from_existing() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();

        // Create map and insert some data using a deterministic hasher
        let shard_count = 8; // Use a fixed shard count for predictable testing
        {
            let map: DiskDashMap<Str, Str, MMapFile, FxBuildHasher> =
                DiskDashMap::with_hasher_and_shards_in(
                    temp_dir.path(),
                    FxBuildHasher,
                    shard_count,
                    OpenDiskHM::new_in,
                )
                .unwrap();
            let key1 = "key1".to_string();
            let value1 = "value1".to_string();
            let key2 = "key2".to_string();
            let value2 = "value2".to_string();
            let key3 = "key3".to_string();
            let value3 = "value3".to_string();
            map.insert(&key1, &value1)?;
            map.insert(&key2, &value2)?;
            map.insert(&key3, &value3)?;
        }

        // Load from existing directory using the same deterministic hasher
        let map: DiskDashMap<Str, Str, MMapFile, FxBuildHasher> =
            DiskDashMap::load_with_hasher_from(temp_dir.path(), FxBuildHasher).unwrap();
        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        let key3 = "key3".to_string();
        assert_eq!(map.get(&key1)?.unwrap().value()?, "value1");
        assert_eq!(map.get(&key2)?.unwrap().value()?, "value2");
        assert_eq!(map.get(&key3)?.unwrap().value()?, "value3");
        assert_eq!(map.len(), 3);
        Ok(())
    }

    #[test]
    fn test_shard_distribution() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let map: DiskDashMap<Str, Str, MMapFile, FxBuildHasher> =
            DiskDashMap::with_shards_in(temp_dir.path(), 8).unwrap();

        // Insert many keys and verify they're distributed across shards
        let mut shard_usage = [0; 8];
        for i in 0..1000 {
            let key = format!("key_{i}");
            let value = format!("value_{i}");
            let shard_idx = map.shard_for_key(&key)?;
            shard_usage[shard_idx] += 1;
            map.insert(&key, &value)?;
        }

        // Verify all shards are used (probabilistically very likely with 1000 keys)
        assert!(shard_usage.iter().all(|&count| count > 0));

        // Verify total count
        assert_eq!(shard_usage.iter().sum::<usize>(), 1000);
        assert_eq!(map.len(), 1000);
        Ok(())
    }

    #[test]
    fn test_clear() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let map: DiskDashMap<Str, Str, MMapFile, FxBuildHasher> =
            DiskDashMap::new_in(temp_dir.path()).unwrap();

        // Insert some data
        for i in 0..100 {
            let key = format!("key_{i}");
            let value = format!("value_{i}");
            map.insert(&key, &value)?;
        }
        assert_eq!(map.len(), 100);

        // Clear and verify (not supported yet)
        map.clear();
        // Clear is not implemented yet, so data should still be there
        assert_eq!(map.len(), 100);
        assert!(!map.is_empty());
        let key_0 = "key_0".to_string();
        assert!(map.contains_key(&key_0)?);
        Ok(())
    }

    #[test]
    fn test_concurrent_modifications() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let map: Arc<DiskDashMap<Str, Str, MMapFile, FxBuildHasher>> =
            Arc::new(DiskDashMap::new_in(temp_dir.path()).unwrap());

        // Insert initial data
        for i in 0..100 {
            let key = format!("key_{i}");
            let value = format!("value_{}", i * 2);
            map.insert(&key, &value)?;
        }

        let handles: Vec<_> = (0..4)
            .map(|thread_id| {
                let map = Arc::clone(&map);
                thread::spawn(move || {
                    match thread_id {
                        0 => {
                            // Reader thread
                            for i in 0..100 {
                                let key = format!("key_{i}");
                                if map.get(&key).unwrap().is_some() {
                                    // Just verify we can read
                                }
                            }
                        }
                        1 => {
                            // Writer thread - updates existing values
                            for i in 0..50 {
                                let key = format!("key_{i}");
                                let value = format!("updated_{}", i * 4);
                                map.insert(&key, &value).unwrap();
                            }
                        }
                        2 => {
                            // Remover thread (remove not implemented yet, so just read)
                            for i in 50..75 {
                                let key = format!("key_{i}");
                                let _ = map.get(&key); // Just read instead of remove
                            }
                        }
                        3 => {
                            // Inserter thread - adds new values
                            for i in 100..150 {
                                let key = format!("key_{i}");
                                let value = format!("new_{}", i * 2);
                                map.insert(&key, &value).unwrap();
                            }
                        }
                        _ => unreachable!(),
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final state (allowing for concurrent modifications)
        // Keys 0-49 should have updated values
        for i in 0..50 {
            let key = format!("key_{i}");
            if let Some(value) = map.get(&key)? {
                assert!(value.value()?.starts_with("updated_"));
            }
        }

        // Keys 50-74 should be removed (but remove isn't implemented yet)
        // So we'll skip this check for now
        // for i in 50..75 {
        //     let key = format!("key_{}", i);
        //     assert!(!map.contains_key(&key));
        // }

        // Keys 100-149 should have new values
        for i in 100..150 {
            let key = format!("key_{i}");
            if let Some(value) = map.get(&key)? {
                assert!(value.value()?.starts_with("new_"));
            }
        }
        Ok(())
    }
}
