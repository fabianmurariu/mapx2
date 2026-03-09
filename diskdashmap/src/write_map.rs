use crossbeam_utils::CachePadded;
use diskhashmap::{ByteStore, BytesEncode, DiskHashMap};
use parking_lot::{RwLock, RwLockWriteGuard};
use rustc_hash::FxBuildHasher;
use std::hash::BuildHasher;
pub type WriteShard<'a, K, V, BS, S> = CachePadded<RwLockWriteGuard<'a, DiskHashMap<K, V, BS, S>>>;

pub struct WriteDiskDashMap<'a, K, V, BS, S = FxBuildHasher>
where
    BS: ByteStore,
    S: BuildHasher,
{
    shards: Box<[WriteShard<'a, K, V, BS, S>]>,
    hasher: S,
    shift: usize,
}

impl<'a, K, V, BS, S> WriteDiskDashMap<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
{
    pub fn new(
        shards: impl IntoIterator<Item = RwLockWriteGuard<'a, DiskHashMap<K, V, BS, S>>>,
        hasher: S,
        shift: usize,
    ) -> Self {
        Self {
            shards: shards.into_iter().map(CachePadded::new).collect(),
            hasher,
            shift,
        }
    }
}

impl<'a, K, V, BS, S> WriteDiskDashMap<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
{
    /// Get the shard index for a given key.
    /// Uses a different hash calculation than the individual shard's find_slot to ensure
    /// better distribution and avoid hash collisions between shard selection and slot selection.
    fn shard_for_key<'b>(
        &mut self,
        key: &'b <K as BytesEncode<'b>>::EItem,
    ) -> diskhashmap::Result<&mut WriteShard<'a, K, V, BS, S>>
    where
        K: BytesEncode<'b>,
    {
        let (_, key_bytes) = K::bytes_encode(key)?;
        let mut hasher = self.hasher.build_hasher();
        let base_hash = K::hash_alt(&key_bytes, &mut hasher);

        // Ok(base_hash as usize % self.shards.len())
        let idx = (base_hash as usize) & ((1 << self.shift) - 1);
        Ok(&mut self.shards[idx])
    }
}
