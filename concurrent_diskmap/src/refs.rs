use std::hash::BuildHasher;

use opendiskmap::{ByteStore, BytesDecode, BytesEncode, entry::Entry};
use parking_lot::RwLockReadGuard;

pub struct Ref<'a, K, V, BS: ByteStore, S: BuildHasher>
where
    K: for<'b> BytesEncode<'b>,
    V: for<'b> BytesEncode<'b> + for<'b> BytesDecode<'b>,
    BS: ByteStore,
    S: BuildHasher,
{
    pub(crate) shard: RwLockReadGuard<'a, opendiskmap::DiskHashMap<K, V, BS, S>>,
    pub(crate) entry: Entry,
}

impl<'a, K, V, BS: ByteStore, S: BuildHasher> Ref<'a, K, V, BS, S>
where
    K: for<'b> BytesEncode<'b> + for<'b> BytesDecode<'b>,
    V: for<'b> BytesEncode<'b> + for<'b> BytesDecode<'b>,
    BS: ByteStore,
    S: BuildHasher,
    // <K as opendiskmap::BytesEncode<'a>>::EItem: Sized,
    // <V as opendiskmap::BytesDecode<'a>>::DItem: Sized,
{
    pub fn new_from_key(
        key: &'a <K as BytesEncode<'a>>::EItem,
        shard: RwLockReadGuard<'a, opendiskmap::DiskHashMap<K, V, BS, S>>,
    ) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        let entry = shard.find_entry(key)?;
        let reader = entry.map(|e| Self { shard, entry: e });
        Ok(reader)
    }

    pub fn key(
        &'a self,
    ) -> Result<<K as BytesDecode<'a>>::DItem, Box<dyn std::error::Error + Send + Sync>> {
        self.shard.get_key(&self.entry)
    }

    pub fn value(
        &'a self,
    ) -> Result<<V as BytesDecode<'a>>::DItem, Box<dyn std::error::Error + Send + Sync>> {
        self.shard.get_value(&self.entry)
    }
}
