use std::hash::BuildHasher;

use crate::disk_map::DiskHashMap;
use crate::error::Result;
use crate::heap::HeapOps;
use crate::types::BytesDecode;
use crate::{ByteStore, Heap};

/// Iterator over key-value pairs in a DiskHashMap
pub struct Iter<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
{
    map: &'a DiskHashMap<K, V, BS, S>,
    current_index: usize,
    remaining: usize,
}

impl<'a, K, V, BS, S> Iter<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
{
    pub(crate) fn new(map: &'a DiskHashMap<K, V, BS, S>) -> Self
    where
        Heap<BS>: HeapOps<BS>,
    {
        Self {
            map,
            current_index: 0,
            remaining: map.len(),
        }
    }
}

impl<'a, K, V, BS, S> Iterator for Iter<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
    K: for<'b> BytesDecode<'b>,
    V: for<'b> BytesDecode<'b>,
    Heap<BS>: HeapOps<BS>,
{
    type Item = Result<(<K as BytesDecode<'a>>::DItem, <V as BytesDecode<'a>>::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        // Find next occupied entry
        while self.current_index < self.map.capacity() {
            let entry = &self.map.entries()[self.current_index];
            
            if entry.is_occupied() {
                // Get key and value from heap
                let key_bytes = match self.map.heap().get(entry.key_pos()) {
                    Some(bytes) => bytes,
                    None => {
                        self.current_index += 1;
                        return Some(Err(crate::error::DiskMapError::Decoding(
                            "Key not found in heap".to_string()
                        )));
                    }
                };

                let value_bytes = match self.map.heap().get(entry.value_pos()) {
                    Some(bytes) => bytes,
                    None => {
                        self.current_index += 1;
                        return Some(Err(crate::error::DiskMapError::Decoding(
                            "Value not found in heap".to_string()
                        )));
                    }
                };

                // Decode key and value
                let key = match K::bytes_decode(key_bytes) {
                    Ok(k) => k,
                    Err(e) => {
                        self.current_index += 1;
                        return Some(Err(e));
                    }
                };

                let value = match V::bytes_decode(value_bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        self.current_index += 1;
                        return Some(Err(e));
                    }
                };

                self.current_index += 1;
                self.remaining -= 1;
                return Some(Ok((key, value)));
            }

            self.current_index += 1;
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<K, V, BS, S> ExactSizeIterator for Iter<'_, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
    K: for<'b> BytesDecode<'b>,
    V: for<'b> BytesDecode<'b>,
    Heap<BS>: HeapOps<BS>,
{
    fn len(&self) -> usize {
        self.remaining
    }
}

/// Iterator over keys in a DiskHashMap
pub struct Keys<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
{
    inner: Iter<'a, K, V, BS, S>,
}

impl<'a, K, V, BS, S> Keys<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
{
    pub(crate) fn new(map: &'a DiskHashMap<K, V, BS, S>) -> Self 
    where
        Heap<BS>: HeapOps<BS>,
    {
        Self {
            inner: Iter::new(map),
        }
    }
}

impl<'a, K, V, BS, S> Iterator for Keys<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
    K: for<'b> BytesDecode<'b>,
    V: for<'b> BytesDecode<'b>,
    Heap<BS>: HeapOps<BS>,
{
    type Item = Result<<K as BytesDecode<'a>>::DItem>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|result| result.map(|(k, _)| k))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K, V, BS, S> ExactSizeIterator for Keys<'_, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
    K: for<'b> BytesDecode<'b>,
    V: for<'b> BytesDecode<'b>,
    Heap<BS>: HeapOps<BS>,
{
    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Iterator over values in a DiskHashMap
pub struct Values<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
{
    inner: Iter<'a, K, V, BS, S>,
}

impl<'a, K, V, BS, S> Values<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
{
    pub(crate) fn new(map: &'a DiskHashMap<K, V, BS, S>) -> Self 
    where
        Heap<BS>: HeapOps<BS>,
    {
        Self {
            inner: Iter::new(map),
        }
    }
}

impl<'a, K, V, BS, S> Iterator for Values<'a, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
    K: for<'b> BytesDecode<'b>,
    V: for<'b> BytesDecode<'b>,
    Heap<BS>: HeapOps<BS>,
{
    type Item = Result<<V as BytesDecode<'a>>::DItem>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|result| result.map(|(_, v)| v))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K, V, BS, S> ExactSizeIterator for Values<'_, K, V, BS, S>
where
    BS: ByteStore,
    S: BuildHasher,
    K: for<'b> BytesDecode<'b>,
    V: for<'b> BytesDecode<'b>,
    Heap<BS>: HeapOps<BS>,
{
    fn len(&self) -> usize {
        self.inner.len()
    }
}

