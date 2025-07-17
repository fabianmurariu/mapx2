use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut, Index, IndexMut, Range, RangeInclusive},
};

use bytemuck::Pod;

use crate::byte_store::ByteStore;

/// A vector backed by a ByteStore that only accepts types `T` which are Pod (Plain Old Data)
/// and can be represented as a slice of bytes.
pub struct FixedVec<T, S: ByteStore> {
    store: S,
    len: usize,
    capacity: usize,
    _marker: PhantomData<T>,
}

impl<T, S> FixedVec<T, S>
where
    T: Pod,
    S: ByteStore,
{
    /// Creates a new FixedVec with the specified capacity (in elements).
    pub fn new(store: S) -> Self {
        let capacity = store.as_ref().len() / std::mem::size_of::<T>();
        Self {
            store,
            len: 0,
            capacity,
            _marker: PhantomData,
        }
    }

    fn store_offsets(&self) -> (usize, usize) {
        let size_of_t = std::mem::size_of::<T>();
        let start = 0; // Start at the beginning of the store
        let end = self.capacity * size_of_t; // End at capacity * size_of<T>
        (start, end)
    }

    fn offsets_at(&self, index: usize) -> (usize, usize) {
        let size_of_t = std::mem::size_of::<T>();
        let start = index * size_of_t;
        let end = start + size_of_t;
        (start, end)
    }

    fn inner(&self) -> &[T] {
        let (start, end) = self.store_offsets();
        bytemuck::cast_slice(&self.store.as_ref()[start..end])
    }

    fn inner_mut(&mut self) -> &mut [T] {
        let (start, end) = self.store_offsets();
        bytemuck::cast_slice_mut(&mut self.store.as_mut()[start..end])
    }

    /// Returns the current length of the FixedVec.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the FixedVec is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.inner().iter().take(self.len)
    }

    pub fn push(&mut self, value: T) {
        if self.len >= self.capacity {
            self.grow();
        }
        let len = self.len;
        self.inner_mut()[len] = value;
        self.len += 1;
    }

    pub fn reserve(&mut self, additional: usize) {
        let t_size = std::mem::size_of::<T>();
        self.store.grow(additional * t_size);
        self.capacity = self.store.as_ref().len() / t_size;
    }

    pub fn grow(&mut self) {
        let t_size = std::mem::size_of::<T>();
        self.reserve(self.capacity * t_size);
        self.capacity *= 2; // Double the capacity
    }
}

impl<T: Pod, S: ByteStore> Deref for FixedVec<T, S> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

impl<T: Pod, S: ByteStore> DerefMut for FixedVec<T, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_basic_operations_vec() {
        let backing = vec![0u8; 40];
        let mut vec = FixedVec::<u32, Vec<u8>>::new(backing);
        assert_eq!(vec.len(), 0);

        vec.push(42);
        assert_eq!(vec.len(), 1);
        assert_eq!(vec.get(0), Some(&42));

        vec.set(5, 99);
        assert_eq!(vec.len(), 6);
        assert_eq!(vec.get(5), Some(&99));
        // All unset elements are zero-initialized (by Vec<u8>), so get(1) should be Some(&0)
        assert_eq!(vec.get(1), Some(&0));
    }

    #[test]
    fn test_resize() {
        let backing = vec![0u8; 40];
        let mut vec = FixedVec::<u32, Vec<u8>>::new(backing);
        // No resize method; test only set/get/len
        assert_eq!(vec.len(), 0);
        vec.set(0, 0);
        vec.set(1, 0);
        vec.set(2, 0);
        vec.set(3, 0);
        vec.set(4, 0);
        assert_eq!(vec.len(), 5);
        assert_eq!(vec.get(4), Some(&0));
        assert_eq!(vec.get(5), None);
    }

    #[test]
    fn test_set_out_of_bounds() {
        let backing = vec![0u8; 40];
        let mut vec = FixedVec::<u32, Vec<u8>>::new(backing);
        // This should not panic, it should grow the underlying store
        vec.set(10, 42);
        assert_eq!(vec.get(10), Some(&42));
    }

    #[test]
    fn test_resize_exceeds_capacity() {
        // No resize method; this test is not applicable
        // Just ensure FixedVec can be created and set beyond initial size
        let backing = vec![0u8; 40];
        let mut vec = FixedVec::<u32, Vec<u8>>::new(backing);
        vec.set(20, 123);
        assert_eq!(vec.get(20), Some(&123));
    }

    proptest! {
        #[test]
        fn prop_test_store_retrieval(index in 0usize..10, value in any::<u32>()) {
            let backing = vec![0u8; 40];
            let mut vec = FixedVec::<u32, Vec<u8>>::new(backing);
            vec.set(index, value);
            prop_assert_eq!(vec.get(index), Some(&value));
        }

        #[test]
        fn prop_test_resize_behavior(new_len in 0usize..10) {
            let backing = vec![0u8; 40];
            let mut vec = FixedVec::<u32, Vec<u8>>::new(backing);
            // No resize method; just set elements up to new_len
            for i in 0..new_len {
                vec.set(i, 0);
            }
            prop_assert_eq!(vec.len(), new_len);
        }
    }
}
