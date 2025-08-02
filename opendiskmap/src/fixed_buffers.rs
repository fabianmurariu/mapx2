use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use bytemuck::Pod;

use crate::byte_store::ByteStore;

/// A vector backed by a ByteStore that only accepts types `T` which are Pod (Plain Old Data)
/// and can be represented as a slice of bytes.
pub struct FixedVec<T, S: ByteStore> {
    store: S,
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
            capacity,
            _marker: PhantomData,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    fn store_offsets(&self) -> (usize, usize) {
        let size_of_t = std::mem::size_of::<T>();
        let start = 0; // Start at the beginning of the store
        let end = self.capacity * size_of_t; // End at capacity * size_of<T>
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

    pub fn new_empty(&self, new_len: usize) -> Self {
        assert!(new_len > self.capacity);
        let new_capacity = new_len.next_power_of_two();
        let t_size = std::mem::size_of::<T>();
        let additional = (new_capacity - self.capacity) * t_size;
        let new_empty_store = self.store.grow_new_empty(additional);
        Self {
            store: new_empty_store,
            capacity: new_capacity,
            _marker: PhantomData,
        }
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
