use std::marker::PhantomData;
use std::ops::{Index, IndexMut};

use crate::entry::Entry;
use crate::error::Result;
use crate::fixed_buffers::FixedVec;
use crate::ByteStore;

/// Trait abstracting entry storage operations for DiskHashMap
/// 
/// This trait abstracts the operations performed on the entries array,
/// allowing for different implementations such as:
/// - Single array implementation (current `FixedVec<Entry, BS>`)
/// - Double array implementation for incremental resizing (future)
pub trait EntriesStorage<BS: ByteStore>: Index<usize, Output = Entry> + IndexMut<usize> {
    /// Returns the current capacity of the entries storage
    fn capacity(&self) -> usize;

    /// Returns the number of entries that are currently occupied
    fn occupied_count(&self) -> usize;

    /// Creates a new entries storage with the specified capacity
    fn new_with_capacity(store: BS, capacity: usize) -> Result<Self>
    where
        Self: Sized;

    /// Creates a new empty entries storage with increased capacity
    fn new_empty(&self, new_capacity: usize) -> Self;

    /// Returns true if the storage is currently being resized (for double array impl)
    fn is_resizing(&self) -> bool {
        false
    }

    /// Returns the effective capacity during resize operations
    /// For single array: returns capacity()
    /// For double array: returns old_capacity + new_capacity during resize
    fn effective_capacity(&self) -> usize {
        self.capacity()
    }

    /// Starts a resize operation (for double array implementation)
    fn start_resize(&mut self, _new_capacity: usize) -> Result<()> {
        Ok(()) // No-op for single array
    }

    /// Performs incremental rehashing during resize (for double array impl)
    /// Returns the number of entries that were rehashed
    fn incremental_rehash(&mut self, _max_entries: usize) -> usize {
        0 // No-op for single array
    }

    /// Completes the resize operation (for double array implementation)
    fn complete_resize(&mut self) -> Result<()> {
        Ok(()) // No-op for single array
    }

    /// Gets an entry at the given index, handling resize state
    /// For single array: simple index access
    /// For double array: checks both old and new arrays based on resize state
    fn get_entry(&self, index: usize) -> &Entry {
        &self[index]
    }

    /// Gets a mutable entry at the given index, handling resize state
    fn get_entry_mut(&mut self, index: usize) -> &mut Entry {
        &mut self[index]
    }

    /// Sets an entry at the given index, handling resize state
    fn set_entry(&mut self, index: usize, entry: Entry) {
        self[index] = entry;
    }

    /// Iterator over all occupied entries (for iteration support)
    fn occupied_entries(&self) -> OccupiedEntriesIter<'_, Self, BS>
    where
        Self: Sized,
    {
        OccupiedEntriesIter {
            entries: self,
            current_index: 0,
            remaining: self.occupied_count(),
            _phantom: PhantomData,
        }
    }

    /// Finds the next probe index for the given hash and current index
    /// This method can be overridden for different probing strategies
    fn next_probe_index(&self, current: usize, _hash: u64) -> usize {
        (current + 1) % self.effective_capacity()
    }

    /// Calculates the starting probe index for the given hash
    fn probe_start(&self, hash: u64) -> usize {
        (hash as usize) % self.effective_capacity()
    }
}

/// Iterator over occupied entries in the entries storage
pub struct OccupiedEntriesIter<'a, E: EntriesStorage<BS> + ?Sized, BS: ByteStore> {
    entries: &'a E,
    current_index: usize,
    remaining: usize,
    _phantom: PhantomData<BS>,
}

impl<'a, E: EntriesStorage<BS>, BS: ByteStore> Iterator for OccupiedEntriesIter<'a, E, BS> {
    type Item = (usize, &'a Entry);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        while self.current_index < self.entries.effective_capacity() {
            let entry = self.entries.get_entry(self.current_index);
            let index = self.current_index;
            self.current_index += 1;

            if entry.is_occupied() {
                self.remaining -= 1;
                return Some((index, entry));
            }
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<E: EntriesStorage<BS>, BS: ByteStore> ExactSizeIterator for OccupiedEntriesIter<'_, E, BS> {
    fn len(&self) -> usize {
        self.remaining
    }
}

/// Single array implementation of EntriesStorage (current implementation)
pub struct SingleArrayEntries<BS: ByteStore> {
    entries: FixedVec<Entry, BS>,
    occupied_count: usize,
}

impl<BS: ByteStore> SingleArrayEntries<BS> {
    pub fn new(entries: FixedVec<Entry, BS>) -> Self {
        let occupied_count = entries.iter().filter(|e| e.is_occupied()).count();
        Self {
            entries,
            occupied_count,
        }
    }

    pub fn into_inner(self) -> FixedVec<Entry, BS> {
        self.entries
    }

    pub fn inner(&self) -> &FixedVec<Entry, BS> {
        &self.entries
    }

    pub fn inner_mut(&mut self) -> &mut FixedVec<Entry, BS> {
        &mut self.entries
    }
}

impl<BS: ByteStore> Index<usize> for SingleArrayEntries<BS> {
    type Output = Entry;

    fn index(&self, index: usize) -> &Self::Output {
        &self.entries[index]
    }
}

impl<BS: ByteStore> IndexMut<usize> for SingleArrayEntries<BS> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.entries[index]
    }
}

impl<BS: ByteStore> EntriesStorage<BS> for SingleArrayEntries<BS> {
    fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    fn occupied_count(&self) -> usize {
        self.occupied_count
    }

    fn new_with_capacity(store: BS, capacity: usize) -> Result<Self> {
        let entries = FixedVec::new(store);
        // Verify that the capacity matches what we expect
        assert!(
            entries.capacity() >= capacity,
            "Store capacity {} is less than requested capacity {}",
            entries.capacity(),
            capacity
        );
        Ok(Self {
            entries,
            occupied_count: 0,
        })
    }

    fn new_empty(&self, new_capacity: usize) -> Self {
        let new_entries = self.entries.new_empty(new_capacity);
        Self {
            entries: new_entries,
            occupied_count: 0,
        }
    }

    fn set_entry(&mut self, index: usize, entry: Entry) {
        let old_entry = &self.entries[index];
        let old_occupied = old_entry.is_occupied();
        let new_occupied = entry.is_occupied();

        self.entries[index] = entry;

        // Update occupied count
        match (old_occupied, new_occupied) {
            (false, true) => self.occupied_count += 1,
            (true, false) => self.occupied_count -= 1,
            _ => {} // No change
        }
    }
}

/// Future implementation: Double array entries for incremental resizing
/// This is a placeholder/documentation for the future implementation
#[allow(dead_code)]
pub struct DoubleArrayEntries<BS: ByteStore> {
    old_entries: Option<FixedVec<Entry, BS>>,
    new_entries: FixedVec<Entry, BS>,
    resize_progress: usize, // Index of the last rehashed entry in old_entries
    occupied_count: usize,
}

// Implementation would go here for DoubleArrayEntries
// This will be the subject of the entries.md plan document