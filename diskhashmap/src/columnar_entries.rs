//! Columnar entries storage with Robin Hood hashing support.
//!
//! This module provides a columnar layout for hash map entries, storing:
//! - State bits (2 bits per entry, packed in u64 chunks) for fast iteration
//! - PSL (Probe Sequence Length) for Robin Hood hashing (1 byte per entry)
//! - Entry structs (the existing Entry with k_pos, v_pos, status)
//!
//! This layout enables:
//! - Fast iteration via bitmap scanning
//! - Higher load factors (85%+) with Robin Hood hashing
//! - Better cache utilization for probing operations

use bytemuck::{Pod, Zeroable};

use crate::byte_store::ByteStore;
use crate::entry::Entry;

/// Entry state encoded as 2 bits (mirrors Entry's Status enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryState {
    /// Slot has never been used
    Empty = 0b00,
    /// Slot contains a valid key-value pair
    Occupied = 0b01,
    /// Slot previously contained data but was deleted (tombstone)
    Deleted = 0b10,
    /// Entry has been migrated during incremental resizing
    Moved = 0b11,
}

impl EntryState {
    #[inline]
    fn from_bits(bits: u8) -> Self {
        match bits & 0b11 {
            0b00 => EntryState::Empty,
            0b01 => EntryState::Occupied,
            0b10 => EntryState::Deleted,
            0b11 => EntryState::Moved,
            _ => unreachable!(),
        }
    }

    /// Convert from Entry's status methods
    pub fn from_entry(entry: &Entry) -> Self {
        if entry.is_occupied() {
            EntryState::Occupied
        } else if entry.is_deleted() {
            EntryState::Deleted
        } else if entry.is_moved() {
            EntryState::Moved
        } else {
            EntryState::Empty
        }
    }
}

/// Header stored at the beginning of the columnar storage.
#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq)]
#[repr(C)]
pub struct ColumnarHeader {
    /// Number of occupied entries
    pub len: u64,
    /// Total capacity (number of slots)
    pub capacity: u64,
}

impl ColumnarHeader {
    pub const SIZE: usize = 16;

    pub fn new(capacity: u64) -> Self {
        Self { len: 0, capacity }
    }
}

/// Columnar entries storage with Robin Hood hashing.
///
/// Memory layout:
/// ```text
/// [Header: 16 bytes]
///   - len: u64 (occupied count)
///   - capacity: u64
/// [State Array: ceil(capacity/32) * 8 bytes]
///   - 2 bits per entry, 32 entries per u64
/// [PSL Array: ceil(capacity/8) * 8 bytes]
///   - 1 byte per entry, aligned to 8 bytes
/// [Entries Array: capacity * 16 bytes]
///   - Entry structs (existing Entry type with k_pos, v_pos, status)
/// ```
#[derive(Debug)]
pub struct ColumnarEntries<BS: ByteStore> {
    store: BS,
    /// Cached offset to state array
    state_offset: usize,
    /// Cached offset to PSL array
    psl_offset: usize,
    /// Cached offset to entries array
    entries_offset: usize,
    /// Cached capacity
    capacity: usize,
}

impl<BS: ByteStore> ColumnarEntries<BS> {
    /// Size of Entry struct (16 bytes = 128 bits)
    const ENTRY_SIZE: usize = std::mem::size_of::<Entry>();

    /// Calculate byte offsets for each section given a capacity.
    ///
    /// Returns (state_offset, psl_offset, entries_offset, total_size)
    pub fn calculate_offsets(capacity: usize) -> (usize, usize, usize, usize) {
        let header_size = ColumnarHeader::SIZE;

        // State array: 2 bits per entry, 32 entries per u64
        let state_u64_count = (capacity + 31) / 32;
        let state_size = state_u64_count * 8;
        let state_offset = header_size;

        // PSL array: 1 byte per entry, aligned to 8 bytes
        let psl_size = (capacity + 7) / 8 * 8;
        let psl_offset = state_offset + state_size;

        // Entries array: 16 bytes per entry (Entry struct)
        let entries_offset = psl_offset + psl_size;
        let entries_size = capacity * Self::ENTRY_SIZE;

        let total_size = entries_offset + entries_size;

        (state_offset, psl_offset, entries_offset, total_size)
    }

    /// Calculate total bytes needed for a given capacity.
    pub fn bytes_needed(capacity: usize) -> usize {
        let (_, _, _, total_size) = Self::calculate_offsets(capacity);
        total_size
    }

    /// Create a new columnar entries storage with the given capacity.
    pub fn new(store: BS, capacity: usize) -> Self {
        let (state_offset, psl_offset, entries_offset, total_size) =
            Self::calculate_offsets(capacity);

        assert!(
            store.as_ref().len() >= total_size,
            "Store has {} bytes but needs {} bytes for capacity {}",
            store.as_ref().len(),
            total_size,
            capacity
        );

        let mut entries = Self {
            store,
            state_offset,
            psl_offset,
            entries_offset,
            capacity,
        };

        // Initialize header
        let header = ColumnarHeader::new(capacity as u64);
        entries.store.as_mut()[..ColumnarHeader::SIZE]
            .copy_from_slice(bytemuck::bytes_of(&header));

        entries
    }

    /// Load existing columnar entries from a store.
    pub fn from_existing(store: BS) -> Self {
        let header: &ColumnarHeader =
            bytemuck::from_bytes(&store.as_ref()[..ColumnarHeader::SIZE]);
        let capacity = header.capacity as usize;

        let (state_offset, psl_offset, entries_offset, _) = Self::calculate_offsets(capacity);

        Self {
            store,
            state_offset,
            psl_offset,
            entries_offset,
            capacity,
        }
    }

    /// Get a reference to the underlying store.
    pub fn store(&self) -> &BS {
        &self.store
    }

    /// Consume self and return the underlying store.
    pub fn into_store(self) -> BS {
        self.store
    }

    // === Header access ===

    fn header(&self) -> &ColumnarHeader {
        bytemuck::from_bytes(&self.store.as_ref()[..ColumnarHeader::SIZE])
    }

    fn header_mut(&mut self) -> &mut ColumnarHeader {
        bytemuck::from_bytes_mut(&mut self.store.as_mut()[..ColumnarHeader::SIZE])
    }

    /// Get number of occupied entries.
    pub fn len(&self) -> usize {
        self.header().len as usize
    }

    /// Get total capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // === State array access ===

    fn state_array(&self) -> &[u64] {
        let u64_count = (self.capacity + 31) / 32;
        let end = self.state_offset + u64_count * 8;
        bytemuck::cast_slice(&self.store.as_ref()[self.state_offset..end])
    }

    fn state_array_mut(&mut self) -> &mut [u64] {
        let u64_count = (self.capacity + 31) / 32;
        let end = self.state_offset + u64_count * 8;
        bytemuck::cast_slice_mut(&mut self.store.as_mut()[self.state_offset..end])
    }

    /// Get state at index (2 bits).
    #[inline]
    pub fn get_state(&self, index: usize) -> EntryState {
        debug_assert!(index < self.capacity);
        let u64_idx = index / 32;
        let bit_offset = (index % 32) * 2;
        let word = self.state_array()[u64_idx];
        let bits = ((word >> bit_offset) & 0b11) as u8;
        EntryState::from_bits(bits)
    }

    /// Set state at index (2 bits).
    #[inline]
    pub fn set_state(&mut self, index: usize, state: EntryState) {
        debug_assert!(index < self.capacity);
        let u64_idx = index / 32;
        let bit_offset = (index % 32) * 2;
        let state_array = self.state_array_mut();
        let word = &mut state_array[u64_idx];
        let mask = !(0b11u64 << bit_offset);
        *word = (*word & mask) | ((state as u64) << bit_offset);
    }

    // === PSL array access ===

    fn psl_array(&self) -> &[u8] {
        &self.store.as_ref()[self.psl_offset..self.psl_offset + self.capacity]
    }

    fn psl_array_mut(&mut self) -> &mut [u8] {
        let cap = self.capacity;
        let offset = self.psl_offset;
        &mut self.store.as_mut()[offset..offset + cap]
    }

    /// Get PSL (Probe Sequence Length) at index.
    #[inline]
    pub fn get_psl(&self, index: usize) -> u8 {
        debug_assert!(index < self.capacity);
        self.psl_array()[index]
    }

    /// Set PSL at index.
    #[inline]
    pub fn set_psl(&mut self, index: usize, psl: u8) {
        debug_assert!(index < self.capacity);
        self.psl_array_mut()[index] = psl;
    }

    // === Entry array access ===

    fn entries_slice(&self) -> &[Entry] {
        let end = self.entries_offset + self.capacity * Self::ENTRY_SIZE;
        bytemuck::cast_slice(&self.store.as_ref()[self.entries_offset..end])
    }

    fn entries_slice_mut(&mut self) -> &mut [Entry] {
        let cap = self.capacity;
        let offset = self.entries_offset;
        let end = offset + cap * Self::ENTRY_SIZE;
        bytemuck::cast_slice_mut(&mut self.store.as_mut()[offset..end])
    }

    /// Get Entry at index.
    #[inline]
    pub fn get_entry(&self, index: usize) -> Entry {
        debug_assert!(index < self.capacity);
        self.entries_slice()[index]
    }

    /// Set Entry at index.
    #[inline]
    pub fn set_entry_at(&mut self, index: usize, entry: Entry) {
        debug_assert!(index < self.capacity);
        self.entries_slice_mut()[index] = entry;
    }

    // === Combined access ===

    /// Get full entry data at index (state, psl, entry).
    #[inline]
    pub fn get_full_entry(&self, index: usize) -> (EntryState, u8, Entry) {
        (
            self.get_state(index),
            self.get_psl(index),
            self.get_entry(index),
        )
    }

    /// Set full entry at index (updates state, psl, and entry).
    #[inline]
    pub fn set_full_entry(&mut self, index: usize, state: EntryState, psl: u8, entry: Entry) {
        self.set_state(index, state);
        self.set_psl(index, psl);
        self.set_entry_at(index, entry);
    }

    // === Robin Hood operations ===

    /// Calculate slot for a hash value.
    #[inline]
    pub fn slot_for_hash(&self, hash: u64) -> usize {
        (hash as usize) % self.capacity
    }

    /// Robin Hood insert.
    ///
    /// Returns `Some(old_entry)` if the key already existed (update case),
    /// or `None` if this was a new insertion.
    ///
    /// The `key_eq` closure takes two entries and returns true if they represent
    /// the same key (by comparing the actual key data in the heap).
    pub fn robin_hood_insert(
        &mut self,
        hash: u64,
        entry: Entry,
        key_eq: impl Fn(&Entry, &Entry) -> bool,
    ) -> Option<Entry> {
        let capacity = self.capacity;
        let mut slot = self.slot_for_hash(hash);
        let mut current_psl: u8 = 0;
        let mut current_entry = entry;

        loop {
            let state = self.get_state(slot);

            match state {
                EntryState::Empty | EntryState::Deleted => {
                    // Found empty/deleted slot, insert here
                    self.set_full_entry(slot, EntryState::Occupied, current_psl, current_entry);
                    self.header_mut().len += 1;
                    return None;
                }
                EntryState::Occupied => {
                    let existing_entry = self.get_entry(slot);

                    // Check if this is the same key (update case)
                    if key_eq(&existing_entry, &current_entry) {
                        let old_entry = existing_entry;
                        self.set_entry_at(slot, current_entry);
                        return Some(old_entry);
                    }

                    let existing_psl = self.get_psl(slot);

                    // Robin Hood: if we've probed further than the existing entry,
                    // swap and continue with the displaced entry
                    if current_psl > existing_psl {
                        // Swap entries
                        self.set_psl(slot, current_psl);
                        self.set_entry_at(slot, current_entry);
                        current_entry = existing_entry;
                        current_psl = existing_psl;
                    }
                }
                EntryState::Moved => {
                    // During incremental resize, treat as empty for new entries
                    self.set_full_entry(slot, EntryState::Occupied, current_psl, current_entry);
                    self.header_mut().len += 1;
                    return None;
                }
            }

            // Move to next slot
            slot = (slot + 1) % capacity;
            current_psl = current_psl.saturating_add(1);

            // Safety check: PSL shouldn't exceed 255 with proper load factor
            debug_assert!(current_psl < 255, "PSL overflow - load factor too high");
        }
    }

    /// Robin Hood lookup.
    ///
    /// Returns `Some((slot_index, entry))` if found, `None` otherwise.
    ///
    /// The `key_eq` closure takes an entry and returns true if it matches the search key.
    pub fn robin_hood_find(
        &self,
        hash: u64,
        key_eq: impl Fn(&Entry) -> bool,
    ) -> Option<(usize, Entry)> {
        if self.capacity == 0 {
            return None;
        }

        let capacity = self.capacity;
        let mut slot = self.slot_for_hash(hash);
        let mut psl: u8 = 0;

        loop {
            let state = self.get_state(slot);

            match state {
                EntryState::Empty => {
                    // Empty slot means key doesn't exist
                    return None;
                }
                EntryState::Occupied => {
                    let existing_psl = self.get_psl(slot);

                    // Early termination: if we've probed more than the entry at this slot,
                    // the key doesn't exist (Robin Hood invariant)
                    if psl > existing_psl {
                        return None;
                    }

                    let entry = self.get_entry(slot);
                    if key_eq(&entry) {
                        return Some((slot, entry));
                    }
                }
                EntryState::Deleted | EntryState::Moved => {
                    // Continue probing past tombstones and moved entries
                }
            }

            slot = (slot + 1) % capacity;
            psl = psl.saturating_add(1);

            // Safety check
            if psl == 255 {
                return None;
            }
        }
    }

    /// Mark entry as deleted (tombstone).
    ///
    /// Returns `Some(entry)` if the slot was occupied, `None` otherwise.
    pub fn robin_hood_delete(&mut self, slot: usize) -> Option<Entry> {
        let state = self.get_state(slot);
        if state == EntryState::Occupied {
            let entry = self.get_entry(slot);
            self.set_state(slot, EntryState::Deleted);
            // Keep PSL for Robin Hood correctness during lookups
            self.header_mut().len -= 1;
            Some(entry)
        } else {
            None
        }
    }

    /// Check if resize is needed based on load factor.
    /// Default threshold is 85%.
    pub fn should_resize(&self) -> bool {
        self.should_resize_at(85)
    }

    /// Check if resize is needed at a specific load factor percentage.
    pub fn should_resize_at(&self, threshold_percent: u64) -> bool {
        if self.capacity == 0 {
            return true;
        }
        let load_percent = (self.len() as u64 * 100) / (self.capacity as u64);
        load_percent >= threshold_percent
    }

    // === Iteration ===

    /// Create an iterator over occupied entries.
    pub fn iter(&self) -> ColumnarEntriesIter<'_, BS> {
        ColumnarEntriesIter {
            entries: self,
            current_u64_idx: 0,
            bit_offset: 0,
            total_found: 0,
            max_entries: self.len(),
        }
    }

    /// Count occupied entries using bit manipulation.
    /// This is faster than iterating for statistics.
    pub fn count_occupied(&self) -> usize {
        self.state_array()
            .iter()
            .map(|&word| {
                // Count pairs where bits == 0b01 (Occupied)
                let low_bits = word & 0x5555555555555555; // Odd bits
                let high_bits = (word >> 1) & 0x5555555555555555; // Even bits
                let occupied = low_bits & !high_bits;
                occupied.count_ones() as usize
            })
            .sum()
    }

    /// Count deleted (tombstone) entries.
    pub fn count_deleted(&self) -> usize {
        self.state_array()
            .iter()
            .map(|&word| {
                // Count pairs where bits == 0b10 (Deleted)
                let low_bits = word & 0x5555555555555555;
                let high_bits = (word >> 1) & 0x5555555555555555;
                let deleted = !low_bits & high_bits;
                deleted.count_ones() as usize
            })
            .sum()
    }

    /// Create a new empty ColumnarEntries with larger capacity.
    /// Used during resize operations.
    pub fn grow_empty(&self, new_capacity: usize) -> Self {
        assert!(new_capacity > self.capacity);
        let new_capacity = new_capacity.next_power_of_two();
        let bytes_needed = Self::bytes_needed(new_capacity);
        let additional = bytes_needed.saturating_sub(self.store.as_ref().len());
        let new_store = self.store.grow_new_empty(additional);
        Self::new(new_store, new_capacity)
    }

    /// Purge the underlying store (for cleanup during resize).
    pub fn purge(self) {
        self.store.purge();
    }
}

/// Iterator over occupied entries in columnar storage.
pub struct ColumnarEntriesIter<'a, BS: ByteStore> {
    entries: &'a ColumnarEntries<BS>,
    current_u64_idx: usize,
    bit_offset: usize, // 0-63, represents which 2-bit pair we're at within the u64
    total_found: usize,
    max_entries: usize,
}

impl<'a, BS: ByteStore> Iterator for ColumnarEntriesIter<'a, BS> {
    type Item = (usize, Entry);

    fn next(&mut self) -> Option<Self::Item> {
        if self.total_found >= self.max_entries {
            return None;
        }

        let state_array = self.entries.state_array();
        let u64_count = state_array.len();

        while self.current_u64_idx < u64_count {
            let word = state_array[self.current_u64_idx];

            while self.bit_offset < 64 {
                let pair_idx = self.bit_offset / 2;
                let state_bits = (word >> self.bit_offset) & 0b11;

                self.bit_offset += 2;

                if state_bits == EntryState::Occupied as u64 {
                    let index = self.current_u64_idx * 32 + pair_idx;
                    if index >= self.entries.capacity {
                        return None;
                    }
                    let entry = self.entries.get_entry(index);
                    self.total_found += 1;
                    return Some((index, entry));
                }
            }

            self.current_u64_idx += 1;
            self.bit_offset = 0;
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.max_entries.saturating_sub(self.total_found);
        (remaining, Some(remaining))
    }
}

impl<'a, BS: ByteStore> ExactSizeIterator for ColumnarEntriesIter<'a, BS> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::byte_store::VecStore;
    use crate::HeapIdx;

    fn create_store(capacity: usize) -> VecStore {
        let bytes_needed = ColumnarEntries::<VecStore>::bytes_needed(capacity);
        let mut store = VecStore::with_capacity(bytes_needed);
        store.grow(bytes_needed);
        store
    }

    fn create_test_entry(k_offset: u64, v_offset: u64) -> Entry {
        let k_idx = HeapIdx::new().with_category(0).with_offset(k_offset);
        let v_idx = HeapIdx::new().with_category(0).with_offset(v_offset);
        Entry::occupied_at_pos(k_idx, v_idx)
    }

    #[test]
    fn test_header_initialization() {
        let store = create_store(64);
        let entries = ColumnarEntries::new(store, 64);

        assert_eq!(entries.len(), 0);
        assert_eq!(entries.capacity(), 64);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_state_operations() {
        let store = create_store(64);
        let mut entries = ColumnarEntries::new(store, 64);

        // All states should be Empty initially
        for i in 0..64 {
            assert_eq!(entries.get_state(i), EntryState::Empty);
        }

        // Set various states
        entries.set_state(0, EntryState::Occupied);
        entries.set_state(1, EntryState::Deleted);
        entries.set_state(31, EntryState::Moved);
        entries.set_state(32, EntryState::Occupied); // Second u64
        entries.set_state(63, EntryState::Deleted);

        assert_eq!(entries.get_state(0), EntryState::Occupied);
        assert_eq!(entries.get_state(1), EntryState::Deleted);
        assert_eq!(entries.get_state(2), EntryState::Empty);
        assert_eq!(entries.get_state(31), EntryState::Moved);
        assert_eq!(entries.get_state(32), EntryState::Occupied);
        assert_eq!(entries.get_state(63), EntryState::Deleted);
    }

    #[test]
    fn test_psl_operations() {
        let store = create_store(64);
        let mut entries = ColumnarEntries::new(store, 64);

        // All PSL should be 0 initially
        for i in 0..64 {
            assert_eq!(entries.get_psl(i), 0);
        }

        // Set various PSL values
        entries.set_psl(0, 1);
        entries.set_psl(1, 5);
        entries.set_psl(63, 255);

        assert_eq!(entries.get_psl(0), 1);
        assert_eq!(entries.get_psl(1), 5);
        assert_eq!(entries.get_psl(2), 0);
        assert_eq!(entries.get_psl(63), 255);
    }

    #[test]
    fn test_entry_operations() {
        let store = create_store(64);
        let mut entries = ColumnarEntries::new(store, 64);

        let entry1 = create_test_entry(100, 200);
        let entry2 = create_test_entry(300, 400);

        entries.set_entry_at(0, entry1);
        entries.set_entry_at(63, entry2);

        let retrieved1 = entries.get_entry(0);
        let retrieved2 = entries.get_entry(63);

        assert_eq!(retrieved1.key_pos().offset(), 100);
        assert_eq!(retrieved1.value_pos().offset(), 200);
        assert_eq!(retrieved2.key_pos().offset(), 300);
        assert_eq!(retrieved2.value_pos().offset(), 400);
    }

    #[test]
    fn test_robin_hood_insert_and_find() {
        let store = create_store(16);
        let mut entries = ColumnarEntries::new(store, 16);

        let entry1 = create_test_entry(100, 1000);
        let entry2 = create_test_entry(200, 2000);
        let entry3 = create_test_entry(300, 3000);

        // Insert entries
        let result = entries.robin_hood_insert(0, entry1, |a, b| a.key_pos() == b.key_pos());
        assert!(result.is_none());
        assert_eq!(entries.len(), 1);

        let result = entries.robin_hood_insert(1, entry2, |a, b| a.key_pos() == b.key_pos());
        assert!(result.is_none());
        assert_eq!(entries.len(), 2);

        let result = entries.robin_hood_insert(2, entry3, |a, b| a.key_pos() == b.key_pos());
        assert!(result.is_none());
        assert_eq!(entries.len(), 3);

        // Find entries
        let found = entries.robin_hood_find(0, |e| e.key_pos().offset() == 100);
        assert!(found.is_some());
        let (_slot, entry) = found.unwrap();
        assert_eq!(entry.key_pos().offset(), 100);
        assert_eq!(entry.value_pos().offset(), 1000);

        let found = entries.robin_hood_find(1, |e| e.key_pos().offset() == 200);
        assert!(found.is_some());

        let found = entries.robin_hood_find(5, |e| e.key_pos().offset() == 999);
        assert!(found.is_none());
    }

    #[test]
    fn test_robin_hood_update() {
        let store = create_store(16);
        let mut entries = ColumnarEntries::new(store, 16);

        let entry1 = create_test_entry(100, 1000);
        let entry2 = create_test_entry(100, 2000); // Same key, different value

        // Insert initial entry
        let result = entries.robin_hood_insert(0, entry1, |a, b| a.key_pos() == b.key_pos());
        assert!(result.is_none());
        assert_eq!(entries.len(), 1);

        // Update with same key
        let result = entries.robin_hood_insert(0, entry2, |a, b| a.key_pos() == b.key_pos());
        assert!(result.is_some());
        let old_entry = result.unwrap();
        assert_eq!(old_entry.value_pos().offset(), 1000);
        assert_eq!(entries.len(), 1); // Length shouldn't change

        // Verify updated value
        let found = entries.robin_hood_find(0, |e| e.key_pos().offset() == 100);
        assert!(found.is_some());
        let (_, entry) = found.unwrap();
        assert_eq!(entry.value_pos().offset(), 2000);
    }

    #[test]
    fn test_robin_hood_delete() {
        let store = create_store(16);
        let mut entries = ColumnarEntries::new(store, 16);

        let entry1 = create_test_entry(100, 1000);

        // Insert
        entries.robin_hood_insert(0, entry1, |a, b| a.key_pos() == b.key_pos());
        assert_eq!(entries.len(), 1);

        // Find the slot
        let found = entries.robin_hood_find(0, |e| e.key_pos().offset() == 100);
        let (slot, _) = found.unwrap();

        // Delete
        let deleted = entries.robin_hood_delete(slot);
        assert!(deleted.is_some());
        assert_eq!(entries.len(), 0);

        // Should not find anymore
        let found = entries.robin_hood_find(0, |e| e.key_pos().offset() == 100);
        assert!(found.is_none());
    }

    #[test]
    fn test_robin_hood_collision() {
        let store = create_store(16);
        let mut entries = ColumnarEntries::new(store, 16);

        // Insert entries that hash to the same slot
        let entry1 = create_test_entry(100, 1000);
        let entry2 = create_test_entry(200, 2000);

        // All hash to slot 0 (hash value 0)
        entries.robin_hood_insert(0, entry1, |a, b| a.key_pos() == b.key_pos());
        entries.robin_hood_insert(0, entry2, |a, b| a.key_pos() == b.key_pos()); // Different key, same hash

        assert_eq!(entries.len(), 2);

        // Both should be findable
        let found1 = entries.robin_hood_find(0, |e| e.key_pos().offset() == 100);
        assert!(found1.is_some());

        let found2 = entries.robin_hood_find(0, |e| e.key_pos().offset() == 200);
        assert!(found2.is_some());
    }

    #[test]
    fn test_robin_hood_swapping() {
        // Test that Robin Hood actually swaps entries based on PSL
        let store = create_store(16);
        let mut entries = ColumnarEntries::new(store, 16);

        // Insert entry at slot 0
        let entry1 = create_test_entry(100, 1000);
        entries.robin_hood_insert(0, entry1, |a, b| a.key_pos() == b.key_pos());

        // Insert entry at slot 1
        let entry2 = create_test_entry(200, 2000);
        entries.robin_hood_insert(1, entry2, |a, b| a.key_pos() == b.key_pos());

        // Now insert entry that hashes to slot 0 - it should probe to slot 1,
        // but entry2 has PSL=0 there while our new entry would have PSL=1
        // So our entry should NOT swap (PSL 1 > PSL 0 means we should swap)
        let entry3 = create_test_entry(300, 3000);
        entries.robin_hood_insert(0, entry3, |a, b| a.key_pos() == b.key_pos());

        // entry3 hashes to 0, finds it occupied (entry1), moves to slot 1
        // At slot 1, entry3 has PSL=1, entry2 has PSL=0
        // Since 1 > 0, entry3 displaces entry2
        // entry2 (now with PSL=0) continues probing to slot 2

        // Verify all three are findable
        assert!(entries.robin_hood_find(0, |e| e.key_pos().offset() == 100).is_some());
        assert!(entries.robin_hood_find(1, |e| e.key_pos().offset() == 200).is_some());
        assert!(entries.robin_hood_find(0, |e| e.key_pos().offset() == 300).is_some());

        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_iterator() {
        let store = create_store(64);
        let mut entries = ColumnarEntries::new(store, 64);

        // Insert some entries at various positions
        let test_entries = vec![
            (0u64, create_test_entry(100, 1000)),
            (5, create_test_entry(200, 2000)),
            (32, create_test_entry(300, 3000)),
            (63, create_test_entry(400, 4000)),
        ];

        for (hash, entry) in &test_entries {
            entries.robin_hood_insert(*hash, *entry, |a, b| a.key_pos() == b.key_pos());
        }

        assert_eq!(entries.len(), 4);

        // Collect iterator results
        let collected: Vec<_> = entries.iter().collect();
        assert_eq!(collected.len(), 4);

        // Verify all entries are in the results
        let offsets: Vec<u64> = collected.iter().map(|(_, e)| e.key_pos().offset()).collect();
        assert!(offsets.contains(&100));
        assert!(offsets.contains(&200));
        assert!(offsets.contains(&300));
        assert!(offsets.contains(&400));
    }

    #[test]
    fn test_count_occupied_and_deleted() {
        let store = create_store(64);
        let mut entries = ColumnarEntries::new(store, 64);

        // Insert entries
        for i in 0..10 {
            let entry = create_test_entry(i as u64 * 100, i as u64 * 1000);
            entries.robin_hood_insert(i as u64, entry, |a, b| a.key_pos() == b.key_pos());
        }

        assert_eq!(entries.count_occupied(), 10);
        assert_eq!(entries.count_deleted(), 0);

        // Delete some entries
        for i in 0..5 {
            let found =
                entries.robin_hood_find(i as u64, |e| e.key_pos().offset() == i as u64 * 100);
            if let Some((slot, _)) = found {
                entries.robin_hood_delete(slot);
            }
        }

        assert_eq!(entries.count_occupied(), 5);
        assert_eq!(entries.count_deleted(), 5);
    }

    #[test]
    fn test_should_resize() {
        let store = create_store(16);
        let mut entries = ColumnarEntries::new(store, 16);

        // Empty - shouldn't need resize
        assert!(!entries.should_resize());

        // Insert up to 85% load
        for i in 0..13 {
            let entry = create_test_entry(i as u64 * 100, i as u64 * 1000);
            entries.robin_hood_insert(i as u64, entry, |a, b| a.key_pos() == b.key_pos());
        }

        // 13/16 = 81.25% - shouldn't need resize yet
        assert!(!entries.should_resize());

        // One more should trigger resize (14/16 = 87.5%)
        let entry = create_test_entry(1300, 13000);
        entries.robin_hood_insert(13, entry, |a, b| a.key_pos() == b.key_pos());

        assert!(entries.should_resize());
    }

    #[test]
    fn test_grow_empty() {
        let store = create_store(16);
        let entries = ColumnarEntries::new(store, 16);

        let new_entries = entries.grow_empty(32);

        assert_eq!(new_entries.capacity(), 32);
        assert_eq!(new_entries.len(), 0);
        assert!(new_entries.is_empty());
    }

    #[test]
    fn test_from_existing() {
        let store = create_store(32);
        let mut entries = ColumnarEntries::new(store, 32);

        // Insert some data
        let entry = create_test_entry(100, 1000);
        entries.robin_hood_insert(5, entry, |a, b| a.key_pos() == b.key_pos());

        // Get the store and recreate
        let store = entries.into_store();
        let entries = ColumnarEntries::from_existing(store);

        assert_eq!(entries.capacity(), 32);
        assert_eq!(entries.len(), 1);

        let found = entries.robin_hood_find(5, |e| e.key_pos().offset() == 100);
        assert!(found.is_some());
    }

    #[test]
    fn test_entry_state_from_entry() {
        let entry = create_test_entry(100, 200);
        assert_eq!(EntryState::from_entry(&entry), EntryState::Occupied);

        let empty_entry = Entry::new();
        assert_eq!(EntryState::from_entry(&empty_entry), EntryState::Empty);
    }

    // === Persistence tests with MMapFile ===

    #[cfg(test)]
    mod persistence_tests {
        use super::*;
        use crate::byte_store::MMapFile;
        use tempfile::tempdir;

        fn create_mmap_store(path: &std::path::Path, capacity: usize) -> MMapFile {
            let bytes_needed = ColumnarEntries::<MMapFile>::bytes_needed(capacity);
            MMapFile::new(path, bytes_needed).expect("Failed to create MMapFile")
        }

        #[test]
        fn test_persistence_basic() {
            let dir = tempdir().expect("Failed to create temp dir");
            let path = dir.path().join("entries.bin");

            // Create and populate entries
            {
                let store = create_mmap_store(&path, 32);
                let mut entries = ColumnarEntries::new(store, 32);

                let entry1 = create_test_entry(100, 1000);
                let entry2 = create_test_entry(200, 2000);

                entries.robin_hood_insert(100, entry1, |a, b| a.key_pos() == b.key_pos());
                entries.robin_hood_insert(200, entry2, |a, b| a.key_pos() == b.key_pos());

                assert_eq!(entries.len(), 2);
            } // entries dropped, data persisted

            // Reload and verify
            {
                let store = MMapFile::from_file(&path).expect("Failed to load MMapFile");
                let entries = ColumnarEntries::<MMapFile>::from_existing(store);

                assert_eq!(entries.capacity(), 32);
                assert_eq!(entries.len(), 2);

                let found1 = entries.robin_hood_find(100, |e| e.key_pos().offset() == 100);
                assert!(found1.is_some());
                assert_eq!(found1.unwrap().1.value_pos().offset(), 1000);

                let found2 = entries.robin_hood_find(200, |e| e.key_pos().offset() == 200);
                assert!(found2.is_some());
                assert_eq!(found2.unwrap().1.value_pos().offset(), 2000);
            }
        }

        #[test]
        fn test_persistence_after_update() {
            let dir = tempdir().expect("Failed to create temp dir");
            let path = dir.path().join("entries.bin");

            // Create entries with initial value
            {
                let store = create_mmap_store(&path, 32);
                let mut entries = ColumnarEntries::new(store, 32);

                let entry = create_test_entry(100, 1000);
                entries.robin_hood_insert(100, entry, |a, b| a.key_pos() == b.key_pos());
            }

            // Reload, update, and persist
            {
                let store = MMapFile::from_file(&path).expect("Failed to load MMapFile");
                let mut entries = ColumnarEntries::<MMapFile>::from_existing(store);

                let entry = create_test_entry(100, 2000);
                let old = entries.robin_hood_insert(100, entry, |a, b| a.key_pos() == b.key_pos());
                assert!(old.is_some());
                assert_eq!(old.unwrap().value_pos().offset(), 1000);
            }

            // Reload and verify update persisted
            {
                let store = MMapFile::from_file(&path).expect("Failed to load MMapFile");
                let entries = ColumnarEntries::<MMapFile>::from_existing(store);

                let found = entries.robin_hood_find(100, |e| e.key_pos().offset() == 100);
                assert!(found.is_some());
                assert_eq!(found.unwrap().1.value_pos().offset(), 2000);
            }
        }

        #[test]
        fn test_persistence_after_delete() {
            let dir = tempdir().expect("Failed to create temp dir");
            let path = dir.path().join("entries.bin");

            // Create entries
            {
                let store = create_mmap_store(&path, 32);
                let mut entries = ColumnarEntries::new(store, 32);

                for i in 0..5 {
                    let entry = create_test_entry(i * 100, i * 1000);
                    entries.robin_hood_insert(i * 100, entry, |a, b| a.key_pos() == b.key_pos());
                }
                assert_eq!(entries.len(), 5);
            }

            // Reload, delete some, persist
            {
                let store = MMapFile::from_file(&path).expect("Failed to load MMapFile");
                let mut entries = ColumnarEntries::<MMapFile>::from_existing(store);

                // Delete entries 0 and 200
                let found = entries.robin_hood_find(0, |e| e.key_pos().offset() == 0);
                if let Some((slot, _)) = found {
                    entries.robin_hood_delete(slot);
                }

                let found = entries.robin_hood_find(200, |e| e.key_pos().offset() == 200);
                if let Some((slot, _)) = found {
                    entries.robin_hood_delete(slot);
                }

                assert_eq!(entries.len(), 3);
            }

            // Reload and verify deletions persisted
            {
                let store = MMapFile::from_file(&path).expect("Failed to load MMapFile");
                let entries = ColumnarEntries::<MMapFile>::from_existing(store);

                assert_eq!(entries.len(), 3);
                assert_eq!(entries.count_deleted(), 2);

                // Deleted entries should not be found
                assert!(entries.robin_hood_find(0, |e| e.key_pos().offset() == 0).is_none());
                assert!(entries.robin_hood_find(200, |e| e.key_pos().offset() == 200).is_none());

                // Remaining entries should be found
                assert!(entries.robin_hood_find(100, |e| e.key_pos().offset() == 100).is_some());
                assert!(entries.robin_hood_find(300, |e| e.key_pos().offset() == 300).is_some());
                assert!(entries.robin_hood_find(400, |e| e.key_pos().offset() == 400).is_some());
            }
        }

        #[test]
        fn test_persistence_psl_and_state() {
            let dir = tempdir().expect("Failed to create temp dir");
            let path = dir.path().join("entries.bin");

            // Create entries with collisions to generate non-zero PSL
            {
                let store = create_mmap_store(&path, 16);
                let mut entries = ColumnarEntries::new(store, 16);

                // Insert multiple entries that hash to the same slot
                for i in 0..5 {
                    let entry = create_test_entry(i * 16, i * 1000); // All hash to slot 0 in cap 16
                    entries.robin_hood_insert(0, entry, |a, b| a.key_pos() == b.key_pos());
                }
            }

            // Reload and verify PSL and states
            {
                let store = MMapFile::from_file(&path).expect("Failed to load MMapFile");
                let entries = ColumnarEntries::<MMapFile>::from_existing(store);

                assert_eq!(entries.len(), 5);

                // All should be findable
                for i in 0..5 {
                    let found = entries.robin_hood_find(0, |e| e.key_pos().offset() == i * 16);
                    assert!(found.is_some(), "Entry with key {} not found", i * 16);
                }

                // Check that PSL values are reasonable (some should be > 0 due to collisions)
                // Due to Robin Hood, we might have entries with PSL > 0 - this is expected
                let has_nonzero_psl = (0..5).any(|i| entries.get_psl(i) > 0);
                // Collisions should cause some entries to have PSL > 0
                assert!(has_nonzero_psl, "Expected some entries to have PSL > 0 due to collisions");
            }
        }

        use proptest::prelude::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(5))]

            #[test]
            fn prop_persistence_round_trip(
                entries_to_insert in prop::collection::vec(
                    (0u64..500, 0u64..10000),
                    5..30
                )
            ) {
                let dir = tempdir().expect("Failed to create temp dir");
                let path = dir.path().join("entries.bin");

                let mut expected = std::collections::HashMap::new();

                // Insert entries
                {
                    let store = create_mmap_store(&path, 64);
                    let mut entries = ColumnarEntries::new(store, 64);

                    for (key, value) in &entries_to_insert {
                        let entry = create_test_entry(*key, *value);
                        entries.robin_hood_insert(*key, entry, |a, b| a.key_pos() == b.key_pos());
                        expected.insert(*key, *value);
                    }
                }

                // Reload and verify
                {
                    let store = MMapFile::from_file(&path).expect("Failed to load MMapFile");
                    let entries = ColumnarEntries::<MMapFile>::from_existing(store);

                    prop_assert_eq!(entries.len(), expected.len());

                    for (key, value) in &expected {
                        let found = entries.robin_hood_find(*key, |e| e.key_pos().offset() == *key);
                        prop_assert!(found.is_some(), "Key {} not found after reload", key);
                        prop_assert_eq!(found.unwrap().1.value_pos().offset(), *value);
                    }
                }
            }

            #[test]
            fn prop_persistence_with_modifications(
                initial_entries in prop::collection::vec(
                    (0u64..200, 0u64..5000),
                    5..20
                ),
                updates in prop::collection::vec(
                    (0u64..200, 5000u64..10000),
                    2..5
                ),
                deletes in prop::collection::vec(0u64..200, 1..3)
            ) {
                let dir = tempdir().expect("Failed to create temp dir");
                let path = dir.path().join("entries.bin");

                let mut expected = std::collections::HashMap::new();

                // Insert initial entries
                {
                    let store = create_mmap_store(&path, 64);
                    let mut entries = ColumnarEntries::new(store, 64);

                    for (key, value) in &initial_entries {
                        let entry = create_test_entry(*key, *value);
                        entries.robin_hood_insert(*key, entry, |a, b| a.key_pos() == b.key_pos());
                        expected.insert(*key, *value);
                    }
                }

                // Reload, apply updates
                {
                    let store = MMapFile::from_file(&path).expect("Failed to load MMapFile");
                    let mut entries = ColumnarEntries::<MMapFile>::from_existing(store);

                    for (key, value) in &updates {
                        let entry = create_test_entry(*key, *value);
                        entries.robin_hood_insert(*key, entry, |a, b| a.key_pos() == b.key_pos());
                        expected.insert(*key, *value);
                    }
                }

                // Reload, apply deletes
                {
                    let store = MMapFile::from_file(&path).expect("Failed to load MMapFile");
                    let mut entries = ColumnarEntries::<MMapFile>::from_existing(store);

                    for key in &deletes {
                        if expected.contains_key(key) {
                            let found = entries.robin_hood_find(*key, |e| e.key_pos().offset() == *key);
                            if let Some((slot, _)) = found {
                                entries.robin_hood_delete(slot);
                                expected.remove(key);
                            }
                        }
                    }
                }

                // Final reload and verify
                {
                    let store = MMapFile::from_file(&path).expect("Failed to load MMapFile");
                    let entries = ColumnarEntries::<MMapFile>::from_existing(store);

                    prop_assert_eq!(entries.len(), expected.len());

                    for (key, value) in &expected {
                        let found = entries.robin_hood_find(*key, |e| e.key_pos().offset() == *key);
                        prop_assert!(found.is_some(), "Key {} not found after modifications", key);
                        prop_assert_eq!(found.unwrap().1.value_pos().offset(), *value);
                    }

                    // Verify deleted keys are not found
                    for key in &deletes {
                        if !expected.contains_key(key) {
                            let found = entries.robin_hood_find(*key, |e| e.key_pos().offset() == *key);
                            prop_assert!(found.is_none(), "Deleted key {} still found", key);
                        }
                    }
                }
            }
        }
    }

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]

        #[test]
        fn prop_insert_find_consistency(
            entries_to_insert in prop::collection::vec(
                (0u64..1000, 0u64..10000),
                1..100
            )
        ) {
            let capacity = 256;
            let store = create_store(capacity);
            let mut entries = ColumnarEntries::new(store, capacity);

            let mut expected = std::collections::HashMap::new();

            for (key, value) in entries_to_insert {
                let entry = create_test_entry(key, value);
                entries.robin_hood_insert(key, entry, |a, b| a.key_pos() == b.key_pos());
                expected.insert(key, value);
            }

            // Verify all entries can be found
            for (key, value) in &expected {
                let found = entries.robin_hood_find(*key, |e| e.key_pos().offset() == *key);
                prop_assert!(found.is_some(), "Key {} not found", key);
                let (_, entry) = found.unwrap();
                prop_assert_eq!(entry.value_pos().offset(), *value);
            }

            // Verify length matches
            prop_assert_eq!(entries.len(), expected.len());
        }

        #[test]
        fn prop_iterator_completeness(
            entries_to_insert in prop::collection::vec(
                (0u64..1000, 0u64..10000),
                1..50
            )
        ) {
            let capacity = 128;
            let store = create_store(capacity);
            let mut entries = ColumnarEntries::new(store, capacity);

            let mut expected = std::collections::HashSet::new();

            for (key, value) in entries_to_insert {
                let entry = create_test_entry(key, value);
                entries.robin_hood_insert(key, entry, |a, b| a.key_pos() == b.key_pos());
                expected.insert(key);
            }

            // Verify iterator returns exactly the expected keys
            let iter_keys: std::collections::HashSet<_> = entries
                .iter()
                .map(|(_, e)| e.key_pos().offset())
                .collect();

            prop_assert_eq!(iter_keys, expected);
        }
    }
}
