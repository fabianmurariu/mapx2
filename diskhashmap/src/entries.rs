use crate::ByteStore;
use crate::entry::Entry;
use crate::error::Result;
use crate::fixed_buffers::FixedVec;
use bytemuck::{Pod, Zeroable};
use either::Either;

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct SlotIdx(i64);

impl SlotIdx {
    pub fn new(i: usize) -> Self {
        Self(i as i64)
    }

    pub fn old(i: usize) -> Self {
        Self(-(i as i64) - 1)
    }

    pub fn value(self) -> usize {
        let val = self.0;
        let mask = val >> 63;
        (val ^ mask) as usize
    }

    pub fn is_old(self) -> bool {
        self.0 < 0
    }

    pub fn max() -> Self {
        Self(i64::MAX)
    }
}

#[cfg(test)]
mod slot_idx_test {

    #[test]
    fn test_slot_idx() {
        let idx = super::SlotIdx::new(42);
        assert_eq!(idx.value(), 42);
        let old_idx = super::SlotIdx::old(42);
        assert_eq!(old_idx.value(), 42);
        assert!(old_idx.0 < 0);
    }
}

#[cfg(test)]
mod double_array_entries_tests {
    use super::*;
    use crate::byte_store::VecStore;
    use crate::entry::Entry;

    fn create_vec_store(capacity: usize) -> VecStore {
        let bytes_needed = capacity * std::mem::size_of::<Entry>();
        let mut store = VecStore::with_capacity(bytes_needed);
        store.grow(bytes_needed);
        store
    }

    fn create_test_entry(pos: u64) -> Entry {
        // Use pos as both the position and hash for testing
        Entry::occupied_at(pos, pos)
    }

    // fn create_deleted_entry() -> Entry {
    //     let mut entry = Entry::new();
    //     // Create the entry then mark it as deleted
    //     entry = entry.with_status(PaddedStatus::from_bytes([Status::Deleted as u8]));
    //     entry
    // }

    // fn create_moved_entry() -> Entry {
    //     let mut entry = Entry::new();
    //     entry.mark_as_moved();
    //     entry
    // }

    #[test]
    fn test_new_double_array_entries_vec_store() {
        let store = create_vec_store(16);
        let entries = FixedVec::new_with_capacity(store, 16);
        let double_entries = DoubleArrayEntries::new(entries);

        assert!(!double_entries.has_old_entries());
        assert_eq!(double_entries.new_entries.capacity(), 16);
    }

    #[test]
    fn test_new_with_old_double_array_entries() {
        let old_store = create_vec_store(8);
        let old_entries = FixedVec::new_with_capacity(old_store, 8);

        let new_store = create_vec_store(16);
        let new_entries = FixedVec::new_with_capacity(new_store, 16);

        let double_entries = DoubleArrayEntries::new_with_old(old_entries, new_entries);

        assert!(double_entries.has_old_entries());
        assert_eq!(double_entries.new_entries.capacity(), 16);
    }

    #[test]
    fn test_basic_entry_operations_vec_store() {
        let store = create_vec_store(16);
        let entries = FixedVec::new_with_capacity(store, 16);
        let mut double_entries = DoubleArrayEntries::new(entries);
        let mut state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 4,
            occupied_count: 0,
        };

        let entry = create_test_entry(100);
        let index = SlotIdx::new(5);

        double_entries.set_entry(index, entry, &mut state, |_| 0);
        state.occupied_count += 1;

        let retrieved = double_entries.get_entry(index);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().pos(), 100);
    }

    #[test]
    fn test_grow_operation() {
        let store = create_vec_store(8);
        let entries = FixedVec::new_with_capacity(store, 8);
        let mut double_entries = DoubleArrayEntries::new(entries);

        // Add some entries before growing
        let mut state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 4,
            occupied_count: 0,
        };

        for i in 0..4 {
            let entry = create_test_entry(i * 10);
            double_entries.set_entry(SlotIdx::new(i as usize), entry, &mut state, |_| 0);
            state.occupied_count += 1;
        }

        // Grow the entries
        let new_state = double_entries.grow(16, state.occupied_count).unwrap();
        assert_eq!(new_state.reindex_offset, 0);
        assert_eq!(new_state.reindex_batch, 4);
        assert_eq!(new_state.occupied_count, 4);

        assert!(double_entries.has_old_entries());
        assert_eq!(double_entries.new_entries.capacity(), 16);
    }

    use proptest::prelude::*;

    fn check_i_can_have_entries(fixtures: &[u64]) {
        let mut capacity = 8;
        let store = create_vec_store(capacity);
        let entries = FixedVec::new_with_capacity(store, capacity);
        let mut entries = DoubleArrayEntries::new(entries);
        let mut state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 4,
            occupied_count: 0,
        };
        for hash in fixtures.iter() {
            let slot = SlotIdx::new(*hash as usize % capacity);
            entries.set_entry(slot, create_test_entry(*hash), &mut state, |e| {
                e.pos() as usize
            });
            state.occupied_count += 1;
            if state.occupied_count as usize > capacity / 2 {
                state = entries.grow(capacity * 2, state.occupied_count).unwrap();
                capacity *= 2;
            }
            let (new, old) = entries.find_entry(*hash as usize, &state);
            let found = new
                .filter(|(_, e)| !e.is_empty())
                .chain(old.filter(|(_, e)| !e.is_empty()))
                .find(|(_, e)| e.pos() == *hash);
            assert!(
                found.is_some(),
                "Should find entry for hash {} in\n{entries:?}",
                hash
            );
        }
    }

    #[test]
    fn i_can_have_entries() {
        let strat = prop::collection::vec(0u64..1000u64, 0..1024).prop_map(|mut vec| {
            vec.sort_unstable();
            vec.dedup();
            use rand::seq::SliceRandom;
            let mut rng = rand::rng();
            vec.shuffle(&mut rng);
            vec
        });
        proptest!(| (entries in strat) |{
            check_i_can_have_entries(&entries);
        });
    }

    #[test]
    fn i_can_have_entries_1() {
        check_i_can_have_entries(&[0, 1, 2, 3]);
    }

    #[test]
    fn i_can_have_entries_2() {
        check_i_can_have_entries(&[0, 1, 2, 3, 4]);
    }
}

/// Double array entries for incremental resizing
///
/// This implementation maintains two entry arrays during resize operations:
/// - old_entries: Contains entries that haven't been rehashed yet
/// - new_entries: Contains rehashed entries and new insertions
///
/// Incremental rehashing distributes the resize work across multiple operations
/// to avoid large pauses, following the algorithm outlined in docs/entries.md
/// the reported size to the external caller is always of new_entries only
#[derive(Debug)]
pub struct DoubleArrayEntries<BS: ByteStore> {
    /// The old entries array (present during resize)
    old_entries: Option<FixedVec<Entry, BS>>,

    /// The new entries array (always present)
    new_entries: FixedVec<Entry, BS>,
}

impl<BS: ByteStore> DoubleArrayEntries<BS> {
    /// Creates a new DoubleArrayEntries in normal state (single array)
    pub fn new(entries: FixedVec<Entry, BS>) -> Self {
        Self {
            old_entries: None,
            new_entries: entries,
        }
    }

    /// Creates a new DoubleArrayEntries with old_entries
    pub fn new_with_old(old: FixedVec<Entry, BS>, new: FixedVec<Entry, BS>) -> Self {
        Self {
            old_entries: Some(old),
            new_entries: new,
        }
    }
}

// State is stored externally by the disk_map
// and is passed in for operations that need it
#[repr(C)]
#[derive(Debug, Pod, Zeroable, Clone, Copy, PartialEq)]
pub(crate) struct EntriesState {
    pub reindex_offset: i64,
    pub reindex_batch: u64,
    pub occupied_count: u64,
}

impl<BS: ByteStore> DoubleArrayEntries<BS> {
    // fn new_with_capacity(store: BS, capacity: usize) -> Result<Self> {
    //     let entries = FixedVec::new_with_capacity(store, capacity);
    //     Ok(Self::new(entries))
    // }

    pub(crate) fn has_old_entries(&self) -> bool {
        self.old_entries.is_some()
    }

    pub(crate) fn len(&self) -> usize {
        self.new_entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.new_entries.len() == 0 && self.old_entries.as_ref().map_or(true, |old| old.len() == 0)
    }

    pub(crate) fn iter(
        &self,
        reindex_offset: i64,
        occupied_count: usize,
    ) -> impl Iterator<Item = (SlotIdx, &Entry)> {
        if self.has_old_entries() {
            assert!(reindex_offset >= 0);
        } else {
            assert!(reindex_offset < 0);
        }
        let old_slice = self
            .old_entries
            .as_ref()
            .into_iter()
            .flat_map(move |old| &old[reindex_offset as usize..]);

        let new_slice = self.new_entries.as_ref().iter();
        old_slice
            .enumerate()
            .map(move |(i, entry)| (SlotIdx::old(i + reindex_offset as usize), entry))
            .chain(
                new_slice
                    .enumerate()
                    .map(|(i, entry)| (SlotIdx::new(i), entry)),
            )
            .filter(|(_, entry)| entry.is_occupied())
            .take(occupied_count)
    }

    pub(crate) fn grow(
        &mut self,
        new_capacity: usize,
        current_occupied_count: u64,
    ) -> Result<EntriesState> {
        // handle the first call to grow from 0
        let init_empty = self.new_entries.is_empty();
        let new_entries = self.new_entries.new_empty(new_capacity);
        let old_entries = std::mem::replace(&mut self.new_entries, new_entries);
        if init_empty {
            // first grow, no need to reindex
            old_entries.purge();
            self.old_entries = None;
            return Ok(EntriesState {
                reindex_offset: -1,
                reindex_batch: 4,
                occupied_count: current_occupied_count,
            });
        }
        self.old_entries = Some(old_entries);
        Ok(EntriesState {
            reindex_offset: 0,
            reindex_batch: 4,
            occupied_count: current_occupied_count,
        })
    }

    pub(crate) fn set_entry(
        &mut self,
        index: SlotIdx,
        entry: Entry,
        state: &mut EntriesState,
        reindex_callback: impl Fn(&Entry) -> usize,
    ) {
        let items = self.new_entries.as_mut();

        #[inline(always)]
        fn insert_into_entries(items: &mut [Entry], index: SlotIdx, entry: Entry) {
            let len = items.len();
            let mut slot_pos = index.value();

            // Fast path - check direct slot first
            let slot = &mut items[slot_pos];
            // Only insert into empty or deleted slots
            if slot.is_empty() || slot.is_deleted() {
                *slot = entry;
                return;
            }

            // Linear probing with manual wrapping
            for _ in 1..len {
                slot_pos = (slot_pos + 1) % len;
                let slot = &mut items[slot_pos];
                if slot.is_empty() || slot.is_deleted() {
                    *slot = entry;
                    return;
                }
            }
            unreachable!("Hash table full");
        }

        insert_into_entries(items, index, entry);

        // Incremental rehashing - only if we're in the middle of a resize
        if state.reindex_offset >= 0 {
            if let Some(old_items) = self.old_entries.as_mut() {
                let old_slice = old_items.as_mut();
                let start = state.reindex_offset as usize;
                let end = (start + state.reindex_batch as usize).min(old_slice.len());
                let done = end == old_slice.len();

                // Rehash a batch of entries
                for i in start..end {
                    let old_entry = &mut old_slice[i];
                    if old_entry.is_occupied() && !old_entry.is_moved() {
                        let new_hash = reindex_callback(old_entry);
                        let new_index = new_hash % items.len();
                        insert_into_entries(items, SlotIdx::new(new_index), *old_entry);
                        old_entry.mark_as_moved();
                    }
                }

                // Update offset once after the batch
                state.reindex_offset = if done {
                    -1
                } else {
                    state.reindex_offset + (end - start) as i64
                };

                // Cleanup old entries when done
                if done {
                    if let Some(old_entries) = self.old_entries.take() {
                        old_entries.purge();
                    }
                }
            }
        }
    }

    pub(crate) fn get_entry(&self, index: SlotIdx) -> Option<&Entry> {
        if !index.is_old() {
            self.new_entries.as_ref().get(index.value())
        } else {
            self.old_entries
                .as_ref()
                .and_then(|old| old.as_ref().get(index.value()))
        }
    }

    /// Directly update an entry at the given index without probing.
    /// Use this for updating existing entries where the slot is already known.
    /// Also performs incremental rehashing if a resize is in progress.
    pub(crate) fn update_entry(
        &mut self,
        index: SlotIdx,
        entry: Entry,
        state: &mut EntriesState,
        reindex_callback: impl Fn(&Entry) -> usize,
    ) {
        // Direct overwrite at the known slot
        if !index.is_old() {
            self.new_entries.as_mut()[index.value()] = entry;
        } else {
            if let Some(old) = self.old_entries.as_mut() {
                old.as_mut()[index.value()] = entry;
            }
        }

        // Incremental rehashing - same as set_entry
        if state.reindex_offset >= 0 {
            let items = self.new_entries.as_mut();
            if let Some(old_items) = self.old_entries.as_mut() {
                let old_slice = old_items.as_mut();
                let start = state.reindex_offset as usize;
                let end = (start + state.reindex_batch as usize).min(old_slice.len());
                let done = end == old_slice.len();

                // Rehash a batch of entries
                for i in start..end {
                    let old_entry = &mut old_slice[i];
                    if old_entry.is_occupied() && !old_entry.is_moved() {
                        let new_hash = reindex_callback(old_entry);
                        let new_index = new_hash % items.len();
                        // Use inline insert logic
                        let len = items.len();
                        let mut slot_pos = new_index;
                        loop {
                            let slot = &mut items[slot_pos];
                            if slot.is_empty() || slot.is_deleted() {
                                *slot = *old_entry;
                                break;
                            }
                            slot_pos = (slot_pos + 1) % len;
                        }
                        old_entry.mark_as_moved();
                    }
                }

                state.reindex_offset = if done {
                    -1
                } else {
                    state.reindex_offset + (end - start) as i64
                };

                if done {
                    if let Some(old_entries) = self.old_entries.take() {
                        old_entries.purge();
                    }
                }
            }
        }
    }

    pub(crate) fn find_entry(
        &self,
        hash: usize,
        state: &EntriesState,
    ) -> (
        impl Iterator<Item = (SlotIdx, &Entry)>,
        impl Iterator<Item = (SlotIdx, &Entry)>,
    ) {
        let index_new = hash % self.new_entries.len();
        let old_iter = self
            .old_entries
            .as_ref()
            .into_iter()
            .filter_map(move |old_entries| {
                let index_old = hash % old_entries.len();
                if state.reindex_offset >= 0 {
                    Some((old_entries, index_old))
                } else {
                    None
                }
            })
            .flat_map(move |(old_entries, index_old)| {
                let reindex_offset = state.reindex_offset as usize;
                if index_old >= reindex_offset {
                    // Start from index_old and wrap around, but only visit entries >= reindex_offset
                    Either::Left(
                        old_entries.as_ref()[index_old..]
                            .iter()
                            .enumerate()
                            .map(move |(pos, entry)| (pos + index_old, entry))
                            .chain(
                                old_entries.as_ref()[reindex_offset..index_old]
                                    .iter()
                                    .enumerate()
                                    .map(move |(pos, entry)| (pos + reindex_offset, entry)),
                            )
                            .map(|(pos, entry)| (SlotIdx::old(pos), entry)),
                    )
                } else {
                    // index_old < reindex_offset, so start from reindex_offset
                    Either::Right(
                        old_entries.as_ref()[reindex_offset..]
                            .iter()
                            .enumerate()
                            .map(move |(pos, entry)| (pos + reindex_offset, entry))
                            .map(|(pos, entry)| (SlotIdx::old(pos), entry)),
                    )
                }
            })
            .into_iter();
        let new_iter = self.new_entries.as_ref()[index_new..]
            .iter()
            .enumerate()
            .map(move |(pos, entry)| (pos + index_new, entry))
            .chain(self.new_entries.as_ref()[..index_new].iter().enumerate())
            .map(|(pos, entry)| (SlotIdx::new(pos), entry));
        (new_iter, old_iter)
    }
}
