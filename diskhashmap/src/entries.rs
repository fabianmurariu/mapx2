use bytemuck::{Pod, Zeroable};

use crate::ByteStore;
use crate::entry::Entry;
use crate::error::Result;
use crate::fixed_buffers::FixedVec;

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
#[derive(Debug, Pod, Zeroable, Clone, Copy)]
pub(crate) struct EntriesState {
    pub reindex_offset: i64,
    pub reindex_batch: u64,
    pub occupied_count: u64,
}

impl<BS: ByteStore> DoubleArrayEntries<BS> {
    fn new_with_capacity(store: BS, capacity: usize) -> Result<Self> {
        let entries = FixedVec::new_with_capacity(store, capacity);
        Ok(Self::new(entries))
    }

    pub(crate) fn has_old_entries(&self) -> bool {
        self.old_entries.is_some()
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

    pub(crate) fn grow(&mut self, new_capacity: usize) -> Result<EntriesState> {
        let new_entries = self.new_entries.new_empty(new_capacity);
        let old_entries = std::mem::replace(&mut self.new_entries, new_entries);
        self.old_entries = Some(old_entries);
        Ok(EntriesState {
            reindex_offset: 0,
            reindex_batch: 4,
            occupied_count: 0,
        })
    }

    pub(crate) fn set_entry(
        &mut self,
        index: SlotIdx,
        entry: Entry,
        state: &mut EntriesState,
        reindex_callback: impl Fn(&Entry) -> usize,
    ) {
        // TODO: handle when SlotIdx is old
        let items = self.new_entries.as_mut();
        // let index = hash % items.len();
        fn insert_into_entries(items: &mut [Entry], index: SlotIdx, entry: Entry) {
            // fast path for empty slot
            let index = index.value();
            if items[index].is_empty() || items[index].is_deleted() || &items[index] == &entry {
                // either empty slot or replacing same key
                items[index] = entry;
                return;
            }
            for pos in (index..items.len()).chain(0..index) {
                let current = &items[pos];
                if current.is_empty() || current.is_deleted() {
                    // Found an empty or deleted slot, insert here
                    items[pos] = entry;
                    return;
                }
                // If the slot is occupied, continue probing
            }
        }
        insert_into_entries(items, index, entry);
        if state.reindex_offset > 0 {
            // we need to re-index
            let mut done = false;
            if let Some(old_items) = self.old_entries.as_ref() {
                let start = state.reindex_offset as usize;
                let end = (state.reindex_offset as usize + state.reindex_batch as usize)
                    .min(old_items.len());
                done = end == old_items.len();
                for entry in &old_items.as_ref()[start..end] {
                    if entry.is_occupied() && !entry.is_moved() {
                        let new_hash = reindex_callback(entry);
                        let new_index = new_hash % items.len();
                        insert_into_entries(items, SlotIdx::new(new_index), *entry);
                    }
                    state.reindex_offset += 1;
                }
            }
            if done {
                // finished reindexing
                self.old_entries = None;
                state.reindex_offset = 0;
                let old_entries = self.old_entries.take().unwrap();
                old_entries.purge();
            }
        }
    }

    pub fn get_entry(&self, index: SlotIdx) -> Option<&Entry> {
        if !index.is_old() {
            self.new_entries.as_ref().get(index.value())
        } else {
            self.old_entries
                .as_ref()
                .and_then(|old| old.as_ref().get(index.value()))
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
                if state.reindex_offset > 0 && index_old >= state.reindex_offset as usize {
                    Some((old_entries, index_old))
                } else {
                    None
                }
            })
            .flat_map(move |(old_entries, index_old)| {
                old_entries.as_ref()[index_old..]
                    .iter()
                    .enumerate()
                    .map(move |(pos, entry)| (pos + index_old, entry))
                    .chain(
                        old_entries.as_ref()[state.reindex_offset as usize..index_old]
                            .iter()
                            .enumerate()
                            .map(|(pos, entry)| (pos + state.reindex_offset as usize, entry)),
                    )
                    .map(|(pos, entry)| (SlotIdx::old(pos), entry))
            });
        let new_iter = self.new_entries.as_ref()[index_new..]
            .iter()
            .enumerate()
            .map(move |(pos, entry)| (pos + index_new, entry))
            .chain(self.new_entries.as_ref()[..index_new].iter().enumerate())
            .map(|(pos, entry)| (SlotIdx::old(pos), entry));
        (new_iter, old_iter)
    }

    // pub(crate) fn new_entries(&self) -> &[Entry] {
    //     self.new_entries.as_ref()
    // }

    // pub(crate) fn old_entries(&self) -> Option<&[Entry]> {
    //     self.old_entries.as_ref().map(|e| e.as_ref())
    // }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::{ByteStore, HeapIdx, VecStore};

//     fn create_test_entry(k_pos: u32, v_pos: u32) -> Entry {
//         Entry::occupied_at_pos(HeapIdx::from(k_pos as u64), HeapIdx::from(v_pos as u64))
//     }

//     fn create_empty_entries(capacity: usize) -> DoubleArrayEntries<VecStore> {
//         // Create a VecStore with the right size for the given capacity
//         let mut store = VecStore::with_capacity(capacity * std::mem::size_of::<Entry>());
//         // Grow the store to the required size
//         store.grow(capacity * std::mem::size_of::<Entry>());
//         DoubleArrayEntries::new_with_capacity(store, capacity).unwrap()
//     }

//     fn create_entries_with_data(data: Vec<(u32, u32)>) -> DoubleArrayEntries<VecStore> {
//         let capacity = (data.len() * 2).max(8); // Ensure low load factor
//         let mut entries = create_empty_entries(capacity);

//         for (k_pos, v_pos) in data {
//             let entry = create_test_entry(k_pos, v_pos);
//             let hash = u64::from(k_pos); // Simple hash for testing
//             let _ = entries.set_entry(hash as usize, entry);
//         }

//         entries
//     }

//     #[test]
//     fn test_create_empty_entries() {
//         let entries = create_empty_entries(16);

//         assert_eq!(entries.capacity(), 16);
//         assert_eq!(entries.occupied_count(), 0);
//         assert!(!entries.is_resizing());
//         // assert_eq!(entries.load_factor(), 0.0);
//     }

//     #[test]
//     fn test_insert_and_lookup() {
//         let mut entries = create_empty_entries(16);
//         let entry = create_test_entry(100, 200);
//         let index = 42;

//         // Insert entry
//         entries.set_entry(index as usize, entry);
//         assert_eq!(entries.occupied_count(), 1);
//         assert!(entries.get_entry(index).is_occupied());
//         assert_eq!(entries.get_entry(index).key_pos(), HeapIdx::from(100u64));
//         assert_eq!(entries.get_entry(index).value_pos(), HeapIdx::from(200u64));
//     }

//     #[test]
//     fn test_find_slot_existing_key() {
//         let data = vec![(100, 200), (101, 201), (102, 202)];
//         let entries = create_entries_with_data(data);

//         // Find existing entry
//         let hash = 101u64;
//         let result = entries[hash];

//         assert!(result.is_ok());
//         let index = result.unwrap();
//         assert_eq!(entries.get_entry(index).key_pos(), HeapIdx::from(101u64));
//         assert_eq!(entries.get_entry(index).value_pos(), HeapIdx::from(201u64));
//     }

//     #[test]
//     fn test_find_slot_nonexistent_key() {
//         let data = vec![(100, 200), (101, 201)];
//         let entries = create_entries_with_data(data);

//         // Find non-existent entry
//         let hash = 999u64;
//         let result = entries.find_slot(hash, |entry| entry.key_pos() == HeapIdx::from(999u64));

//         assert!(result.is_err());
//         let empty_index = result.unwrap_err();
//         assert!(entries.get_entry(empty_index).is_empty());
//     }

//     #[test]
//     fn test_load_factor_calculation() {
//         let mut entries = create_empty_entries(10);
//         assert_eq!(entries.load_factor(), 0.0);

//         // Add some entries
//         for i in 0..3 {
//             let entry = create_test_entry(i, i + 100);
//             let _ = entries.insert_entry(u64::from(i), entry);
//         }

//         assert_eq!(entries.occupied_count(), 3);
//         assert_eq!(entries.load_factor(), 0.3);
//     }

//     #[test]
//     fn test_should_resize_trigger() {
//         let config = ResizeConfig {
//             load_factor_threshold: 0.75,
//             ..Default::default()
//         };

//         let mut entries = create_empty_entries(4);
//         entries.set_config(config);

//         // Add entries until we hit the threshold
//         assert!(!entries.should_resize()); // 0/4 = 0.0

//         let _ = entries.insert_entry(1, create_test_entry(1, 1));
//         assert!(!entries.should_resize()); // 1/4 = 0.25

//         let _ = entries.insert_entry(2, create_test_entry(2, 2));
//         assert!(!entries.should_resize()); // 2/4 = 0.5

//         let _ = entries.insert_entry(3, create_test_entry(3, 3));
//         assert!(!entries.should_resize()); // 3/4 = 0.75 (exactly at threshold)

//         let _ = entries.insert_entry(4, create_test_entry(4, 4));
//         assert!(entries.should_resize()); // 4/4 = 1.0 > 0.75
//     }

//     #[test]
//     fn test_start_resize() {
//         let data = vec![(1, 10), (2, 20), (3, 30)];
//         let mut entries = create_entries_with_data(data);
//         let original_capacity = entries.capacity();

//         assert!(!entries.is_resizing());
//         assert_eq!(entries.occupied_count(), 3);

//         // Start resize
//         entries.start_resize(original_capacity * 2).unwrap();

//         assert!(entries.is_resizing());
//         assert_eq!(entries.capacity(), original_capacity * 2);
//         assert_eq!(
//             entries.effective_capacity(),
//             original_capacity + original_capacity * 2
//         );
//         assert_eq!(entries.get_rehash_progress(), 0);
//         assert_eq!(entries.occupied_count(), 3); // Still tracking occupied entries
//     }

//     #[test]
//     fn test_incremental_rehash() {
//         let data = vec![(1, 10), (2, 20), (3, 30), (4, 40)];
//         let mut entries = create_entries_with_data(data);
//         let original_capacity = entries.capacity();

//         // Start resize
//         entries.start_resize(original_capacity * 2).unwrap();
//         assert!(entries.is_resizing());

//         // Perform incremental rehash
//         let rehashed = entries.incremental_rehash_simple();
//         assert!(rehashed > 0, "Expected to rehash some entries, but got 0");
//         // After incremental rehash, either we have progress > 0 (still rehashing) or we're done (!is_resizing())
//         assert!(
//             entries.get_rehash_progress() > 0 || !entries.is_resizing(),
//             "Expected either progress > 0 or completed resize"
//         );

//         // Continue until resize is complete
//         while entries.is_resizing() {
//             entries.incremental_rehash_simple();
//         }

//         assert!(!entries.is_resizing());
//         assert_eq!(entries.get_rehash_progress(), 0);
//         assert_eq!(entries.occupied_count(), 4);
//     }

//     #[test]
//     fn test_complete_resize() {
//         let data = vec![(1, 10), (2, 20), (3, 30)];
//         let mut entries = create_entries_with_data(data);
//         let original_capacity = entries.capacity();

//         // Start resize
//         entries.start_resize(original_capacity * 2).unwrap();
//         assert!(entries.is_resizing());

//         // Complete resize in one go
//         entries
//             .complete_resize(|k_pos, _| u64::from(k_pos))
//             .unwrap();

//         assert!(!entries.is_resizing());
//         assert_eq!(entries.occupied_count(), 3);

//         // Verify all entries are still findable
//         for (k_pos, v_pos) in &[(1, 10), (2, 20), (3, 30)] {
//             let result = entries.find_slot(*k_pos as u64, |e| {
//                 e.key_pos() == HeapIdx::from(*k_pos as u64)
//             });
//             assert!(result.is_ok());
//             let index = result.unwrap();
//             assert_eq!(
//                 entries.get_entry(index).value_pos(),
//                 HeapIdx::from(*v_pos as u64)
//             );
//         }
//     }

//     #[test]
//     fn test_load_from_normal_state() {
//         // Create entries in normal state (no old array)
//         let data = vec![(5, 50), (6, 60)];
//         let original_entries = create_entries_with_data(data);

//         // Simulate loading from disk in normal state
//         let config = ResizeConfig::default();
//         let loaded = DoubleArrayEntries::load_from_state(
//             None,                                      // No old array
//             (*original_entries.new_entries()).clone(), // Simulate cloning the new array
//             0,                                         // No rehash progress
//             config,
//         );

//         assert!(!loaded.is_resizing());
//         assert_eq!(loaded.occupied_count(), 2);
//         assert_eq!(loaded.get_rehash_progress(), 0);

//         // Verify entries are accessible
//         let result = loaded.find_slot(5u64, |e| e.key_pos() == HeapIdx::from(5u64));
//         assert!(result.is_ok());
//     }

//     #[test]
//     fn test_load_from_mid_resize_state() {
//         // Create entries and start resize to get into mid-resize state
//         let data = vec![
//             (1, 10),
//             (2, 20),
//             (3, 30),
//             (4, 40),
//             (5, 50),
//             (6, 60),
//             (7, 70),
//             (8, 80),
//         ];
//         let mut entries = create_entries_with_data(data);
//         let original_capacity = entries.capacity();

//         // Set a smaller batch size to ensure partial rehashing
//         let mut config = entries.config().clone();
//         config.rehash_batch_size = 2; // Only rehash 2 entries at a time
//         entries.set_config(config);

//         entries.start_resize(original_capacity * 2).unwrap();

//         // Perform partial rehash - should only rehash 2 entries due to small batch size
//         let rehashed = entries.incremental_rehash_simple();
//         let progress = entries.get_rehash_progress();

//         // Ensure we're actually in mid-resize state
//         if !entries.is_resizing() {
//             // If rehashing completed, skip this test - the batch was too small for the data
//             return;
//         }

//         // Simulate saving and loading from mid-resize state
//         let old_entries = entries.old_entries().cloned();
//         let new_entries = (*entries.new_entries()).clone();
//         let config = entries.config().clone();

//         let loaded =
//             DoubleArrayEntries::load_from_state(old_entries, new_entries, progress, config);

//         assert!(loaded.is_resizing());
//         assert_eq!(loaded.get_rehash_progress(), progress);
//         assert!(loaded.occupied_count() > 0);

//         // Can continue rehashing from where we left off
//         let mut loaded = loaded;
//         while loaded.is_resizing() {
//             loaded.incremental_rehash_simple();
//         }

//         assert!(!loaded.is_resizing());
//         assert_eq!(loaded.occupied_count(), 8);
//     }

//     #[test]
//     fn test_occupied_entries_iterator_normal_state() {
//         let data = vec![(10, 100), (20, 200), (30, 300)];
//         let entries = create_entries_with_data(data.clone());

//         let occupied: Vec<_> = entries.occupied_entries().collect();
//         assert_eq!(occupied.len(), 3);

//         // Verify all entries are found
//         for (k_pos, v_pos) in data {
//             let found = occupied.iter().any(|(_, entry)| {
//                 entry.key_pos() == HeapIdx::from(k_pos as u64)
//                     && entry.value_pos() == HeapIdx::from(v_pos as u64)
//             });
//             assert!(found, "Entry ({}, {}) not found", k_pos, v_pos);
//         }
//     }

//     #[test]
//     fn test_occupied_entries_iterator_during_resize() {
//         let data = vec![(1, 10), (2, 20), (3, 30)];
//         let mut entries = create_entries_with_data(data.clone());

//         // Start resize but don't complete it
//         entries.start_resize(entries.capacity() * 2).unwrap();
//         entries.incremental_rehash_simple(); // Partially rehash

//         let occupied: Vec<_> = entries.occupied_entries().collect();
//         assert_eq!(occupied.len(), 3);

//         // All original entries should still be findable
//         for (k_pos, v_pos) in data {
//             let found = occupied.iter().any(|(_, entry)| {
//                 entry.key_pos() == HeapIdx::from(k_pos as u64)
//                     && entry.value_pos() == HeapIdx::from(v_pos as u64)
//                     && !entry.is_moved()
//             });
//             assert!(
//                 found,
//                 "Entry ({}, {}) not found or marked as moved incorrectly",
//                 k_pos, v_pos
//             );
//         }
//     }

//     #[test]
//     fn test_set_entry_updates_occupied_count() {
//         let mut entries = create_empty_entries(8);
//         assert_eq!(entries.occupied_count(), 0);

//         // Set an occupied entry
//         let entry = create_test_entry(1, 10);
//         entries.set_entry(0, entry);
//         assert_eq!(entries.occupied_count(), 1);

//         // Replace with another occupied entry (count shouldn't change)
//         let entry2 = create_test_entry(2, 20);
//         entries.set_entry(0, entry2);
//         assert_eq!(entries.occupied_count(), 1);

//         // Set to empty entry (count should decrease)
//         entries.set_entry(0, Entry::new());
//         assert_eq!(entries.occupied_count(), 0);
//     }

//     #[test]
//     fn test_rehash_progress_bounds() {
//         let data = vec![(1, 10), (2, 20)];
//         let mut entries = create_entries_with_data(data);
//         let capacity = entries.capacity();

//         entries.start_resize(capacity * 2).unwrap();

//         // Try to set progress beyond capacity
//         entries.set_rehash_progress(capacity + 100);
//         assert_eq!(entries.get_rehash_progress(), capacity); // Should be clamped

//         // Set valid progress
//         entries.set_rehash_progress(capacity / 2);
//         assert_eq!(entries.get_rehash_progress(), capacity / 2);
//     }

//     #[test]
//     fn test_new_empty_preserves_config() {
//         let config = ResizeConfig {
//             load_factor_threshold: 0.6,
//             rehash_batch_size: 16,
//             growth_factor: 1.5,
//         };

//         let entries = create_empty_entries(8);
//         let mut entries =
//             DoubleArrayEntries::new_with_config((*entries.new_entries()).clone(), config.clone());

//         let empty = entries.new_empty(16);

//         assert_eq!(empty.capacity(), 16);
//         assert_eq!(empty.occupied_count(), 0);
//         assert_eq!(empty.config().load_factor_threshold, 0.6);
//         assert_eq!(empty.config().rehash_batch_size, 16);
//         assert_eq!(empty.config().growth_factor, 1.5);
//     }

//     #[test]
//     fn test_dual_array_indexing() {
//         let data = vec![(1, 10), (2, 20)];
//         let mut entries = create_entries_with_data(data);
//         let new_capacity = entries.capacity();

//         // Start resize to get dual arrays
//         entries.start_resize(new_capacity * 2).unwrap();

//         // Test indexing in new array (indices 0 to new_capacity-1)
//         let new_entry = create_test_entry(99, 990);
//         entries.set_entry(0, new_entry);
//         assert_eq!(entries.get_entry(0).key_pos(), HeapIdx::from(99u64));

//         // Test indexing in old array (indices new_capacity and above)
//         let old_index = new_capacity;
//         let old_entry = entries.get_entry(old_index);
//         assert!(old_entry.is_occupied() || old_entry.is_empty()); // Should be valid
//     }

//     #[test]
//     fn test_configuration_updates() {
//         let mut entries = create_empty_entries(8);

//         let new_config = ResizeConfig {
//             load_factor_threshold: 0.5,
//             rehash_batch_size: 4,
//             growth_factor: 3.0,
//         };

//         entries.set_config(new_config.clone());

//         assert_eq!(entries.config().load_factor_threshold, 0.5);
//         assert_eq!(entries.config().rehash_batch_size, 4);
//         assert_eq!(entries.config().growth_factor, 3.0);
//     }

//     #[test]
//     fn test_zero_capacity_handling() {
//         let store = VecStore::new();

//         // Create entries with minimal capacity
//         let result = DoubleArrayEntries::empty_with_capacity(store, 0);

//         // Should handle zero capacity gracefully
//         if let Ok(entries) = result {
//             assert_eq!(entries.load_factor(), 0.0);
//             assert!(!entries.should_resize());
//         }
//     }

//     #[test]
//     fn test_find_slot_with_collisions() {
//         let mut entries = create_empty_entries(4); // Small capacity to force collisions

//         // Insert entries that will collide (same hash % capacity)
//         let entry1 = create_test_entry(1, 10);
//         let entry2 = create_test_entry(5, 50); // 5 % 4 = 1, same as 1 % 4

//         entries.insert_entry(1, entry1).unwrap();
//         entries.insert_entry(5, entry2).unwrap();

//         // Both entries should be findable
//         let result1 = entries.find_slot(1, |e| e.key_pos() == HeapIdx::from(1u64));
//         assert!(result1.is_ok());

//         let result2 = entries.find_slot(5, |e| e.key_pos() == HeapIdx::from(5u64));
//         assert!(result2.is_ok());

//         // Verify they have different indices due to collision resolution
//         assert_ne!(result1.unwrap(), result2.unwrap());
//     }
// }
