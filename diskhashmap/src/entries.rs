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

#[cfg(test)]
mod double_array_entries_tests {
    use super::*;
    use crate::byte_store::{VecStore, MMapFile};
    use crate::entry::{Entry, Status, PaddedStatus};
    use crate::HeapIdx;
    use tempfile::TempDir;
    use std::collections::HashMap;

    fn create_vec_store(capacity: usize) -> VecStore {
        let bytes_needed = capacity * std::mem::size_of::<Entry>();
        let mut store = VecStore::with_capacity(bytes_needed);
        store.grow(bytes_needed);
        store
    }

    fn create_test_entry(k_pos: u64, v_pos: u64) -> Entry {
        Entry::occupied_at_pos(HeapIdx::from(k_pos), HeapIdx::from(v_pos))
    }

    fn create_deleted_entry() -> Entry {
        let mut entry = Entry::new();
        // Create the entry then mark it as deleted
        entry = entry.with_status(PaddedStatus::from_bytes([Status::Deleted as u8]));
        entry
    }

    fn create_moved_entry() -> Entry {
        let mut entry = Entry::new();
        entry.mark_as_moved();
        entry
    }

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

        let entry = create_test_entry(100, 200);
        let index = SlotIdx::new(5);

        double_entries.set_entry(index, entry, &mut state, |_| 0);
        state.occupied_count += 1;

        let retrieved = double_entries.get_entry(index);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().key_pos(), HeapIdx::from(100));
        assert_eq!(retrieved.unwrap().value_pos(), HeapIdx::from(200));
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
            let entry = create_test_entry(i * 10, i * 20);
            double_entries.set_entry(SlotIdx::new(i as usize), entry, &mut state, |_| 0);
            state.occupied_count += 1;
        }

        // Grow the entries
        let new_state = double_entries.grow(16).unwrap();
        assert_eq!(new_state.reindex_offset, 0);
        assert_eq!(new_state.reindex_batch, 4);
        assert_eq!(new_state.occupied_count, 0);

        assert!(double_entries.has_old_entries());
        assert_eq!(double_entries.new_entries.capacity(), 16);
    }

    #[test]
    fn test_reindexing_process() {
        let store = create_vec_store(4);
        let entries = FixedVec::new_with_capacity(store, 4);
        let mut double_entries = DoubleArrayEntries::new(entries);

        let mut state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 2,
            occupied_count: 0,
        };

        // Fill the initial array
        for i in 0..4 {
            let entry = create_test_entry(i * 10, i * 20);
            double_entries.set_entry(SlotIdx::new(i as usize), entry, &mut state, |_| 0);
            state.occupied_count += 1;
        }

        // Grow and start reindexing
        state = double_entries.grow(8).unwrap();

        // Insert a new entry, which should trigger reindexing
        let new_entry = create_test_entry(999, 888);
        double_entries.set_entry(SlotIdx::new(0), new_entry, &mut state, |entry| {
            (u64::from(entry.key_pos()) as usize) % 8  // Simple hash function for testing
        });

        // After the first insertion following grow, reindexing should have started and possibly completed
        // If there are still old entries, we're in the middle of reindexing
        // If there are no old entries, reindexing completed in one batch
        if double_entries.has_old_entries() {
            assert!(state.reindex_offset >= 0, "Should be in reindexing mode with reindex_offset >= 0");
        } else {
            // Reindexing completed immediately due to small batch size
            assert_eq!(state.reindex_offset, -1, "Should have completed reindexing");
        }

        // Continue reindexing until complete
        while double_entries.has_old_entries() {
            let dummy_entry = create_test_entry(1111, 2222);
            double_entries.set_entry(SlotIdx::new(1), dummy_entry, &mut state, |entry| {
                (u64::from(entry.key_pos()) as usize) % 8
            });
        }

        // After reindexing is complete, old entries should be gone
        assert!(!double_entries.has_old_entries());
        assert_eq!(state.reindex_offset, -1);
    }

    #[test]
    fn test_entry_retrieval_during_reindexing() {
        let store = create_vec_store(4);
        let entries = FixedVec::new_with_capacity(store, 4);
        let mut double_entries = DoubleArrayEntries::new(entries);

        let mut state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 1,
            occupied_count: 0,
        };

        // Add entries to original array
        for i in 0..3 {
            let entry = create_test_entry(i * 100, i * 200);
            double_entries.set_entry(SlotIdx::new(i as usize), entry, &mut state, |_| 0);
            state.occupied_count += 1;
        }

        // Grow
        state = double_entries.grow(8).unwrap();

        // Verify we can still retrieve old entries
        let old_entry = double_entries.get_entry(SlotIdx::old(1));
        assert!(old_entry.is_some());
        assert_eq!(old_entry.unwrap().key_pos(), HeapIdx::from(100));

        // Add a new entry to trigger partial reindexing
        let new_entry = create_test_entry(500, 600);
        double_entries.set_entry(SlotIdx::new(0), new_entry, &mut state, |entry| {
            (u64::from(entry.key_pos()) as usize) % 8
        });

        // Should still be able to retrieve from both arrays
        let new_retrieved = double_entries.get_entry(SlotIdx::new(0));
        assert!(new_retrieved.is_some());
        assert_eq!(new_retrieved.unwrap().key_pos(), HeapIdx::from(500));
    }

    #[test]
    fn test_iterator_functionality() {
        let store = create_vec_store(8);
        let entries = FixedVec::new_with_capacity(store, 8);
        let mut double_entries = DoubleArrayEntries::new(entries);

        let mut state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 4,
            occupied_count: 0,
        };

        // Add some entries
        let test_entries = vec![
            (0, create_test_entry(10, 20)),
            (2, create_test_entry(30, 40)),
            (4, create_test_entry(50, 60)),
        ];

        for (idx, entry) in &test_entries {
            double_entries.set_entry(SlotIdx::new(*idx), *entry, &mut state, |_| 0);
            state.occupied_count += 1;
        }

        // Test iterator in normal state
        let iter_entries: Vec<_> = double_entries.iter(-1, state.occupied_count as usize).collect();
        assert_eq!(iter_entries.len(), 3);

        // All entries should be marked as "new" (not old)
        for (slot_idx, _) in &iter_entries {
            assert!(!slot_idx.is_old());
        }
    }

    #[test]
    fn test_iterator_during_reindexing() {
        let store = create_vec_store(4);
        let entries = FixedVec::new_with_capacity(store, 4);
        let mut double_entries = DoubleArrayEntries::new(entries);

        let mut state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 2,
            occupied_count: 0,
        };

        // Fill original array
        for i in 0..3 {
            let entry = create_test_entry(i * 10, i * 20);
            double_entries.set_entry(SlotIdx::new(i as usize), entry, &mut state, |_| 0);
            state.occupied_count += 1;
        }

        // Grow
        state = double_entries.grow(8).unwrap();
        state.occupied_count = 3; // Reset for iteration

        // Test iterator shows old entries
        let iter_entries: Vec<_> = double_entries.iter(0, state.occupied_count as usize).collect();
        assert_eq!(iter_entries.len(), 3);

        // All should be from old array
        for (slot_idx, _) in &iter_entries {
            assert!(slot_idx.is_old());
        }
    }

    #[test]
    fn test_find_entry_functionality() {
        let store = create_vec_store(8);
        let entries = FixedVec::new_with_capacity(store, 8);
        let double_entries = DoubleArrayEntries::new(entries);

        let state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 4,
            occupied_count: 0,
        };

        let hash = 42;
        let (new_iter, old_iter) = double_entries.find_entry(hash, &state);

        // Should have entries from new array
        let new_count = new_iter.count();
        assert_eq!(new_count, 8); // Should iterate through all slots

        // Should have no entries from old array (since no old array exists)
        let old_count = old_iter.count();
        assert_eq!(old_count, 0);
    }

    #[test]
    fn test_comprehensive_insertion_and_retrieval() {
        let store = create_vec_store(16);
        let entries = FixedVec::new_with_capacity(store, 16);
        let mut double_entries = DoubleArrayEntries::new(entries);

        let mut state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 4,
            occupied_count: 0,
        };

        let mut inserted_entries = HashMap::new();

        // Insert many entries
        for i in 0..10 {
            let key_pos = i * 100;
            let val_pos = i * 200;
            let entry = create_test_entry(key_pos, val_pos);
            let slot = SlotIdx::new((i % 16) as usize);

            double_entries.set_entry(slot, entry, &mut state, |_| 0);
            inserted_entries.insert(slot.value(), (key_pos, val_pos));
            state.occupied_count += 1;
        }

        // Verify all entries can be retrieved
        for (slot_idx, (expected_key, expected_val)) in &inserted_entries {
            let retrieved = double_entries.get_entry(SlotIdx::new(*slot_idx));
            assert!(retrieved.is_some(), "Entry at slot {} should exist", slot_idx);
            let entry = retrieved.unwrap();
            assert_eq!(entry.key_pos(), HeapIdx::from(*expected_key));
            assert_eq!(entry.value_pos(), HeapIdx::from(*expected_val));
            assert!(entry.is_occupied());
        }
    }

    #[test]
    fn test_mmap_file_backend() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_entries.bin");

        let store = MMapFile::new(&file_path, 16 * std::mem::size_of::<Entry>()).unwrap();
        let entries = FixedVec::new_with_capacity(store, 16);
        let mut double_entries = DoubleArrayEntries::new(entries);

        let mut state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 4,
            occupied_count: 0,
        };

        // Insert some entries
        for i in 0..5 {
            let entry = create_test_entry(i * 10, i * 20);
            double_entries.set_entry(SlotIdx::new(i as usize), entry, &mut state, |_| 0);
            state.occupied_count += 1;
        }

        // Verify entries persist (by dropping and recreating)
        drop(double_entries);

        let store2 = MMapFile::from_file(&file_path).unwrap();
        let entries2 = FixedVec::new(store2);
        let double_entries2 = DoubleArrayEntries::new(entries2);

        // Verify data persisted
        for i in 0..5 {
            let retrieved = double_entries2.get_entry(SlotIdx::new(i as usize));
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().key_pos(), HeapIdx::from(i * 10));
            assert_eq!(retrieved.unwrap().value_pos(), HeapIdx::from(i * 20));
        }
    }

    #[test]
    fn test_loading_double_array_from_disk() {
        let temp_dir = TempDir::new().unwrap();
        let old_file_path = temp_dir.path().join("old_entries.bin");
        let new_file_path = temp_dir.path().join("new_entries.bin");

        // Create and populate old entries
        let old_store = MMapFile::new(&old_file_path, 8 * std::mem::size_of::<Entry>()).unwrap();
        let mut old_entries = FixedVec::new_with_capacity(old_store, 8);
        for i in 0..4 {
            old_entries[i as usize] = create_test_entry(i * 10, i * 20);
        }
        drop(old_entries);

        // Create new entries
        let new_store = MMapFile::new(&new_file_path, 16 * std::mem::size_of::<Entry>()).unwrap();
        let mut new_entries = FixedVec::new_with_capacity(new_store, 16);
        new_entries[0] = create_test_entry(100, 200);
        drop(new_entries);

        // Load both from disk
        let old_store_loaded = MMapFile::from_file(&old_file_path).unwrap();
        let old_entries_loaded = FixedVec::new(old_store_loaded);

        let new_store_loaded = MMapFile::from_file(&new_file_path).unwrap();
        let new_entries_loaded = FixedVec::new(new_store_loaded);

        let double_entries = DoubleArrayEntries::new_with_old(old_entries_loaded, new_entries_loaded);

        // Verify we can access both old and new entries
        assert!(double_entries.has_old_entries());

        let old_entry = double_entries.get_entry(SlotIdx::old(1));
        assert!(old_entry.is_some());
        assert_eq!(old_entry.unwrap().key_pos(), HeapIdx::from(10));

        let new_entry = double_entries.get_entry(SlotIdx::new(0));
        assert!(new_entry.is_some());
        assert_eq!(new_entry.unwrap().key_pos(), HeapIdx::from(100));
    }

    #[test]
    fn test_complete_reindexing_cycle() {
        let store = create_vec_store(4);
        let entries = FixedVec::new_with_capacity(store, 4);
        let mut double_entries = DoubleArrayEntries::new(entries);

        let mut state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 1, // Small batch size to test incremental reindexing
            occupied_count: 0,
        };

        // Fill original array completely
        for i in 0..4 {
            let entry = create_test_entry(i * 100, i * 200);
            double_entries.set_entry(SlotIdx::new(i as usize), entry, &mut state, |_| 0);
            state.occupied_count += 1;
        }

        // Grow
        state = double_entries.grow(8).unwrap();
        assert!(double_entries.has_old_entries());

        // Track entries moved during reindexing
        let mut insertions = 0;

        // Force complete reindexing by inserting entries
        while double_entries.has_old_entries() {
            let dummy_entry = create_test_entry(9999, 8888);
            double_entries.set_entry(SlotIdx::new(insertions % 8), dummy_entry, &mut state, |entry| {
                (u64::from(entry.key_pos()) as usize) % 8  // Simple hash for testing
            });
            insertions += 1;

            // Safety check to prevent infinite loop
            assert!(insertions < 100, "Reindexing took too many iterations");
        }

        // Verify old entries are cleaned up
        assert!(!double_entries.has_old_entries());
        assert_eq!(state.reindex_offset, -1);
    }

    #[test]
    fn test_entry_states_handling() {
        let store = create_vec_store(8);
        let entries = FixedVec::new_with_capacity(store, 8);
        let mut double_entries = DoubleArrayEntries::new(entries);

        let mut state = EntriesState {
            reindex_offset: -1,
            reindex_batch: 4,
            occupied_count: 0,
        };

        // Test different entry states
        double_entries.set_entry(SlotIdx::new(0), create_test_entry(100, 200), &mut state, |_| 0);
        double_entries.set_entry(SlotIdx::new(1), create_deleted_entry(), &mut state, |_| 0);
        double_entries.set_entry(SlotIdx::new(2), create_moved_entry(), &mut state, |_| 0);

        // Verify states
        let occupied = double_entries.get_entry(SlotIdx::new(0)).unwrap();
        assert!(occupied.is_occupied());

        let deleted = double_entries.get_entry(SlotIdx::new(1)).unwrap();
        assert!(deleted.is_deleted());

        let moved = double_entries.get_entry(SlotIdx::new(2)).unwrap();
        assert!(moved.is_moved());
    }

    // Property-based tests
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_property_entry_insertion_and_retrieval(
            entries in prop::collection::vec((0u64..1000u64, 0u64..1000u64), 1..16),
            capacity in 16usize..32usize
        ) {
            prop_assume!(entries.len() <= capacity);

            let store = create_vec_store(capacity);
            let fixed_entries = FixedVec::new_with_capacity(store, capacity);
            let mut double_entries = DoubleArrayEntries::new(fixed_entries);

            let mut state = EntriesState {
                reindex_offset: -1,
                reindex_batch: 4,
                occupied_count: 0,
            };

            let mut inserted_entries = std::collections::HashMap::new();

            // Insert entries using direct slot mapping to avoid collisions
            for (i, (k_pos, v_pos)) in entries.iter().enumerate() {
                if i >= capacity { break; }

                let entry = create_test_entry(*k_pos, *v_pos);
                let slot = SlotIdx::new(i); // Use index directly to avoid collisions

                double_entries.set_entry(slot, entry, &mut state, |_| 0);
                inserted_entries.insert(i, (*k_pos, *v_pos));
                state.occupied_count += 1;
            }

            // Verify all inserted entries can be retrieved
            for (slot_idx, (expected_k, expected_v)) in inserted_entries.iter() {
                let retrieved = double_entries.get_entry(SlotIdx::new(*slot_idx));
                prop_assert!(retrieved.is_some(), "Entry at slot {} should exist", slot_idx);
                let entry = retrieved.unwrap();
                prop_assert_eq!(entry.key_pos(), HeapIdx::from(*expected_k));
                prop_assert_eq!(entry.value_pos(), HeapIdx::from(*expected_v));
                prop_assert!(entry.is_occupied());
            }
        }

        #[test]
        fn test_property_grow_and_reindex(
            initial_entries in prop::collection::vec((0u64..500u64, 0u64..500u64), 1..8),
            initial_capacity in 8usize..16usize,
            new_capacity in 16usize..32usize
        ) {
            prop_assume!(new_capacity > initial_capacity);
            prop_assume!(initial_entries.len() <= initial_capacity);

            let store = create_vec_store(initial_capacity);
            let entries = FixedVec::new_with_capacity(store, initial_capacity);
            let mut double_entries = DoubleArrayEntries::new(entries);

            let mut state = EntriesState {
                reindex_offset: -1,
                reindex_batch: 2,
                occupied_count: 0,
            };

            let mut original_entries = std::collections::HashMap::new();

            // Insert initial entries
            for (i, (k_pos, v_pos)) in initial_entries.iter().enumerate() {
                if i >= initial_capacity { break; }
                let entry = create_test_entry(*k_pos, *v_pos);
                let slot = SlotIdx::new(i);
                double_entries.set_entry(slot, entry, &mut state, |_| 0);
                original_entries.insert(i, (*k_pos, *v_pos));
                state.occupied_count += 1;
            }

            // Grow the array
            state = double_entries.grow(new_capacity).unwrap();
            prop_assert!(double_entries.has_old_entries());

            // Force complete reindexing
            let mut iterations = 0;
            while double_entries.has_old_entries() && iterations < 1000 {
                let dummy_entry = create_test_entry(9999, 8888);
                double_entries.set_entry(SlotIdx::new(0), dummy_entry, &mut state, |entry| {
                    (u64::from(entry.key_pos()) as usize) % new_capacity
                });
                iterations += 1;
            }

            // Verify reindexing completed
            prop_assert!(!double_entries.has_old_entries());
            prop_assert_eq!(state.reindex_offset, -1);
        }

        #[test]
        fn test_property_iterator_consistency(
            entries in prop::collection::vec((1u64..100u64, 1u64..100u64), 1..16),
            capacity in 16usize..32usize
        ) {
            prop_assume!(entries.len() <= capacity);

            let store = create_vec_store(capacity);
            let fixed_entries = FixedVec::new_with_capacity(store, capacity);
            let mut double_entries = DoubleArrayEntries::new(fixed_entries);

            let mut state = EntriesState {
                reindex_offset: -1,
                reindex_batch: 4,
                occupied_count: 0,
            };

            let mut expected_entries = std::collections::HashSet::new();

            // Insert entries using direct indexing to avoid collisions
            for (i, (k_pos, v_pos)) in entries.iter().enumerate() {
                if i >= capacity { break; }

                let entry = create_test_entry(*k_pos, *v_pos);
                let slot = SlotIdx::new(i);

                double_entries.set_entry(slot, entry, &mut state, |_| 0);
                expected_entries.insert((*k_pos, *v_pos));
                state.occupied_count += 1;
            }

            // Test iterator returns correct entries
            let iter_entries: std::collections::HashSet<_> = double_entries
                .iter(-1, state.occupied_count as usize)
                .map(|(_, entry)| (u64::from(entry.key_pos()), u64::from(entry.value_pos())))
                .collect();

            prop_assert_eq!(iter_entries, expected_entries);
        }

        #[test]
        fn test_property_slot_idx_encoding(
            values in prop::collection::vec(0usize..10000usize, 1..100)
        ) {
            for value in values {
                // Test new SlotIdx
                let new_idx = SlotIdx::new(value);
                prop_assert_eq!(new_idx.value(), value);
                prop_assert!(!new_idx.is_old());

                // Test old SlotIdx
                let old_idx = SlotIdx::old(value);
                prop_assert_eq!(old_idx.value(), value);
                prop_assert!(old_idx.is_old());

                // Test that old and new are different
                prop_assert_ne!(new_idx.0, old_idx.0);
            }
        }

        #[test]
        fn test_property_double_array_persistence_mmap(
            entries in prop::collection::vec((1u64..100u64, 1u64..100u64), 1..8),
            capacity in 8usize..16usize
        ) {
            prop_assume!(entries.len() <= capacity);

            let temp_dir = TempDir::new().unwrap();
            let file_path = temp_dir.path().join("prop_test_entries.bin");

            let mut inserted_entries = std::collections::HashMap::new();

            // Create and populate entries
            {
                let store = MMapFile::new(&file_path, capacity * std::mem::size_of::<Entry>()).unwrap();
                let fixed_entries = FixedVec::new_with_capacity(store, capacity);
                let mut double_entries = DoubleArrayEntries::new(fixed_entries);

                let mut state = EntriesState {
                    reindex_offset: -1,
                    reindex_batch: 4,
                    occupied_count: 0,
                };

                // Use direct indexing to avoid collisions
                for (i, (k_pos, v_pos)) in entries.iter().enumerate() {
                    if i >= capacity { break; }

                    let entry = create_test_entry(*k_pos, *v_pos);
                    let slot = SlotIdx::new(i);

                    double_entries.set_entry(slot, entry, &mut state, |_| 0);
                    inserted_entries.insert(i, (*k_pos, *v_pos));
                    state.occupied_count += 1;
                }
            } // Drop to ensure data is persisted

            // Reload and verify
            {
                let store = MMapFile::from_file(&file_path).unwrap();
                let fixed_entries = FixedVec::new(store);
                let double_entries = DoubleArrayEntries::new(fixed_entries);

                for (slot_idx, (expected_k, expected_v)) in inserted_entries.iter() {
                    let retrieved = double_entries.get_entry(SlotIdx::new(*slot_idx));
                    prop_assert!(retrieved.is_some(), "Entry at slot {} should exist after reload", slot_idx);
                    let entry = retrieved.unwrap();
                    prop_assert_eq!(entry.key_pos(), HeapIdx::from(*expected_k));
                    prop_assert_eq!(entry.value_pos(), HeapIdx::from(*expected_v));
                    prop_assert!(entry.is_occupied());
                }
            }
        }
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
        if state.reindex_offset >= 0 && self.old_entries.is_some() {
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
                state.reindex_offset = -1;
                if let Some(old_entries) = self.old_entries.take() {
                    old_entries.purge();
                }
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
}
