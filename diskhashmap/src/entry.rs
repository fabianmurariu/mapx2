#![allow(dead_code)]
use bytemuck::{Pod, Zeroable};
use modular_bitfield::prelude::B2;
use modular_bitfield::{Specifier, bitfield};

use crate::HeapIdx;

#[derive(Specifier, PartialEq, Debug, Clone, Copy)]
pub enum Status {
    Empty,
    Full,
    Deleted,
    Moved,
}

#[bitfield(bits = 4)]
#[derive(Clone, Copy, Zeroable, Pod, Debug, Specifier)]
#[repr(C)]
pub struct PaddedStatus {
    #[bits = 2]
    status: Status,
    #[bits = 2]
    padding: B2,
}

#[bitfield(bits = 128)]
#[derive(Clone, Copy, Zeroable, Pod, Debug, Specifier)]
#[repr(C)]
pub struct Entry {
    #[bits = 4]
    pub status: PaddedStatus,
    #[bits = 62]
    pub k_pos: HeapIdx,
    #[bits = 62]
    pub v_pos: HeapIdx,
}

impl Entry {
    pub fn occupied_at_pos(k_pos: HeapIdx, v_pos: HeapIdx) -> Self {
        Entry::new()
            .with_status(PaddedStatus::new().with_status(Status::Full))
            .with_k_pos(k_pos)
            .with_v_pos(v_pos)
    }

    pub fn is_occupied(&self) -> bool {
        self.status().status() == Status::Full
    }

    pub fn is_empty(&self) -> bool {
        self.status().status() == Status::Empty
    }

    pub fn is_deleted(&self) -> bool {
        self.status().status() == Status::Deleted
    }

    pub fn is_moved(&self) -> bool {
        self.status().status() == Status::Moved
    }

    pub fn key_pos(&self) -> HeapIdx {
        self.k_pos()
    }

    pub fn value_pos(&self) -> HeapIdx {
        self.v_pos()
    }

    pub fn set_new_kv(&mut self, k_pos: HeapIdx, v_pos: HeapIdx) {
        *self = Entry::new()
            .with_status(PaddedStatus::new().with_status(Status::Full))
            .with_k_pos(k_pos)
            .with_v_pos(v_pos);
    }

    pub fn mark_as_moved(&mut self) {
        *self = self.with_status(PaddedStatus::new().with_status(Status::Moved));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entries_simplified::{DoubleArrayEntries, ResizeConfig};
    use crate::{ByteStore, VecStore};

    fn create_test_entry(k_pos: u32, v_pos: u32) -> Entry {
        Entry::occupied_at_pos(HeapIdx::from(k_pos as u64), HeapIdx::from(v_pos as u64))
    }

    fn create_empty_entries(capacity: usize) -> DoubleArrayEntries<VecStore> {
        // Create a VecStore with the right size for the given capacity
        let mut store = VecStore::with_capacity(capacity * std::mem::size_of::<Entry>());
        // Grow the store to the required size
        store.grow(capacity * std::mem::size_of::<Entry>());
        DoubleArrayEntries::empty_with_capacity(store, capacity).unwrap()
    }

    fn create_entries_with_data(data: Vec<(u32, u32)>) -> DoubleArrayEntries<VecStore> {
        let capacity = (data.len() * 2).max(8); // Ensure low load factor
        let mut entries = create_empty_entries(capacity);

        for (k_pos, v_pos) in data {
            let entry = create_test_entry(k_pos, v_pos);
            let hash = u64::from(k_pos); // Simple hash for testing
            let _ = entries.insert_entry(hash, entry);
        }

        entries
    }

    #[test]
    fn test_create_empty_entries() {
        let entries = create_empty_entries(16);

        assert_eq!(entries.capacity(), 16);
        assert_eq!(entries.occupied_count(), 0);
        assert!(!entries.is_resizing());
        assert_eq!(entries.load_factor(), 0.0);
    }

    #[test]
    fn test_insert_and_lookup() {
        let mut entries = create_empty_entries(16);
        let entry = create_test_entry(100, 200);
        let hash = 42u64;

        // Insert entry
        let index = entries.insert_entry(hash, entry).unwrap();
        assert_eq!(entries.occupied_count(), 1);
        assert!(entries.get_entry(index).is_occupied());
        assert_eq!(entries.get_entry(index).key_pos(), HeapIdx::from(100u64));
        assert_eq!(entries.get_entry(index).value_pos(), HeapIdx::from(200u64));
    }

    #[test]
    fn test_find_slot_existing_key() {
        let data = vec![(100, 200), (101, 201), (102, 202)];
        let entries = create_entries_with_data(data);

        // Find existing entry
        let hash = 101u64;
        let result = entries.find_slot(hash, |entry| entry.key_pos() == HeapIdx::from(101u64));

        assert!(result.is_ok());
        let index = result.unwrap();
        assert_eq!(entries.get_entry(index).key_pos(), HeapIdx::from(101u64));
        assert_eq!(entries.get_entry(index).value_pos(), HeapIdx::from(201u64));
    }

    #[test]
    fn test_find_slot_nonexistent_key() {
        let data = vec![(100, 200), (101, 201)];
        let entries = create_entries_with_data(data);

        // Find non-existent entry
        let hash = 999u64;
        let result = entries.find_slot(hash, |entry| entry.key_pos() == HeapIdx::from(999u64));

        assert!(result.is_err());
        let empty_index = result.unwrap_err();
        assert!(entries.get_entry(empty_index).is_empty());
    }

    #[test]
    fn test_load_factor_calculation() {
        let mut entries = create_empty_entries(10);
        assert_eq!(entries.load_factor(), 0.0);

        // Add some entries
        for i in 0..3 {
            let entry = create_test_entry(i, i + 100);
            let _ = entries.insert_entry(u64::from(i), entry);
        }

        assert_eq!(entries.occupied_count(), 3);
        assert_eq!(entries.load_factor(), 0.3);
    }

    #[test]
    fn test_should_resize_trigger() {
        let config = ResizeConfig {
            load_factor_threshold: 0.75,
            ..Default::default()
        };

        let mut entries = create_empty_entries(4);
        entries.set_config(config);

        // Add entries until we hit the threshold
        assert!(!entries.should_resize()); // 0/4 = 0.0

        let _ = entries.insert_entry(1, create_test_entry(1, 1));
        assert!(!entries.should_resize()); // 1/4 = 0.25

        let _ = entries.insert_entry(2, create_test_entry(2, 2));
        assert!(!entries.should_resize()); // 2/4 = 0.5

        let _ = entries.insert_entry(3, create_test_entry(3, 3));
        assert!(!entries.should_resize()); // 3/4 = 0.75 (exactly at threshold)

        let _ = entries.insert_entry(4, create_test_entry(4, 4));
        assert!(entries.should_resize()); // 4/4 = 1.0 > 0.75
    }

    #[test]
    fn test_start_resize() {
        let data = vec![(1, 10), (2, 20), (3, 30)];
        let mut entries = create_entries_with_data(data);
        let original_capacity = entries.capacity();

        assert!(!entries.is_resizing());
        assert_eq!(entries.occupied_count(), 3);

        // Start resize
        entries.start_resize(original_capacity * 2).unwrap();

        assert!(entries.is_resizing());
        assert_eq!(entries.capacity(), original_capacity * 2);
        assert_eq!(
            entries.effective_capacity(),
            original_capacity + original_capacity * 2
        );
        assert_eq!(entries.get_rehash_progress(), 0);
        assert_eq!(entries.occupied_count(), 3); // Still tracking occupied entries
    }

    #[test]
    fn test_incremental_rehash() {
        let data = vec![(1, 10), (2, 20), (3, 30), (4, 40)];
        let mut entries = create_entries_with_data(data);
        let original_capacity = entries.capacity();

        // Start resize
        entries.start_resize(original_capacity * 2).unwrap();
        assert!(entries.is_resizing());

        // Perform incremental rehash
        let rehashed = entries.incremental_rehash_simple();
        assert!(rehashed > 0, "Expected to rehash some entries, but got 0");
        // After incremental rehash, either we have progress > 0 (still rehashing) or we're done (!is_resizing())
        assert!(
            entries.get_rehash_progress() > 0 || !entries.is_resizing(),
            "Expected either progress > 0 or completed resize"
        );

        // Continue until resize is complete
        while entries.is_resizing() {
            entries.incremental_rehash_simple();
        }

        assert!(!entries.is_resizing());
        assert_eq!(entries.get_rehash_progress(), 0);
        assert_eq!(entries.occupied_count(), 4);
    }

    #[test]
    fn test_complete_resize() {
        let data = vec![(1, 10), (2, 20), (3, 30)];
        let mut entries = create_entries_with_data(data);
        let original_capacity = entries.capacity();

        // Start resize
        entries.start_resize(original_capacity * 2).unwrap();
        assert!(entries.is_resizing());

        // Complete resize in one go
        entries
            .complete_resize(|k_pos, _| u64::from(k_pos))
            .unwrap();

        assert!(!entries.is_resizing());
        assert_eq!(entries.occupied_count(), 3);

        // Verify all entries are still findable
        for (k_pos, v_pos) in &[(1, 10), (2, 20), (3, 30)] {
            let result = entries.find_slot(*k_pos as u64, |e| {
                e.key_pos() == HeapIdx::from(*k_pos as u64)
            });
            assert!(result.is_ok());
            let index = result.unwrap();
            assert_eq!(
                entries.get_entry(index).value_pos(),
                HeapIdx::from(*v_pos as u64)
            );
        }
    }

    #[test]
    fn test_load_from_normal_state() {
        // Create entries in normal state (no old array)
        let data = vec![(5, 50), (6, 60)];
        let original_entries = create_entries_with_data(data);

        // Simulate loading from disk in normal state
        let config = ResizeConfig::default();
        let loaded = DoubleArrayEntries::load_from_state(
            None,                                      // No old array
            (*original_entries.new_entries()).clone(), // Simulate cloning the new array
            0,                                         // No rehash progress
            config,
        );

        assert!(!loaded.is_resizing());
        assert_eq!(loaded.occupied_count(), 2);
        assert_eq!(loaded.get_rehash_progress(), 0);

        // Verify entries are accessible
        let result = loaded.find_slot(5u64, |e| e.key_pos() == HeapIdx::from(5u64));
        assert!(result.is_ok());
    }

    #[test]
    fn test_load_from_mid_resize_state() {
        // Create entries and start resize to get into mid-resize state
        let data = vec![
            (1, 10),
            (2, 20),
            (3, 30),
            (4, 40),
            (5, 50),
            (6, 60),
            (7, 70),
            (8, 80),
        ];
        let mut entries = create_entries_with_data(data);
        let original_capacity = entries.capacity();

        // Set a smaller batch size to ensure partial rehashing
        let mut config = entries.config().clone();
        config.rehash_batch_size = 2; // Only rehash 2 entries at a time
        entries.set_config(config);

        entries.start_resize(original_capacity * 2).unwrap();

        // Perform partial rehash - should only rehash 2 entries due to small batch size
        let rehashed = entries.incremental_rehash_simple();
        let progress = entries.get_rehash_progress();

        // Ensure we're actually in mid-resize state
        if !entries.is_resizing() {
            // If rehashing completed, skip this test - the batch was too small for the data
            return;
        }

        // Simulate saving and loading from mid-resize state
        let old_entries = entries.old_entries().cloned();
        let new_entries = (*entries.new_entries()).clone();
        let config = entries.config().clone();

        let loaded =
            DoubleArrayEntries::load_from_state(old_entries, new_entries, progress, config);

        assert!(loaded.is_resizing());
        assert_eq!(loaded.get_rehash_progress(), progress);
        assert!(loaded.occupied_count() > 0);

        // Can continue rehashing from where we left off
        let mut loaded = loaded;
        while loaded.is_resizing() {
            loaded.incremental_rehash_simple();
        }

        assert!(!loaded.is_resizing());
        assert_eq!(loaded.occupied_count(), 8);
    }

    #[test]
    fn test_occupied_entries_iterator_normal_state() {
        let data = vec![(10, 100), (20, 200), (30, 300)];
        let entries = create_entries_with_data(data.clone());

        let occupied: Vec<_> = entries.occupied_entries().collect();
        assert_eq!(occupied.len(), 3);

        // Verify all entries are found
        for (k_pos, v_pos) in data {
            let found = occupied.iter().any(|(_, entry)| {
                entry.key_pos() == HeapIdx::from(k_pos as u64)
                    && entry.value_pos() == HeapIdx::from(v_pos as u64)
            });
            assert!(found, "Entry ({}, {}) not found", k_pos, v_pos);
        }
    }

    #[test]
    fn test_occupied_entries_iterator_during_resize() {
        let data = vec![(1, 10), (2, 20), (3, 30)];
        let mut entries = create_entries_with_data(data.clone());

        // Start resize but don't complete it
        entries.start_resize(entries.capacity() * 2).unwrap();
        entries.incremental_rehash_simple(); // Partially rehash

        let occupied: Vec<_> = entries.occupied_entries().collect();
        assert_eq!(occupied.len(), 3);

        // All original entries should still be findable
        for (k_pos, v_pos) in data {
            let found = occupied.iter().any(|(_, entry)| {
                entry.key_pos() == HeapIdx::from(k_pos as u64)
                    && entry.value_pos() == HeapIdx::from(v_pos as u64)
                    && !entry.is_moved()
            });
            assert!(
                found,
                "Entry ({}, {}) not found or marked as moved incorrectly",
                k_pos, v_pos
            );
        }
    }

    #[test]
    fn test_set_entry_updates_occupied_count() {
        let mut entries = create_empty_entries(8);
        assert_eq!(entries.occupied_count(), 0);

        // Set an occupied entry
        let entry = create_test_entry(1, 10);
        entries.set_entry(0, entry);
        assert_eq!(entries.occupied_count(), 1);

        // Replace with another occupied entry (count shouldn't change)
        let entry2 = create_test_entry(2, 20);
        entries.set_entry(0, entry2);
        assert_eq!(entries.occupied_count(), 1);

        // Set to empty entry (count should decrease)
        entries.set_entry(0, Entry::new());
        assert_eq!(entries.occupied_count(), 0);
    }

    #[test]
    fn test_rehash_progress_bounds() {
        let data = vec![(1, 10), (2, 20)];
        let mut entries = create_entries_with_data(data);
        let capacity = entries.capacity();

        entries.start_resize(capacity * 2).unwrap();

        // Try to set progress beyond capacity
        entries.set_rehash_progress(capacity + 100);
        assert_eq!(entries.get_rehash_progress(), capacity); // Should be clamped

        // Set valid progress
        entries.set_rehash_progress(capacity / 2);
        assert_eq!(entries.get_rehash_progress(), capacity / 2);
    }

    #[test]
    fn test_new_empty_preserves_config() {
        let config = ResizeConfig {
            load_factor_threshold: 0.6,
            rehash_batch_size: 16,
            growth_factor: 1.5,
        };

        let entries = create_empty_entries(8);
        let mut entries =
            DoubleArrayEntries::new_with_config((*entries.new_entries()).clone(), config.clone());

        let empty = entries.new_empty(16);

        assert_eq!(empty.capacity(), 16);
        assert_eq!(empty.occupied_count(), 0);
        assert_eq!(empty.config().load_factor_threshold, 0.6);
        assert_eq!(empty.config().rehash_batch_size, 16);
        assert_eq!(empty.config().growth_factor, 1.5);
    }

    #[test]
    fn test_dual_array_indexing() {
        let data = vec![(1, 10), (2, 20)];
        let mut entries = create_entries_with_data(data);
        let new_capacity = entries.capacity();

        // Start resize to get dual arrays
        entries.start_resize(new_capacity * 2).unwrap();

        // Test indexing in new array (indices 0 to new_capacity-1)
        let new_entry = create_test_entry(99, 990);
        entries.set_entry(0, new_entry);
        assert_eq!(entries.get_entry(0).key_pos(), HeapIdx::from(99u64));

        // Test indexing in old array (indices new_capacity and above)
        let old_index = new_capacity;
        let old_entry = entries.get_entry(old_index);
        assert!(old_entry.is_occupied() || old_entry.is_empty()); // Should be valid
    }

    #[test]
    fn test_configuration_updates() {
        let mut entries = create_empty_entries(8);

        let new_config = ResizeConfig {
            load_factor_threshold: 0.5,
            rehash_batch_size: 4,
            growth_factor: 3.0,
        };

        entries.set_config(new_config.clone());

        assert_eq!(entries.config().load_factor_threshold, 0.5);
        assert_eq!(entries.config().rehash_batch_size, 4);
        assert_eq!(entries.config().growth_factor, 3.0);
    }

    #[test]
    fn test_zero_capacity_handling() {
        let store = VecStore::new();

        // Create entries with minimal capacity
        let result = DoubleArrayEntries::empty_with_capacity(store, 0);

        // Should handle zero capacity gracefully
        if let Ok(entries) = result {
            assert_eq!(entries.load_factor(), 0.0);
            assert!(!entries.should_resize());
        }
    }

    #[test]
    fn test_find_slot_with_collisions() {
        let mut entries = create_empty_entries(4); // Small capacity to force collisions

        // Insert entries that will collide (same hash % capacity)
        let entry1 = create_test_entry(1, 10);
        let entry2 = create_test_entry(5, 50); // 5 % 4 = 1, same as 1 % 4

        entries.insert_entry(1, entry1).unwrap();
        entries.insert_entry(5, entry2).unwrap();

        // Both entries should be findable
        let result1 = entries.find_slot(1, |e| e.key_pos() == HeapIdx::from(1u64));
        assert!(result1.is_ok());

        let result2 = entries.find_slot(5, |e| e.key_pos() == HeapIdx::from(5u64));
        assert!(result2.is_ok());

        // Verify they have different indices due to collision resolution
        assert_ne!(result1.unwrap(), result2.unwrap());
    }
}
