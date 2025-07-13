use crate::byte_store::ByteStore;
use std::mem::size_of;

/// A slotted byte store that stores offsets at the beginning and data at the end.
///
/// Layout:
/// [offset_0][offset_1]...[offset_n][free_space][data_n]...[data_1][data_0]
///
/// Offsets grow from the beginning, data grows from the end towards the beginning.
///
/// # Example
///
/// ```
/// use diskmap::Buffers;
///
/// let mut store = Buffers::new(vec![0u8; 1024]);
///
/// // Add some data
/// let idx1 = store.append(b"hello");
/// let idx2 = store.append(b"world");
///
/// // Retrieve data
/// assert_eq!(store.get(idx1).unwrap(), b"hello");
/// assert_eq!(store.get(idx2).unwrap(), b"world");
/// assert_eq!(store.len(), 2);
/// ```
pub struct Buffers<T: ByteStore> {
    buffer: T,
    /// Number of slots (entries) currently stored
    count: usize,
    /// Current position where the next data will be written (grows backwards)
    data_end: usize,
}

impl<T: ByteStore> Buffers<T> {
    /// Create a new ByteStore with the given buffer
    pub fn new(buffer: T) -> Self {
        let data_end = buffer.as_ref().len();
        Self {
            buffer,
            count: 0,
            data_end,
        }
    }

    /// Append a byte slice to the store, returning the index of the stored data
    pub fn append(&mut self, bytes: &[u8]) -> usize {
        let offset_size = size_of::<usize>();
        let needed_space = offset_size + bytes.len();

        // Check if we have enough space, if not, grow the buffer
        loop {
            let offsets_end = self.count * offset_size;
            if offsets_end + needed_space <= self.data_end {
                break; // We have enough space
            }

            // Grow the buffer and move existing data
            let old_len = self.buffer.as_ref().len();
            let old_data_end = self.data_end;
            let data_size = old_len - old_data_end;

            // Grow the buffer
            self.buffer.grow();
            let new_len = self.buffer.as_ref().len();
            let new_data_end = new_len - data_size;

            // Move existing data from old position to new position
            if data_size > 0 {
                let buffer = self.buffer.as_mut();
                // Copy data from [old_data_end..old_len] to [new_data_end..new_len]
                buffer.copy_within(old_data_end..old_len, new_data_end);
                // Zero out the old data area
                buffer[old_data_end..new_data_end].fill(0);

                // Update all stored offsets to point to new positions
                let offset_size = size_of::<usize>();
                let offset_delta = new_data_end as i64 - old_data_end as i64;

                for i in 0..self.count {
                    let offset_pos = i * offset_size;
                    // Read current offset
                    let offset_bytes: [u8; 8] = buffer[offset_pos..offset_pos + offset_size]
                        .try_into()
                        .expect("Failed to read offset");
                    let old_offset = usize::from_le_bytes(offset_bytes);

                    // Update offset
                    let new_offset = (old_offset as i64 + offset_delta) as usize;
                    let new_offset_bytes = new_offset.to_le_bytes();
                    buffer[offset_pos..offset_pos + offset_size].copy_from_slice(&new_offset_bytes);
                }
            }

            // Update data_end
            self.data_end = new_data_end;
        }

        // Calculate new data position
        let new_data_end = self.data_end - bytes.len();

        // Write the data at the end
        let buffer = self.buffer.as_mut();
        buffer[new_data_end..self.data_end].copy_from_slice(bytes);

        // Write the offset at the beginning
        let offset_pos = self.count * offset_size;
        let offset_bytes = new_data_end.to_le_bytes();
        buffer[offset_pos..offset_pos + offset_size].copy_from_slice(&offset_bytes);

        // Update state
        let index = self.count;
        self.count += 1;
        self.data_end = new_data_end;

        index
    }

    /// Get the number of stored entries
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get a reference to the data at the given index
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        if index >= self.count {
            return None;
        }

        let buffer = self.buffer.as_ref();
        let offset_size = size_of::<usize>();

        // Read the offset
        let offset_pos = index * offset_size;
        let offset_bytes: [u8; 8] = buffer[offset_pos..offset_pos + offset_size]
            .try_into()
            .ok()?;
        let start_offset = usize::from_le_bytes(offset_bytes);

        // Calculate the end offset
        // Data is stored in reverse order, so:
        // - Entry 0 (first stored) is at the end
        // - Entry 1 (second stored) ends where entry 0 starts
        // - Entry i ends where entry (i-1) starts
        let end_offset = if index == 0 {
            // Last stored entry goes to the current buffer end
            buffer.len()
        } else {
            // Get the start of the previously stored entry (index - 1)
            let prev_offset_pos = (index - 1) * offset_size;
            let prev_offset_bytes: [u8; 8] = buffer[prev_offset_pos..prev_offset_pos + offset_size]
                .try_into()
                .ok()?;
            usize::from_le_bytes(prev_offset_bytes)
        };

        if start_offset > end_offset || end_offset > buffer.len() {
            return None;
        }

        Some(&buffer[start_offset..end_offset])
    }

    /// Get the remaining free space in the store
    pub fn free_space(&self) -> usize {
        let offset_size = size_of::<usize>();
        let offsets_end = self.count * offset_size;
        if self.data_end > offsets_end {
            self.data_end - offsets_end
        } else {
            0
        }
    }

    /// Clear all entries from the store
    pub fn clear(&mut self) {
        self.count = 0;
        self.data_end = self.buffer.as_ref().len();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_basic_operations() {
        let mut store = Buffers::new(vec![0u8; 1024]);

        // Test empty store
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        assert_eq!(store.get(0), None);

        // Add some data
        let data1 = b"hello";
        let data2 = b"world";
        let data3 = b"rust";

        let idx1 = store.append(data1);
        let idx2 = store.append(data2);
        let idx3 = store.append(data3);

        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);
        assert_eq!(idx3, 2);
        assert_eq!(store.len(), 3);
        assert!(!store.is_empty());

        // Retrieve data
        assert_eq!(store.get(0).unwrap(), data1);
        assert_eq!(store.get(1).unwrap(), data2);
        assert_eq!(store.get(2).unwrap(), data3);
        assert_eq!(store.get(3), None);
    }

    #[test]
    fn test_empty_data() {
        let mut store = Buffers::new(vec![0u8; 1024]);

        let idx = store.append(b"");
        assert_eq!(idx, 0);
        assert_eq!(store.get(0).unwrap(), b"");
    }

    #[test]
    fn test_auto_growing() {
        let mut store = Buffers::new(vec![0u8; 32]);

        // Add data that will require growing
        let mut indices = Vec::new();
        for i in 0..5 {
            let data = format!("data{i}");
            let idx = store.append(data.as_bytes());
            indices.push(idx);
        }

        // Verify we can read all stored data
        for (i, &idx) in indices.iter().enumerate() {
            let expected = format!("data{i}");
            assert_eq!(store.get(idx).unwrap(), expected.as_bytes());
        }

        // Should be able to add more data due to auto-growing
        let overflow_idx = store.append(b"overflow");
        assert_eq!(store.get(overflow_idx).unwrap(), b"overflow");
    }

    #[test]
    fn test_clear() {
        let mut store = Buffers::new(vec![0u8; 1024]);

        store.append(b"test1");
        store.append(b"test2");
        assert_eq!(store.len(), 2);

        store.clear();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        assert_eq!(store.get(0), None);

        // Should be able to add data again after clear
        let idx = store.append(b"after_clear");
        assert_eq!(idx, 0);
        assert_eq!(store.get(0).unwrap(), b"after_clear");
    }

    #[test]
    fn test_different_backing_types() {
        // Test with Vec
        let mut vec_store = Buffers::new(vec![0u8; 1024]);
        vec_store.append(b"vec_test");
        assert_eq!(vec_store.get(0).unwrap(), b"vec_test");

        // Test with array
        let mut array_store = Buffers::new([0u8; 1024]);
        array_store.append(b"array_test");
        assert_eq!(array_store.get(0).unwrap(), b"array_test");

        // Test with boxed slice
        let mut boxed_store = Buffers::new(vec![0u8; 1024].into_boxed_slice());
        boxed_store.append(b"boxed_test");
        assert_eq!(boxed_store.get(0).unwrap(), b"boxed_test");
    }

    proptest! {
        #[test]
        fn prop_test_store_retrieval(
            data_list in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 0..100),
                0..50
            )
        ) {
            let mut store = Buffers::new(vec![0u8; 8192]);
            let mut indices = Vec::new();

            // Store all data (will auto-grow as needed)
            for data in &data_list {
                let idx = store.append(data);
                indices.push(idx);
            }

            // Verify all stored data can be retrieved correctly
            for (i, &idx) in indices.iter().enumerate() {
                prop_assert_eq!(store.get(idx).unwrap(), data_list[i].as_slice());
            }

            // Verify indices are sequential
            for (i, &idx) in indices.iter().enumerate() {
                prop_assert_eq!(idx, i);
            }

            // Verify length is correct
            prop_assert_eq!(store.len(), indices.len());
        }

        #[test]
        fn prop_test_store_bounds(
            data in prop::collection::vec(any::<u8>(), 1..1000)
        ) {
            let mut store = Buffers::new(vec![0u8; 1024]);

            // Store the data (will auto-grow if needed)
            let idx = store.append(&data);

            // Storage should always succeed due to auto-growing
            prop_assert_eq!(store.get(idx).unwrap(), data.as_slice());
            prop_assert_eq!(store.len(), 1);
        }

        #[test]
        fn prop_test_no_out_of_bounds_access(
            data_list in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 0..50),
                0..20
            ),
            access_indices in prop::collection::vec(any::<usize>(), 0..30)
        ) {
            let mut store = Buffers::new(vec![0u8; 4096]);

            // Store data (will auto-grow as needed)
            let mut valid_indices = Vec::new();
            for data in &data_list {
                let idx = store.append(data);
                valid_indices.push(idx);
            }

            // Test access with various indices
            for &access_idx in &access_indices {
                let result = store.get(access_idx);
                if access_idx < valid_indices.len() {
                    // Should be able to access valid indices
                    prop_assert!(result.is_some());
                } else {
                    // Should return None for invalid indices
                    prop_assert_eq!(result, None);
                }
            }
        }

        #[test]
        fn prop_test_free_space_calculation(
            data_list in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 1..20),
                0..10
            )
        ) {
            let buffer_size = 1024;
            let mut store = Buffers::new(vec![0u8; buffer_size]);
            let initial_free_space = store.free_space();
            prop_assert_eq!(initial_free_space, buffer_size);

            for data in &data_list {
                let space_before = store.free_space();
                let _idx = store.append(data);
                let space_after = store.free_space();

                // Due to auto-growing, we might have more space than expected
                if space_before >= data.len() + size_of::<usize>() {
                    // No growth needed
                    prop_assert_eq!(space_after, space_before - (data.len() + size_of::<usize>()));
                } else {
                    // Buffer grew, so space_after should be positive
                    prop_assert!(space_after > 0);
                }
            }
        }
    }

    #[test]
    fn test_exact_interface_requirements() {
        let mut store = Buffers::new(vec![0u8; 1024]);

        // Test the exact interface: append(&mut self, bytes: &[u8]) -> usize
        let data1 = b"first";
        let data2 = b"second";
        let data3 = b"";

        let idx1 = store.append(data1);
        let idx2 = store.append(data2);
        let idx3 = store.append(data3);

        // Test len(&self) -> usize
        assert_eq!(store.len(), 3);

        // Test get(&self, i: usize) -> &[u8] (returns Option<&[u8]> for safety)
        assert_eq!(store.get(idx1).unwrap(), data1);
        assert_eq!(store.get(idx2).unwrap(), data2);
        assert_eq!(store.get(idx3).unwrap(), data3);

        // Verify indices are sequential starting from 0
        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);
        assert_eq!(idx3, 2);
    }
}
