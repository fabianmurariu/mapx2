use crate::byte_store::ByteStore;
use std::{mem::size_of, ops::Index};

/// A slotted byte store that stores offsets at the beginning and data at the end.
///
/// Layout:
/// [offset_0][offset_1]...[offset_n][free_space][data_n]...[data_1][data_0]
///
/// Offsets are cumulative lengths from the end: [0, len_0, len_0+len_1, ...]
/// This means offset_i represents the total bytes from buffer end to start of entry i.
/// Data grows from the end towards the beginning.
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

#[derive(Debug, Clone, Copy)]
pub struct BuffersSlice<'a, T: ByteStore> {
    buffer: &'a T,
    start: usize,
    end: usize,
}

impl<'a, T: ByteStore> BuffersSlice<'a, T> {
    /// Create a slice of this BuffersSlice from [start, end)
    pub fn slice(&self, start: usize, end: usize) -> BuffersSlice<'a, T> {
        assert!(start <= end, "start must be <= end");
        assert!(end <= self.len(), "end out of bounds");
        BuffersSlice {
            buffer: self.buffer,
            start: self.start + start,
            end: self.start + end,
        }
    }

    /// Return the number of entries in this slice
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Return a consuming iterator over the entries in this slice
    pub fn iter(self) -> BuffersSliceIter<'a, T> {
        BuffersSliceIter {
            slice: self,
            pos: 0,
        }
    }

    /// Get the stored cumulative length offset for a given index in the slice
    fn get_cumulative_offset(&self, index: usize) -> Option<usize> {
        let global_index = self.start + index;
        let offset_size = size_of::<usize>();
        let buffer = self.buffer.as_ref();
        let offset_pos = global_index * offset_size;
        if offset_pos + offset_size > buffer.len() {
            return None;
        }
        let offset_bytes: [u8; 8] = buffer[offset_pos..offset_pos + offset_size]
            .try_into()
            .ok()?;
        Some(usize::from_le_bytes(offset_bytes))
    }

    /// Get a reference to the data at the given index in the slice
    pub fn get(&self, index: usize) -> Option<&'a [u8]> {
        if index >= self.len() {
            return None;
        }
        let buffer = self.buffer.as_ref();
        let buffer_len = buffer.len();

        // Get cumulative lengths from buffer end
        let end_cumulative = self.get_cumulative_offset(index)?;
        let start_cumulative = if index == 0 {
            if self.start == 0 {
                0
            } else {
                // get the offset for the entry before the slice
                let offset_size = size_of::<usize>();
                let offset_pos = (self.start - 1) * offset_size;
                let offset_bytes: [u8; 8] = buffer[offset_pos..offset_pos + offset_size]
                    .try_into()
                    .ok()?;
                usize::from_le_bytes(offset_bytes)
            }
        } else {
            self.get_cumulative_offset(index - 1)?
        };

        // Convert cumulative lengths to absolute positions
        let start_offset = buffer_len - end_cumulative;
        let end_offset = buffer_len - start_cumulative;

        if start_offset > end_offset || end_offset > buffer_len {
            return None;
        }

        Some(&buffer[start_offset..end_offset])
    }
}

pub struct BuffersSliceIter<'a, T: ByteStore> {
    slice: BuffersSlice<'a, T>,
    pos: usize,
}

impl<'a, T: ByteStore> Iterator for BuffersSliceIter<'a, T> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.pos;
        if idx < self.slice.len() {
            self.pos += 1;
            self.slice.get(idx)
        } else {
            None
        }
    }
}

impl<'a, B> IntoIterator for BuffersSlice<'a, B>
where
    B: ByteStore,
{
    type Item = &'a [u8];
    type IntoIter = BuffersSliceIter<'a, B>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
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

    /// Create a slice of the buffers from [start, end)
    pub fn slice(&self, start: usize, end: usize) -> BuffersSlice<'_, T> {
        assert!(start <= end, "start must be <= end");
        assert!(end <= self.len(), "end out of bounds");
        BuffersSlice {
            buffer: &self.buffer,
            start,
            end,
        }
    }

    /// Return an iterator over all entries (by reference)
    pub fn iter(&self) -> BuffersSliceIter<'_, T> {
        self.slice(0, self.len()).iter()
    }

    /// Append a byte slice to the store, returning the index of the stored data
    pub fn append(&mut self, bytes: impl AsRef<[u8]>) -> usize {
        let bytes = bytes.as_ref();
        let offset_size = size_of::<usize>();
        let needed_space = offset_size + bytes.len();

        // Check if we have enough space, if not, grow the buffer
        loop {
            let offsets_end = self.offsets_end();
            if offsets_end + needed_space <= self.data_end {
                break; // We have enough space
            }

            // Grow the buffer and move existing data
            let old_len = self.buffer.as_ref().len();
            let old_data_end = self.data_end;
            let data_size = old_len - old_data_end;

            // Grow the buffer directly to accommodate the new data
            let buffer_len = self.buffer.as_ref().len();
            let free_space = self.free_space();
            let additional_space = (free_space + needed_space)
                .next_power_of_two()
                .max(buffer_len * 2);
            self.buffer.grow(additional_space);

            let new_len = self.buffer.as_ref().len();
            let new_data_end = new_len - data_size;

            // Move existing data from old position to new position
            if data_size > 0 {
                let buffer = self.buffer.as_mut();
                // Copy data from [old_data_end..old_len] to [new_data_end..new_len]
                buffer.copy_within(old_data_end..old_len, new_data_end);
                // Zero out the old data area
                buffer[old_data_end..new_data_end].fill(0);

                // Offsets don't need updating! They're relative to buffer end
            }

            // Update data_end
            self.data_end = new_data_end;
        }

        // Calculate new data position
        let new_data_end = self.data_end - bytes.len();

        // Write the data at the end
        let buffer = self.buffer.as_mut();
        buffer[new_data_end..self.data_end].copy_from_slice(bytes);

        // Calculate cumulative length from buffer end
        let buffer_len = buffer.len();
        let cumulative_length = buffer_len - new_data_end;

        // Write the cumulative offset at the beginning
        let offset_pos = self.count * offset_size;
        let offset_bytes = cumulative_length.to_le_bytes();
        buffer[offset_pos..offset_pos + offset_size].copy_from_slice(&offset_bytes);

        // Update state
        let index = self.count;
        self.count += 1;
        self.data_end = new_data_end;

        index
    }

    fn offsets_end(&self) -> usize {
        self.count * size_of::<usize>()
    }

    /// Get the number of stored entries
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get the stored cumulative length offset for a given index
    fn get_cumulative_offset(&self, index: usize) -> Option<usize> {
        if index >= self.count {
            return None;
        }

        let buffer = self.buffer.as_ref();
        let offset_size = size_of::<usize>();
        let offset_pos = index * offset_size;
        let offset_bytes: [u8; 8] = buffer[offset_pos..offset_pos + offset_size]
            .try_into()
            .ok()?;
        Some(usize::from_le_bytes(offset_bytes))
    }

    /// Get a reference to the data at the given index
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        if index >= self.count {
            return None;
        }

        let buffer = self.buffer.as_ref();
        let buffer_len = buffer.len();

        // Get cumulative lengths from buffer end
        let end_cumulative = self.get_cumulative_offset(index)?;
        let start_cumulative = if index == 0 {
            0
        } else {
            self.get_cumulative_offset(index - 1)?
        };

        // Convert cumulative lengths to absolute positions
        let start_offset = buffer_len - end_cumulative;
        let end_offset = buffer_len - start_cumulative;

        if start_offset > end_offset || end_offset > buffer_len {
            return None;
        }

        Some(&buffer[start_offset..end_offset])
    }

    /// Get the remaining free space in the store
    #[allow(clippy::implicit_saturating_sub)]
    pub fn free_space(&self) -> usize {
        let offsets_end = self.offsets_end();
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

impl<B> Index<usize> for Buffers<B>
where
    B: ByteStore,
{
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("Index out of bounds")
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
    }

    #[test]
    fn test_buffers_slice_and_iter() {
        let mut store = Buffers::new(vec![0u8; 1024]);
        for d in ["zero", "one", "two", "three", "four", "five"] {
            store.append(d);
        }

        // Full slice
        let slice = store.slice(0, store.len());
        assert_eq!(slice.len(), store.len());
        for (i, entry) in slice.iter().enumerate() {
            assert_eq!(entry, store.get(i).unwrap());
        }

        // Subslice
        let sub = store.slice(2, 5);
        assert_eq!(sub.len(), 3);
        assert_eq!(sub.get(0).unwrap(), b"two");
        assert_eq!(sub.get(2).unwrap(), b"four".as_ref());
        let collected: Vec<_> = sub.clone().iter().collect();
        assert_eq!(
            collected,
            vec![b"two".as_ref(), b"three".as_ref(), b"four".as_ref()]
        );

        // Nested slice (consume the slice)
        let nested = sub.slice(1, 3);
        assert_eq!(nested.len(), 2);
        let mut iter = nested.iter();
        assert_eq!(iter.next().unwrap(), b"three".as_ref());
        assert_eq!(iter.next().unwrap(), b"four".as_ref());
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_buffers_iter_equivalence() {
        let mut store = Buffers::new(vec![0u8; 512]);
        for i in 0..10 {
            let s = format!("entry{i}");
            store.append(s.as_bytes());
        }
        let manual: Vec<_> = (0..store.len()).map(|i| store.get(i).unwrap()).collect();
        let iter: Vec<_> = store.iter().collect();
        assert_eq!(manual, iter);
    }

    proptest! {
        #[test]
        fn prop_slice_iter_equivalence(
            data_list in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 0..20),
                1..30
            )
        ) {
            let mut store = Buffers::new(vec![0u8; 4096]);
            for data in &data_list {
                store.append(data);
            }
            let len = store.len();
            prop_assert!(len == data_list.len());

            // Try all valid slices
            for start in 0..=len {
                for end in start..=len {
                    let slice = store.slice(start, end);
                    let expected: Vec<_> = (start..end).map(|i| store.get(i).unwrap()).collect();
                    let iter: Vec<_> = slice.clone().iter().collect();
                    prop_assert_eq!(expected, iter);
                    prop_assert_eq!(slice.len(), end - start);
                }
            }
        }

        #[test]
        fn prop_nested_slice_equivalence(
            data_list in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 0..10),
                5..20
            )
        ) {
            let mut store = Buffers::new(vec![0u8; 2048]);
            for data in &data_list {
                store.append(data);
            }
            let len = store.len();
            if len < 5 { return Ok(()); }
            let outer = store.slice(1, len-1);
            let mid = outer.slice(1, outer.len()-1);
            let expected: Vec<_> = (2..len-2).map(|i| store.get(i).unwrap()).collect();
            let iter: Vec<_> = mid.iter().collect();
            prop_assert_eq!(expected, iter);
        }
    }

    proptest! {
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
    }

    proptest! {
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
    }

    proptest! {
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

    #[test]
    fn test_offset_system_understanding() {
        let mut store = Buffers::new(vec![0u8; 64]);

        // Add some data to understand how offsets work
        let data1 = b"hello"; // 5 bytes
        let data2 = b"world"; // 5 bytes
        let data3 = b"rust"; // 4 bytes

        let idx1 = store.append(data1);
        let idx2 = store.append(data2);
        let idx3 = store.append(data3);

        // Check cumulative offsets - these are cumulative lengths from buffer end
        let offset1 = store.get_cumulative_offset(idx1).unwrap();
        let offset2 = store.get_cumulative_offset(idx2).unwrap();
        let offset3 = store.get_cumulative_offset(idx3).unwrap();

        // Verify the data layout
        assert_eq!(store.get(idx1).unwrap(), data1);
        assert_eq!(store.get(idx2).unwrap(), data2);
        assert_eq!(store.get(idx3).unwrap(), data3);

        // New system: offsets are cumulative lengths from buffer end
        // Data layout: [offset_0][offset_1][offset_2][free_space][data_2][data_1][data_0]
        // offset_0 = len(data_0) = 5
        // offset_1 = len(data_0) + len(data_1) = 5 + 5 = 10
        // offset_2 = len(data_0) + len(data_1) + len(data_2) = 5 + 5 + 4 = 14

        // The offsets should be in ascending order as they're cumulative
        assert_eq!(offset1, 5); // Just "hello"
        assert_eq!(offset2, 10); // "hello" + "world"
        assert_eq!(offset3, 14); // "hello" + "world" + "rust"
        assert!(offset1 < offset2);
        assert!(offset2 < offset3);
    }

    #[test]
    fn test_offset_system_with_growth() {
        let mut store = Buffers::new(vec![0u8; 32]);

        // Add data that will trigger growth
        let data1 = b"first";
        let data2 = b"second";

        let idx1 = store.append(data1);
        let offset1_before = store.get_cumulative_offset(idx1).unwrap();

        let idx2 = store.append(data2);
        let offset2_before = store.get_cumulative_offset(idx2).unwrap();

        // This should trigger growth
        let data3 = b"this_will_trigger_growth_because_its_long";
        let idx3 = store.append(data3);

        let offset1_after = store.get_cumulative_offset(idx1).unwrap();
        let offset2_after = store.get_cumulative_offset(idx2).unwrap();
        let offset3_after = store.get_cumulative_offset(idx3).unwrap();

        // Verify data is still correct
        assert_eq!(store.get(idx1).unwrap(), data1);
        assert_eq!(store.get(idx2).unwrap(), data2);
        assert_eq!(store.get(idx3).unwrap(), data3);

        // The offsets should NOT have changed when buffer grew!
        // They're cumulative lengths from end, so they stay the same
        assert_eq!(offset1_after, offset1_before); // Still same cumulative length
        assert_eq!(offset2_after, offset2_before); // Still same cumulative length

        // Cumulative offsets are in ascending order
        assert!(offset1_after < offset2_after);
        assert!(offset2_after < offset3_after);
    }

    #[test]
    fn test_detailed_offset_analysis() {
        // This test demonstrates the CURRENT system vs what you expected
        let mut store = Buffers::new(vec![0u8; 32]);

        // Add first piece of data
        let data1 = b"AAAA"; // 4 bytes
        let idx1 = store.append(data1);
        let offset1 = store.get_cumulative_offset(idx1).unwrap();

        // New system: offset is cumulative length from buffer end
        // With 32-byte buffer and 4-byte data:
        // Layout: [8-byte offset][24 bytes free][4 bytes data]
        // offset1 should be 4 (cumulative length from end)
        assert_eq!(offset1, 4); // Cumulative length: just data1
        assert_eq!(store.data_end, 28); // data_end tracks where next data goes

        // Add second piece of data
        let data2 = b"BBBB"; // 4 bytes
        let idx2 = store.append(data2);
        let offset2 = store.get_cumulative_offset(idx2).unwrap();

        // Now layout: [8-byte offset1][8-byte offset2][16 bytes free][4 bytes data2][4 bytes data1]
        // offset2 should be 8 (cumulative length: data1 + data2)
        assert_eq!(offset2, 8); // Cumulative length: data1 + data2
        assert_eq!(store.data_end, 24);

        // The NEW SYSTEM:
        // - Offsets are cumulative lengths from end: [4, 8]
        // - When buffer grows, only data moves, offsets stay the same!
        // - Much simpler and more efficient!

        // Force growth by adding data that won't fit
        let large_data = vec![b'X'; 20]; // This should trigger growth
        let idx3 = store.append(&large_data);

        // After growth, the original offsets should NOT have changed!
        let offset1_after = store.get_cumulative_offset(idx1).unwrap();
        let offset2_after = store.get_cumulative_offset(idx2).unwrap();
        let offset3_after = store.get_cumulative_offset(idx3).unwrap();

        // Offsets stay the same because they're relative to buffer end
        assert_eq!(offset1_after, offset1); // Still 4 bytes from end
        assert_eq!(offset2_after, offset2); // Still 8 bytes from end
        assert_eq!(offset3_after, 28); // data1 + data2 + data3 = 4 + 4 + 20 = 28

        // Data is still retrievable correctly
        assert_eq!(store.get(idx1).unwrap(), data1);
        assert_eq!(store.get(idx2).unwrap(), data2);
        assert_eq!(store.get(idx3).unwrap(), &large_data);
    }
}
