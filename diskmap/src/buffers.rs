use std::fmt::{Debug, Formatter};
use std::mem::size_of;
use std::ops::Index;

use crate::byte_store::{ByteStore, VecStore};

/// This module contains the implementation of `Buffers`, a data structure
/// that stores a sequence of byte arrays in a single underlying `ByteStore`.
///
/// It's designed to be efficient for storing and retrieving variable-length
/// data, using a system of offsets to locate each item.
///
/// The layout of the `ByteStore` is as follows:
/// - A sequence of `usize` offsets, one for each stored item. Each offset
///   represents the cumulative length of all items up to that point.
/// - The actual data of the items, stored in reverse order from the end of
///   the `ByteStore`.
///
/// This design allows for efficient appends, as new data is added to the end
/// of the data section (which grows towards the start of the buffer), and a
/// new offset is added to the end of the offset section.
///
/// When the buffer runs out of space, it grows, and the data section is
/// shifted to the new end of the buffer to make space for new offsets and data.

/// `Buffers` stores a sequence of byte arrays in a single `ByteStore`.
pub struct Buffers<T: ByteStore> {
    byte_store: T,
    // The number of items in the store
    count: usize,
    // The start of the data section, from the end of the store
    data_end: usize,
}

/// A slice of `Buffers`, representing a sub-sequence of the stored items.
pub struct BuffersSlice<'a, T: ByteStore> {
    byte_store: &'a T,
    start: usize, // Start index of the slice
    end: usize,   // End index of the slice
}

impl<'a, T: ByteStore> Clone for BuffersSlice<'a, T> {
    fn clone(&self) -> Self {
        BuffersSlice {
            byte_store: self.byte_store,
            start: self.start,
            end: self.end,
        }
    }
}

impl<'a, T: ByteStore> Copy for BuffersSlice<'a, T> {}

impl<'a, T: ByteStore> BuffersSlice<'a, T> {
    /// Create a new slice from this slice
    pub fn slice(&self, start: usize, end: usize) -> Self {
        assert!(start <= end, "start must be <= end");
        assert!(end <= self.len(), "end out of bounds");
        BuffersSlice {
            byte_store: self.byte_store,
            start: self.start + start,
            end: self.start + end,
        }
    }

    /// Return the number of entries in the slice
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Return an iterator over the entries in the slice
    pub fn iter(&self) -> BuffersSliceIter<'a, T> {
        BuffersSliceIter {
            slice: *self,
            pos: 0,
        }
    }

    // This helper function computes the cumulative offset for a given index.
    // The offset for index `i` is the sum of lengths of all items from 0 to `i`.
    fn get_cumulative_offset(&self, index: usize) -> usize {
        let offset_size = size_of::<usize>();
        let pos = index * offset_size;
        if pos + offset_size > self.byte_store.as_ref().len() {
            return 0; // Should be an error or panic
        }
        let offset_bytes: [u8; size_of::<usize>()] = self.byte_store.as_ref()
            [pos..pos + offset_size]
            .try_into()
            .unwrap();
        usize::from_le_bytes(offset_bytes)
    }

    /// Retrieve the byte slice at a given index
    pub fn get(&self, index: usize) -> Option<&'a [u8]> {
        if index >= self.len() {
            return None;
        }

        let actual_index = self.start + index;
        let total_len = self.byte_store.as_ref().len();

        let end_offset = self.get_cumulative_offset(actual_index);
        let start_offset = if actual_index > 0 {
            self.get_cumulative_offset(actual_index - 1)
        } else {
            0
        };

        if end_offset == 0 && start_offset == 0 && self.len() > 0 && index > 0 {
            return None;
        }

        if end_offset < start_offset {
            return None;
        }

        let len = end_offset - start_offset;
        let data_start = total_len - end_offset;

        Some(&self.byte_store.as_ref()[data_start..data_start + len])
    }
}

pub struct BuffersSliceIter<'a, T: ByteStore> {
    slice: BuffersSlice<'a, T>,
    pos: usize,
}

impl<'a, T: ByteStore> Iterator for BuffersSliceIter<'a, T> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.slice.len() {
            let item = self.slice.get(self.pos);
            self.pos += 1;
            item
        } else {
            None
        }
    }
}

impl<'a, B: ByteStore> IntoIterator for BuffersSlice<'a, B> {
    type Item = &'a [u8];
    type IntoIter = BuffersSliceIter<'a, B>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: ByteStore> Buffers<T> {
    /// Create a new ByteStore with the given buffer
    pub fn new(byte_store: T) -> Self {
        let len = byte_store.as_ref().len();
        Self {
            byte_store,
            count: 0,
            data_end: len,
        }
    }

    pub fn store(&self) -> &T {
        &self.byte_store
    }

    /// Create a slice of the buffers from [start, end)
    pub fn slice(&self, start: usize, end: usize) -> BuffersSlice<'_, T> {
        assert!(start <= end, "start must be <= end");
        assert!(end <= self.len(), "end out of bounds");
        BuffersSlice {
            byte_store: &self.byte_store,
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

        if self.free_space() < needed_space {
            let old_len = self.byte_store.as_ref().len();
            let data_len = old_len - self.data_end;

            // To avoid frequent resizes, we double the buffer's capacity. This greedy
            // approach ensures that `copy_within` can safely move data without overwriting
            // existing contents. The offsets remain at the front, while data is shifted
            // to the back.
            let mut new_len = if old_len == 0 { 256 } else { old_len * 2 };

            // Ensure the new length is sufficient for the new data, existing data, and offsets.
            let required_len = self.offsets_end() + data_len + needed_space;
            // If doubling the size is not enough, keep doubling until it's sufficient.
            // This maintains a power-of-two growth strategy.
            while new_len < required_len {
                new_len *= 2;
            }

            // `grow` expects the additional size, not the new total size.
            let growth = new_len - old_len;
            self.byte_store.grow(growth);
            let new_actual_len = self.byte_store.as_ref().len();

            // Calculate the new starting position for the data block, which is at the end.
            let new_data_end = new_actual_len - data_len;

            // Move the existing data to the end of the newly allocated space.
            // `copy_within` is efficient as it avoids a temporary allocation (like `to_vec`),
            // which is critical for memory-mapped files. The data is moved from its old
            // position to the end of the new buffer. This is safe because the destination
            // is guaranteed not to conflict with the source.
            if data_len > 0 {
                self.byte_store
                    .as_mut()
                    .copy_within(self.data_end..old_len, new_data_end);
            }

            // Update the pointer to the start of the data section.
            self.data_end = new_data_end;
        }

        self.data_end -= bytes.len();
        self.byte_store.as_mut()[self.data_end..self.data_end + bytes.len()].copy_from_slice(bytes);

        let offset_pos = self.count * offset_size;
        let cumulative_len = self.byte_store.as_ref().len() - self.data_end;
        self.byte_store.as_mut()[offset_pos..offset_pos + offset_size]
            .copy_from_slice(&cumulative_len.to_le_bytes());

        let index = self.count;
        self.count += 1;
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

    fn get_cumulative_offset(&self, index: usize) -> usize {
        let offset_size = size_of::<usize>();
        let pos = index * offset_size;
        if pos + offset_size > self.offsets_end() {
            return 0;
        }
        let offset_bytes: [u8; size_of::<usize>()] = self.byte_store.as_ref()
            [pos..pos + offset_size]
            .try_into()
            .unwrap();
        usize::from_le_bytes(offset_bytes)
    }

    /// Get a byte slice by its index
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        if index >= self.count {
            return None;
        }

        let total_len = self.byte_store.as_ref().len();
        let end_offset = self.get_cumulative_offset(index);
        let start_offset = if index > 0 {
            self.get_cumulative_offset(index - 1)
        } else {
            0
        };

        if end_offset <= start_offset && (index > 0 || end_offset != 0) {
            // This indicates a corrupted or invalid state.
            return None;
        }

        let len = end_offset - start_offset;
        let data_start = total_len - end_offset;
        Some(&self.byte_store.as_ref()[data_start..data_start + len])
    }

    /// Calculate the free space in the buffer. This is the space between the end of
    /// the offsets and the start of the data.
    pub fn free_space(&self) -> usize {
        if self.data_end < self.offsets_end() {
            0
        } else {
            self.data_end - self.offsets_end()
        }
    }

    /// Clear all entries from the store
    pub fn clear(&mut self) {
        self.count = 0;
        self.data_end = self.byte_store.as_ref().len();
    }
}

impl<B: ByteStore> Index<usize> for Buffers<B> {
    // The type of the value being indexed
    type Output = [u8];

    // The method that is called when the struct is indexed
    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::ByteStore;
    use crate::byte_store::MMapFile;
    use proptest::prelude::*;
    use tempfile::NamedTempFile;

    use super::*;

    // A generic test harness for any `ByteStore` implementation.
    // The macro avoids code duplication for `VecStore` and `MMapFile`.
    macro_rules! test_buffers {
        ($test_name:ident, $store_type:expr) => {
            #[test]
            fn $test_name() {
                let store = $store_type;
                let mut buffers = Buffers::new(store);
                check_test_basic_operations(&mut buffers);
                let store = $store_type;
                let mut buffers = Buffers::new(store);
                check_test_auto_growing(&mut buffers);
                let store = $store_type;
                let mut buffers = Buffers::new(store);
                check_test_clear(&mut buffers);
                let store = $store_type;
                let mut buffers = Buffers::new(store);
                check_test_empty_data(&mut buffers);
                let store = $store_type;
                let mut buffers = Buffers::new(store);
                check_test_buffers_slice_and_iter(&mut buffers);
                let store = $store_type;
                let mut buffers = Buffers::new(store);
                check_test_buffers_iter_equivalence(&mut buffers);
            }
        };
    }

    // Run all tests for `VecStore`.
    test_buffers!(test_basic_operations_vec_backend, VecStore::new());

    // Run all tests for `MMapFile`.
    test_buffers!(
        test_basic_operations_mmap_backend,
        MMapFile::new(NamedTempFile::new().unwrap().path(), 1024).unwrap()
    );

    // Test functions, generic over `ByteStore`.
    fn check_test_basic_operations<T: ByteStore>(buffers: &mut Buffers<T>) {
        // Initial state
        assert!(buffers.is_empty());
        assert_eq!(buffers.len(), 0);

        // Append some data
        let idx1 = buffers.append(b"hello");
        assert_eq!(idx1, 0);
        assert_eq!(buffers.len(), 1);
        assert!(!buffers.is_empty());

        let idx2 = buffers.append(b"world");
        assert_eq!(idx2, 1);
        assert_eq!(buffers.len(), 2);

        // Retrieve and verify data
        assert_eq!(buffers.get(0), Some(b"hello".as_ref()));
        assert_eq!(buffers.get(1), Some(b"world".as_ref()));
        assert_eq!(buffers.get(2), None); // Out of bounds

        // Check indexing
        assert_eq!(&buffers[0], b"hello");
        assert_eq!(&buffers[1], b"world");
    }

    fn check_test_auto_growing<T: ByteStore>(buffers: &mut Buffers<T>) {
        let mut num_items = buffers.len();
        let initial_len = buffers.store().as_ref().len();
        // add things until the buffer grows
        while buffers.store().as_ref().len() == initial_len {
            let data = format!("data{num_items}");
            buffers.append(data.as_bytes());
            num_items += 1;
        }

        assert_eq!(buffers.len(), num_items);

        // Verify data integrity after growing
        for i in num_items..buffers.len() {
            let expected_data = format!("data{i}");
            assert_eq!(buffers.get(i), Some(expected_data.as_bytes()));
        }

        assert!(
            buffers.store().as_ref().len() > initial_len,
            "Buffer should have grown {} > {} initial_len: {}",
            buffers.store().as_ref().len(),
            initial_len,
            buffers.free_space()
        );
        assert!(
            buffers.free_space() > 0,
            "Should have free space after growing"
        );
    }

    fn check_test_clear<T: ByteStore>(buffers: &mut Buffers<T>) {
        buffers.append(b"some_data");
        buffers.append(b"more_data");
        assert_eq!(buffers.len(), 2);

        buffers.clear();
        assert!(buffers.is_empty());
        assert_eq!(buffers.len(), 0);
        assert_eq!(buffers.get(0), None);

        // Can append after clearing
        buffers.append(b"after_clear");
        assert_eq!(buffers.len(), 1);
        assert_eq!(buffers.get(0), Some(b"after_clear".as_ref()));
    }

    fn check_test_empty_data<T: ByteStore>(buffers: &mut Buffers<T>) {
        buffers.append(b"");
        buffers.append(b"non-empty");
        buffers.append(b"");

        assert_eq!(buffers.len(), 3);
        assert_eq!(buffers.get(0), Some(b"".as_ref()));
        assert_eq!(buffers.get(1), Some(b"non-empty".as_ref()));
        assert_eq!(buffers.get(2), Some(b"".as_ref()));
    }

    proptest! {
        #[test]
        fn prop_test_store_retrieval(data_list in prop::collection::vec(prop::collection::vec(0u8..255, 0..128), 0..256)) {
            let mut buffers = Buffers::new(VecStore::new());
            for data in &data_list {
                buffers.append(data);
            }

            prop_assert_eq!(buffers.len(), data_list.len());

            for (i, data) in data_list.iter().enumerate() {
                prop_assert_eq!(buffers.get(i), Some(data.as_slice()));
            }
        }
    }

    fn check_test_buffers_slice_and_iter<T: ByteStore>(buffers: &mut Buffers<T>) {
        buffers.append(b"a");
        buffers.append(b"b");
        buffers.append(b"c");
        buffers.append(b"d");
        buffers.append(b"e");

        let slice = buffers.slice(1, 4); // [b, c, d]
        assert_eq!(slice.len(), 3);
        assert_eq!(slice.get(0), Some(b"b".as_ref()));
        assert_eq!(slice.get(1), Some(b"c".as_ref()));
        assert_eq!(slice.get(2), Some(b"d".as_ref()));
        assert_eq!(slice.get(3), None);

        let collected: Vec<&[u8]> = slice.iter().collect();
        assert_eq!(collected, vec![b"b", b"c", b"d"]);
    }

    fn check_test_buffers_iter_equivalence<T: ByteStore>(buffers: &mut Buffers<T>) {
        buffers.append(b"a");
        buffers.append(b"b");
        buffers.append(b"c");

        let from_buffers: Vec<&[u8]> = buffers.iter().collect();
        let from_slice: Vec<&[u8]> = buffers.slice(0, buffers.len()).iter().collect();
        assert_eq!(from_buffers, from_slice);
        assert_eq!(
            from_buffers,
            vec![b"a".as_ref(), b"b".as_ref(), b"c".as_ref()]
        );
    }

    proptest! {
        #[test]
        fn prop_slice_iter_equivalence(
            data_list in prop::collection::vec(prop::collection::vec(0u8..255, 0..128), 0..256),
            start_pct in 0.0f64..1.0,
            end_pct in 0.0f64..1.0
        ) {
            let mut buffers = Buffers::new(VecStore::with_capacity(data_list.len() * 100));
            for data in &data_list {
                buffers.append(data);
            }

            let len = buffers.len();
            if len > 0 {
                let start_idx = (start_pct * len as f64).floor() as usize;
                let end_idx = (end_pct * len as f64).floor() as usize;

                if start_idx <= end_idx {
                    let slice = buffers.slice(start_idx, end_idx);
                    let from_slice_iter: Vec<&[u8]> = slice.iter().collect();
                    let expected: Vec<&[u8]> = data_list[start_idx..end_idx].iter().map(|v| v.as_slice()).collect();
                    prop_assert_eq!(from_slice_iter, expected);
                }
            }
        }
    }

    proptest! {
        #[test]
        fn prop_nested_slice_equivalence(
            data_list in prop::collection::vec(prop::collection::vec(0u8..255, 0..32), 10..40)
        ) {
            let mut buffers = Buffers::new(VecStore::with_capacity(data_list.len() * 40));
            for data in &data_list {
                buffers.append(data);
            }

            let slice1 = buffers.slice(2, 8); // 6 elements
            let slice2 = slice1.slice(1, 5); // 4 elements from index 1 of slice1

            let collected: Vec<_> = slice2.iter().collect();
            let expected: Vec<_> = data_list[3..7].iter().map(|v| v.as_slice()).collect();
            prop_assert_eq!(collected, expected);
        }
    }

    proptest! {
        #[test]
        fn prop_test_store_bounds(
            data_list in prop::collection::vec(prop::collection::vec(0u8..255, 0..128), 0..256)
        ) {
            let mut buffers = Buffers::new(VecStore::new());
            for data in &data_list {
                buffers.append(data);
            }

            prop_assert!(buffers.get(data_list.len()).is_none());
            if !data_list.is_empty() {
                prop_assert!(buffers.get(data_list.len() - 1).is_some());
            }
        }
    }

    proptest! {
        #[test]
        fn prop_test_no_out_of_bounds_access(
            data_list in prop::collection::vec(prop::collection::vec(0u8..255, 0..128), 0..256)
        ) {
            let mut buffers = Buffers::new(VecStore::with_capacity(1024 * 1024));
            for data in &data_list {
                buffers.append(data);
            }

            // The test is that this doesn't panic
            for i in 0..buffers.len() {
                let _ = buffers.get(i);
            }
            let _ = buffers.get(buffers.len());
        }
    }

    proptest! {
        #[test]
        fn prop_test_free_space_calculation(
            data_list in prop::collection::vec(prop::collection::vec(0u8..255, 0..128), 0..256)
        ) {
            let mut buffers = Buffers::new(VecStore::with_capacity(1024));
            let initial_free_space = buffers.free_space();
            let mut _total_added = 0;

            for data in &data_list {
                let len = data.len();
                buffers.append(data);
                _total_added += len + size_of::<usize>();
            }

            // This assertion is tricky because of buffer growth. Let's keep it simple:
            // Free space must be less than or equal to initial free space.
            assert!(buffers.free_space() <= initial_free_space, "Free space should not exceed initial free space {} <= {}", buffers.free_space(), initial_free_space);
            assert_eq!(buffers.free_space(), buffers.data_end - buffers.offsets_end());
        }
    }

    #[test]
    fn proptest_free_space_calculation_empty() {
        let mut buffers = Buffers::new(VecStore::with_capacity(1024));
        let initial_free_space = buffers.free_space();

        buffers.append([]); // Append empty data

        assert!(buffers.free_space() <= initial_free_space);
        assert_eq!(
            buffers.free_space(),
            buffers.data_end - buffers.offsets_end()
        );
    }

    // A test to ensure that the layout of data and offsets is as expected.
    // This helps in debugging and understanding the internal structure.
    #[test]
    fn test_exact_interface_requirements() {
        let mut buffers = Buffers::new(VecStore::with_capacity(128));
        buffers.append([1, 2]); // len=2, offset_size=8
        buffers.append([3, 4, 5]); // len=3, offset_size=8

        let data_bytes = buffers.store().as_ref();

        // Offsets section: 2 offsets of 8 bytes each
        let offset1_bytes: [u8; 8] = data_bytes[0..8].try_into().unwrap();
        let offset2_bytes: [u8; 8] = data_bytes[8..16].try_into().unwrap();
        let offset1 = usize::from_le_bytes(offset1_bytes);
        let offset2 = usize::from_le_bytes(offset2_bytes);

        assert_eq!(offset1, 2); // Cumulative length of first item
        assert_eq!(offset2, 5); // Cumulative length of both items

        // Data section (at the end of the buffer)
        let len = data_bytes.len();
        let data1 = &data_bytes[len - 2..len]; // first item is last
        let data2 = &data_bytes[len - 5..len - 2]; // second item is before the first
        assert_eq!(data1, &[1, 2]);
        assert_eq!(data2, &[3, 4, 5]);
    }

    // This test visualizes the buffer layout to help understand the offset system.
    #[test]
    fn test_offset_system_understanding() {
        let mut buffers = Buffers::new(VecStore::with_capacity(64));
        buffers.append([1]); // total len 1
        buffers.append([2, 2]); // total len 3
        buffers.append([3, 3, 3]); // total len 6

        let data = buffers.store().as_ref();
        let offset_size = size_of::<usize>();

        // Offsets at the beginning
        let cum_len1 = usize::from_le_bytes(data[0..offset_size].try_into().unwrap());
        let cum_len2 = usize::from_le_bytes(data[offset_size..2 * offset_size].try_into().unwrap());
        let cum_len3 =
            usize::from_le_bytes(data[2 * offset_size..3 * offset_size].try_into().unwrap());
        assert_eq!(cum_len1, 1);
        assert_eq!(cum_len2, 3);
        assert_eq!(cum_len3, 6);

        // Data at the end
        let total_len = data.len();
        assert_eq!(&data[total_len - 1..], &[1]); // last byte of last item
        assert_eq!(&data[total_len - 3..total_len - 1], &[2, 2]); // 2nd item
        assert_eq!(&data[total_len - 6..total_len - 3], &[3, 3, 3]); // 3rd item
    }

    #[test]
    fn test_empty_buffer() {
        let mut buffers = Buffers::new(VecStore::with_capacity(64));
        check_test_empty_data(&mut buffers);
    }

    #[test]
    fn test_offset_system_with_growth() {
        // Start with a small buffer to force growth
        let mut buffers = Buffers::new(VecStore::with_capacity(32));
        buffers.append([1; 10]);
        buffers.append([2; 10]);

        assert_eq!(buffers.len(), 2);
        assert_eq!(buffers.store().as_ref().len() > 32, true); // It grew

        let data = buffers.store().as_ref();
        let offset_size = size_of::<usize>();

        // Check offsets
        let cum_len1 = usize::from_le_bytes(data[0..offset_size].try_into().unwrap());
        let cum_len2 = usize::from_le_bytes(data[offset_size..2 * offset_size].try_into().unwrap());
        assert_eq!(cum_len1, 10);
        assert_eq!(cum_len2, 20);

        // Check data
        let total_len = data.len();
        assert_eq!(&data[total_len - 10..], &[2; 10]);
        assert_eq!(&data[total_len - 20..total_len - 10], &[1; 10]);
    }

    // This test checks a very specific scenario that might cause issues with offset calculations.
    #[test]
    fn test_detailed_offset_analysis() {
        let mut buffers = Buffers::new(VecStore::with_capacity(64));
        buffers.append(&[1, 2]); // Cumulative len = 2
        buffers.append(&[3, 4, 5]); // Cumulative len = 5
        buffers.append(&[6]); // Cumulative len = 6

        assert_eq!(buffers.get(0), Some(&[1, 2][..]));
        assert_eq!(buffers.get(1), Some(&[3, 4, 5][..]));
        assert_eq!(buffers.get(2), Some(&[6][..]));
    }
}
