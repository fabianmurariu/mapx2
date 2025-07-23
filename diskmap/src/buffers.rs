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
/// use diskmap::byte_store::VecStore;
///
/// let mut store = Buffers::new(VecStore::with_capacity(1024));
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
    byte_store: T,
    /// Number of slots (entries) currently stored
    count: usize,
    /// Current position where the next data will be written (grows backwards)
    data_end: usize,
}

#[derive(Debug)]
pub struct BuffersSlice<'a, T: ByteStore> {
    byte_store: &'a T,
    start: usize,
    end: usize,
}

impl<'a, T> Clone for BuffersSlice<'a, T>
where
    T: ByteStore,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T> Copy for BuffersSlice<'a, T> where T: ByteStore {}

impl<'a, T: ByteStore> BuffersSlice<'a, T> {
    /// Create a slice of this BuffersSlice from [start, end)
    pub fn slice(&self, start: usize, end: usize) -> BuffersSlice<'a, T> {
        assert!(start <= end, "start must be <= end");
        assert!(end <= self.len(), "end out of bounds");
        BuffersSlice {
            byte_store: self.byte_store,
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
        let buffer = self.byte_store.as_ref();
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
        let buffer = self.byte_store.as_ref();
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
            if new_len < required_len {
                new_len = required_len;
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

    /// Get the stored cumulative length offset for a given index
    fn get_cumulative_offset(&self, index: usize) -> Option<usize> {
        if index >= self.count {
            return None;
        }

        let buffer = self.byte_store.as_ref();
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

        let buffer = self.byte_store.as_ref();
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
        if self.data_end >= offsets_end {
            self.data_end - offsets_end
        } else {
            0
        }
    }

    /// Clear all entries from the store
    pub fn clear(&mut self) {
        self.count = 0;
        self.data_end = self.byte_store.as_ref().len();
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
    use crate::byte_store::{ByteStore, MMapFile, VecStore};
    use proptest::prelude::*;

    #[cfg(test)]
    macro_rules! test_buffers {
        ($name:ident, $size:expr, $body:expr) => {
            paste::item! {
                fn [<check_ $name>]<B: ByteStore>(store: Buffers<B>) {
                    $body(store);
                }

                #[test]
                fn [<$name _vec_backend>]() {
                    let size = $size;
                    let mut s = VecStore::new();
                    s.grow(size);
                    let buffers = Buffers::new(s);
                    [<check_ $name>](buffers);
                }

                #[test]
                fn [<$name _mmap_backend>]() {
                    use tempfile::NamedTempFile;
                    let size = $size;
                    let tmp = NamedTempFile::new().unwrap();
                    let mmap_file = MMapFile::new(tmp.path(), size).unwrap();
                    let buffers = Buffers::new(mmap_file);
                    [<check_ $name>](buffers);
                }
            }
        };
    }

    test_buffers!(
        test_basic_operations,
        1024,
        (|mut store: Buffers<B>| {
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
        })
    );

    test_buffers!(
        test_empty_data,
        1024,
        (|mut store: Buffers<B>| {
            let idx = store.append(b"");
            assert_eq!(idx, 0);
            assert_eq!(store.get(0).unwrap(), b"");
        })
    );

    test_buffers!(
        test_auto_growing,
        32,
        (|mut store: Buffers<B>| {
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
        })
    );

    test_buffers!(
        test_clear,
        1024,
        (|mut store: Buffers<B>| {
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
        })
    );

    // // test_different_backing_types is not rewritten with the macro since it tests array and boxed slice specifically.

    proptest! {
        #[test]
        fn prop_test_store_retrieval(
            data_list in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 0..100),
                0..50
            )
        ) {
            let mut s = VecStore::new();
            s.grow(8192);
            let mut store = Buffers::new(s);
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

    test_buffers!(
        test_buffers_slice_and_iter,
        1024,
        (|mut store: Buffers<B>| {
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
            let collected: Vec<_> = sub.iter().collect();
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
        })
    );

    test_buffers!(
        test_buffers_iter_equivalence,
        512,
        (|mut store: Buffers<B>| {
            for i in 0..10 {
                let s = format!("entry{i}");
                store.append(s.as_bytes());
            }
            let manual: Vec<_> = (0..store.len()).map(|i| store.get(i).unwrap()).collect();
            let iter: Vec<_> = store.iter().collect();
            assert_eq!(manual, iter);
        })
    );

    proptest! {
        #[test]
        fn prop_slice_iter_equivalence(
            data_list in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 0..20),
                1..30
            )
        ) {
            let mut s = VecStore::new();
            s.grow(4096);
            let mut store = Buffers::new(s);
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
            let mut s = VecStore::new();
            s.grow(2048);
            let mut store = Buffers::new(s);
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
            let mut s = VecStore::new();
            s.grow(8192);
            let mut store = Buffers::new(s);

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
            let mut s = VecStore::new();
            s.grow(8192);
            let mut store = Buffers::new(s);

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
            let mut s = VecStore::new();
            s.grow(buffer_size);
            let mut store = Buffers::new(s);
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
        let mut s = VecStore::new();
        s.grow(8192);
        let mut store = Buffers::new(s);

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
        let mut s = VecStore::new();
        s.grow(64);
        let mut store = Buffers::new(s);

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
        let mut s = VecStore::new();
        s.grow(32);
        let mut store = Buffers::new(s);

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
        let mut store = Buffers::new(VecStore::new());

        let data1 = b"AAAA";
        let idx1 = store.append(data1);
        assert_eq!(store.get(idx1).unwrap(), data1);

        let data2 = b"BBBB";
        let idx2 = store.append(data2);
        assert_eq!(store.get(idx1).unwrap(), data1);
        assert_eq!(store.get(idx2).unwrap(), data2);

        let large_data = vec![b'X'; 200];
        let idx3 = store.append(&large_data);
        assert_eq!(store.get(idx1).unwrap(), data1);
        assert_eq!(store.get(idx2).unwrap(), data2);
        assert_eq!(store.get(idx3).unwrap(), large_data.as_slice());

        let offset1 = store.get_cumulative_offset(idx1).unwrap();
        let offset2 = store.get_cumulative_offset(idx2).unwrap();
        let offset3 = store.get_cumulative_offset(idx3).unwrap();

        assert_eq!(offset1, data1.len());
        assert_eq!(offset2, data1.len() + data2.len());
        assert_eq!(offset3, data1.len() + data2.len() + large_data.len());
    }
}
