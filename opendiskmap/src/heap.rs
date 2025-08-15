use std::collections::HashMap;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};

use bytemuck::{Pod, Zeroable};
use modular_bitfield::prelude::*;

use crate::byte_store::{ByteStore, MMapFile, VecStore};

#[bitfield]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Index {
    pub category: B4,
    pub offset: B60,
}

impl From<Index> for u64 {
    fn from(index: Index) -> u64 {
        u64::from_le_bytes(index.into_bytes())
    }
}

impl From<u64> for Index {
    fn from(value: u64) -> Self {
        Index::from_bytes(value.to_le_bytes())
    }
}

const SLAB_SIZES: [usize; 12] = [
    64,     // 64 bytes
    128,    // 128 bytes
    256,    // 256 bytes
    512,    // 512 bytes
    1024,   // 1 KB
    2048,   // 2 KB
    4096,   // 4 KB
    8192,   // 8 KB
    12288,  // 12 KB
    16384,  // 16 KB
    20480,  // 20 KB
    24576,  // 24 KB
];
const METADATA_SIZE: usize = 16; // 2 * size_of::<u64>()

#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(C)]
struct SlabMetadata {
    element_size: u64,
    count: u64,
}

pub struct Slab<S: ByteStore> {
    store: S,
    element_size: usize,
    count: usize,
}

impl<S: ByteStore> Slab<S> {
    pub fn new(mut store: S, element_size: usize) -> Self {
        // Ensure we have space for metadata
        if store.as_ref().len() < METADATA_SIZE {
            store.grow(METADATA_SIZE);
        }
        
        // Write metadata to the beginning of the store
        let metadata = SlabMetadata {
            element_size: element_size as u64,
            count: 0,
        };
        
        let metadata_bytes = bytemuck::bytes_of(&metadata);
        store.as_mut()[..METADATA_SIZE].copy_from_slice(metadata_bytes);
        
        Self {
            store,
            element_size,
            count: 0,
        }
    }

    pub fn from_existing(store: S) -> io::Result<Self> {
        if store.as_ref().len() < METADATA_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Store too small to contain metadata",
            ));
        }
        
        let metadata_bytes = &store.as_ref()[..METADATA_SIZE];
        let metadata: SlabMetadata = *bytemuck::from_bytes(metadata_bytes);
        
        Ok(Self {
            store,
            element_size: metadata.element_size as usize,
            count: metadata.count as usize,
        })
    }

    fn resolve_pos(&self, i: usize) -> Range<usize> {
        let start = METADATA_SIZE + i * self.element_size;
        let end = start + self.element_size;
        start..end
    }

    fn update_metadata(&mut self) {
        let metadata = SlabMetadata {
            element_size: self.element_size as u64,
            count: self.count as u64,
        };
        
        let metadata_bytes = bytemuck::bytes_of(&metadata);
        self.store.as_mut()[..METADATA_SIZE].copy_from_slice(metadata_bytes);
    }

    pub fn append(&mut self, data: &[u8]) -> u64 {
        let pos_range = self.resolve_pos(self.count);
        let required_capacity = pos_range.end;
        
        if self.store.as_ref().len() < required_capacity {
            let additional = required_capacity - self.store.as_ref().len();
            self.store.grow(additional);
        }

        let slice = &mut self.store.as_mut()[pos_range];
        
        let copy_len = data.len().min(self.element_size);
        slice[..copy_len].copy_from_slice(&data[..copy_len]);
        
        if copy_len < self.element_size {
            slice[copy_len..].fill(0);
        }

        let result = self.count as u64;
        self.count += 1;
        self.update_metadata();
        result
    }

    pub fn get(&self, offset: u64) -> Option<&[u8]> {
        let offset = offset as usize;
        if offset >= self.count {
            return None;
        }

        let pos_range = self.resolve_pos(offset);
        Some(&self.store.as_ref()[pos_range])
    }

    pub fn len(&self) -> usize {
        self.count
    }
}

pub struct Heap<S: ByteStore> {
    slabs: HashMap<u8, Slab<S>>,
    base_path: PathBuf,
}

impl Heap<VecStore> {
    pub fn new_in_memory() -> Self {
        Self {
            slabs: HashMap::new(),
            base_path: PathBuf::new(),
        }
    }
}

impl Heap<MMapFile> {
    pub fn new<P: AsRef<Path>>(base_path: P) -> io::Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_path)?;

        Ok(Self {
            slabs: HashMap::new(),
            base_path,
        })
    }

    pub fn load_from<P: AsRef<Path>>(base_path: P) -> io::Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        let mut slabs = HashMap::new();

        if base_path.exists() && base_path.is_dir() {
            for entry in std::fs::read_dir(&base_path)? {
                let entry = entry?;
                let path = entry.path();
                
                if path.is_file() {
                    if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                        if let Some(slab_name) = file_name.strip_suffix(".bin") {
                            if let Some(slab_id_str) = slab_name.strip_prefix("slab") {
                                if let Ok(slab_id) = slab_id_str.parse::<u8>() {
                                    if (slab_id as usize) < SLAB_SIZES.len() {
                                        let mmap_file = MMapFile::from_file(&path)?;
                                        let slab = Slab::from_existing(mmap_file)?;
                                        slabs.insert(slab_id, slab);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(Self { slabs, base_path })
    }
}

impl<S: ByteStore> Heap<S>
where
    Self: HeapOps<S>,
{
    pub fn append(&mut self, data: &[u8]) -> Index {
        let category = self.find_size_category(data.len()) as u8;
        
        let slab = self.get_or_create_slab(category);
        let offset = slab.append(data);
        
        Index::new()
            .with_category(category as u8)
            .with_offset(offset)
    }

    pub fn get(&self, index: Index) -> Option<&[u8]> {
        let category = index.category();
        let offset = index.offset();
        
        self.slabs.get(&category)?.get(offset)
    }

    fn find_size_category(&self, size: usize) -> usize {
        match SLAB_SIZES.binary_search(&size) {
            Ok(index) => index,
            Err(index) => {
                if index >= SLAB_SIZES.len() {
                    SLAB_SIZES.len() - 1
                } else {
                    index
                }
            }
        }
    }
}

trait HeapOps<S: ByteStore> {
    fn get_or_create_slab(&mut self, category: u8) -> &mut Slab<S>;
}

impl HeapOps<VecStore> for Heap<VecStore> {
    fn get_or_create_slab(&mut self, category: u8) -> &mut Slab<VecStore> {
        if !self.slabs.contains_key(&category) {
            let element_size = SLAB_SIZES[category as usize];
            let store = VecStore::new();
            let slab = Slab::new(store, element_size);
            self.slabs.insert(category, slab);
        }
        
        self.slabs.get_mut(&category).unwrap()
    }
}

impl HeapOps<MMapFile> for Heap<MMapFile> {
    fn get_or_create_slab(&mut self, category: u8) -> &mut Slab<MMapFile> {
        if !self.slabs.contains_key(&category) {
            let element_size = SLAB_SIZES[category as usize];
            let slab_path = self.base_path.join(format!("slab{}.bin", category));
            
            let initial_size = METADATA_SIZE + element_size * 128;
            let store = MMapFile::new(&slab_path, initial_size)
                .unwrap_or_else(|e| panic!("Failed to create slab file {}: {}", slab_path.display(), e));
            
            let slab = Slab::new(store, element_size);
            self.slabs.insert(category, slab);
        }
        
        self.slabs.get_mut(&category).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_index_bitfield() {
        let index = Index::new()
            .with_category(2)
            .with_offset(0x123456789ABCDEF);
        
        assert_eq!(index.category(), 2);
        assert_eq!(index.offset(), 0x123456789ABCDEF);
        
        let as_u64: u64 = index.into();
        let from_u64: Index = as_u64.into();
        
        assert_eq!(index, from_u64);
    }

    #[test]
    fn test_slab_append_and_get() {
        let store = VecStore::new();
        let mut slab = Slab::new(store, 64);
        
        let data = b"hello";
        let offset = slab.append(data);
        
        assert_eq!(offset, 0);
        assert_eq!(slab.len(), 1);
        
        let retrieved = slab.get(offset).unwrap();
        assert_eq!(retrieved.len(), 64);
        assert_eq!(&retrieved[..5], data);
        assert_eq!(&retrieved[5..64], &[0u8; 59]);
    }

    #[test]
    fn test_heap_in_memory() {
        let mut heap = Heap::new_in_memory();
        
        let data1 = b"hi";                    // 2 bytes -> category 0 (64 bytes)
        let data2 = b"hello world!";          // 12 bytes -> category 0 (64 bytes)
        let data3 = b"medium";                // 6 bytes -> category 0 (64 bytes)
        
        let index1 = heap.append(data1);
        let index2 = heap.append(data2);
        let index3 = heap.append(data3);
        
        // All should be in category 0 since they're all <= 64 bytes
        assert_eq!(index1.category(), 0);
        assert_eq!(index2.category(), 0);
        assert_eq!(index3.category(), 0);
        
        let retrieved1 = heap.get(index1).unwrap();
        let retrieved2 = heap.get(index2).unwrap();
        let retrieved3 = heap.get(index3).unwrap();
        
        // All retrieved data should be 64 bytes (padded)
        assert_eq!(retrieved1.len(), 64);
        assert_eq!(retrieved2.len(), 64);
        assert_eq!(retrieved3.len(), 64);
        
        assert_eq!(&retrieved1[..2], data1);
        assert_eq!(&retrieved2[..12], data2);
        assert_eq!(&retrieved3[..6], data3);
    }

    #[test]
    fn test_heap_persistent() -> io::Result<()> {
        let temp_dir = TempDir::new()?;
        let heap_path = temp_dir.path().join("heap");
        
        let index1;
        let index2;
        
        {
            let mut heap = Heap::new(&heap_path)?;
            
            let data1 = b"persistent";
            let data2 = b"data";
            
            index1 = heap.append(data1);
            index2 = heap.append(data2);
        }
        
        {
            let heap = Heap::load_from(&heap_path)?;
            
            let retrieved1 = heap.get(index1).unwrap();
            let retrieved2 = heap.get(index2).unwrap();
            
            assert_eq!(&retrieved1[..10], b"persistent");
            assert_eq!(&retrieved2[..4], b"data");
        }
        
        Ok(())
    }

    #[test]
    fn test_find_size_category() {
        let heap = Heap::new_in_memory();
        
        // SLAB_SIZES = [64, 128, 256, 512, 1024, 2048, 4096, 8192, 12288, 16384, 20480, 24576]
        assert_eq!(heap.find_size_category(1), 0);     // -> 64 bytes
        assert_eq!(heap.find_size_category(64), 0);    // -> 64 bytes
        assert_eq!(heap.find_size_category(65), 1);    // -> 128 bytes
        assert_eq!(heap.find_size_category(128), 1);   // -> 128 bytes
        assert_eq!(heap.find_size_category(129), 2);   // -> 256 bytes
        assert_eq!(heap.find_size_category(256), 2);   // -> 256 bytes
        assert_eq!(heap.find_size_category(257), 3);   // -> 512 bytes
        assert_eq!(heap.find_size_category(1024), 4);  // -> 1024 bytes
        assert_eq!(heap.find_size_category(16384), 9); // -> 16384 bytes
        assert_eq!(heap.find_size_category(25000), 11); // -> 24576 bytes (largest)
    }

    #[test]
    fn test_slab_metadata_persistence() -> io::Result<()> {
        let temp_dir = TempDir::new()?;
        let slab_path = temp_dir.path().join("test_slab.bin");
        
        // Create a slab and add some data
        {
            let mmap_file = MMapFile::new(&slab_path, METADATA_SIZE + 64 * 10)?;
            let mut slab = Slab::new(mmap_file, 64);
            
            assert_eq!(slab.element_size, 64);
            assert_eq!(slab.len(), 0);
            
            slab.append(b"hello");
            slab.append(b"world");
            
            assert_eq!(slab.len(), 2);
        }
        
        // Load the slab and verify metadata and data
        {
            let mmap_file = MMapFile::from_file(&slab_path)?;
            let slab = Slab::from_existing(mmap_file)?;
            
            assert_eq!(slab.element_size, 64);
            assert_eq!(slab.len(), 2);
            
            let data1 = slab.get(0).unwrap();
            let data2 = slab.get(1).unwrap();
            
            assert_eq!(data1.len(), 64);
            assert_eq!(data2.len(), 64);
            assert_eq!(&data1[..5], b"hello");
            assert_eq!(&data2[..5], b"world");
        }
        
        Ok(())
    }
}