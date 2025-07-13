use std::fs::{File, OpenOptions};
use std::io;

use memmap2::MmapMut;

pub trait ByteStore: AsRef<[u8]> + AsMut<[u8]> {
    // doubles the capacity of the store
    fn grow(&mut self);
}

impl ByteStore for Vec<u8> {
    fn grow(&mut self) {
        let new_capacity = if self.capacity() == 0 {
            1
        } else {
            self.capacity() * 2
        };
        self.reserve(new_capacity - self.len());
        self.resize(new_capacity, 0);
    }
}

impl<const N: usize> ByteStore for [u8; N] {
    fn grow(&mut self) {
        // Arrays have fixed size and cannot grow
        // This is a no-op for arrays
    }
}

impl ByteStore for Box<[u8]> {
    fn grow(&mut self) {
        let old_len = self.len();
        let new_len = if old_len == 0 { 1 } else { old_len * 2 };

        let mut new_vec = vec![0u8; new_len];
        new_vec[..old_len].copy_from_slice(self);
        *self = new_vec.into_boxed_slice();
    }
}

pub struct MMapFile {
    mmap: MmapMut,
    file: File,
}

impl MMapFile {
    pub fn new(file: File) -> io::Result<Self> {
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self { mmap, file })
    }
}

impl AsRef<[u8]> for MMapFile {
    fn as_ref(&self) -> &[u8] {
        &self.mmap
    }
}

impl AsMut<[u8]> for MMapFile {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.mmap
    }
}

impl ByteStore for MMapFile {
    fn grow(&mut self) {
        // Flush and fsync current changes
        if let Err(_) = self.mmap.flush() {
            return; // Handle error gracefully
        }
        if let Err(_) = self.file.sync_all() {
            return; // Handle error gracefully
        }

        let current_size = self.mmap.len();
        let new_size = current_size * 2;

        // Create a temporary file name
        let temp_path = format!("/tmp/mmap_grow_{}", std::process::id());

        // Create new file with double capacity
        if let Ok(new_file) = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)
        {
            // Set the size of the new file
            if new_file.set_len(new_size as u64).is_ok() {
                // Create new mmap
                if let Ok(mut new_mmap) = unsafe { MmapMut::map_mut(&new_file) } {
                    // Copy existing data
                    new_mmap[..current_size].copy_from_slice(&self.mmap[..]);

                    // Zero out the new space
                    new_mmap[current_size..].fill(0);

                    // Replace the old file and mmap
                    self.mmap = new_mmap;
                    self.file = new_file;
                }
            }
        }
    }
}
