use std::fs::File;
use std::io;

use memmap2::MmapMut;

pub trait ByteStore: AsRef<[u8]> + AsMut<[u8]> {
    // grows the current store by `additional` bytes
    fn grow(&mut self, additional: usize);

    // creates a new store grows it by `additional` bytes
    // the new store should be empty
    // and have enough capacity to hold the current size + additional
    fn grow_new_empty(&self, additional: usize) -> Self;

    // returns the number of resize events
    fn stats(&self) -> u64;
}

#[derive(Debug, Clone, Default)]
pub struct VecStore {
    vec: Vec<u8>,
    resizes: u64,
}

impl VecStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            vec: Vec::with_capacity(capacity),
            resizes: 0,
        }
    }
}

impl AsRef<[u8]> for VecStore {
    fn as_ref(&self) -> &[u8] {
        &self.vec
    }
}

impl AsMut<[u8]> for VecStore {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.vec
    }
}

impl ByteStore for VecStore {
    fn grow(&mut self, additional: usize) {
        self.resizes += 1;
        self.vec.resize(self.vec.len() + additional, 0);
    }

    fn grow_new_empty(&self, additional: usize) -> Self {
        let len = (self.vec.len() + additional).next_power_of_two();
        Self {
            vec: vec![0u8; len],
            resizes: 0,
        }
    }

    fn stats(&self) -> u64 {
        self.resizes
    }
}

impl<const N: usize> ByteStore for [u8; N] {
    fn grow(&mut self, _additional: usize) {
        // Arrays have fixed size and cannot grow
        // This is a no-op for arrays
        panic!("Cannot grow fixed size arrays")
    }

    fn grow_new_empty(&self, _additional: usize) -> Self {
        panic!("can't add additional to fixed size N")
    }

    fn stats(&self) -> u64 {
        0
    }
}

impl ByteStore for Box<[u8]> {
    fn grow(&mut self, additional: usize) {
        let old_len = self.len();
        let new_len = (old_len + additional).next_power_of_two();

        let mut new_vec = vec![0u8; new_len];
        new_vec[..old_len].copy_from_slice(self);
        *self = new_vec.into_boxed_slice();
    }

    fn grow_new_empty(&self, additional: usize) -> Self {
        let old_len = self.len();
        let new_len = (old_len + additional).next_power_of_two();

        vec![0u8; new_len].into_boxed_slice()
    }

    fn stats(&self) -> u64 {
        0 // Not tracked for Box<[u8]> directly
    }
}

pub struct MMapFile {
    mmap: MmapMut,
    file: File,
    path: std::path::PathBuf,
    idx: usize,
    resizes: u64,
}

impl MMapFile {
    pub fn new<P: AsRef<std::path::Path>>(path: P, length_bytes: usize) -> io::Result<Self> {
        Self::new_inner(path, length_bytes, 0)
    }

    fn new_inner<P: AsRef<std::path::Path>>(
        path: P,
        length_bytes: usize,
        idx: usize,
    ) -> io::Result<Self> {
        use std::fs::OpenOptions;

        // Round length_kb to nearest power of 2 (in bytes)
        let mut size = length_bytes.max(1);
        size = size.next_power_of_two();

        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let path = path.to_path_buf();

        file.set_len(size as u64)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };
        // mmap.advise(memmap2::Advice::Sequential)
        // .expect("Failed to advise mmap");
        Ok(Self {
            mmap,
            file,
            idx,
            path,
            resizes: 0,
        })
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
    fn grow(&mut self, additional: usize) {
        self.resizes += 1;
        // Flush and fsync current changes
        self.mmap
            .flush()
            .unwrap_or_else(|_| panic!("Unrecoverable error flushing mmap"));

        let current_size = self.mmap.len();
        let new_size = (current_size + additional).next_power_of_two();

        // Drop Mmap to make sure we can resize the file
        // then resize the file
        // then remap it
        // do NOT make a new file

        let mut old_mmap = MmapMut::map_anon(1)
            .unwrap_or_else(|_| panic!("Unrecoverable error creating anonymous mmap"));

        std::mem::swap(&mut self.mmap, &mut old_mmap);
        drop(old_mmap);
        self.file.set_len(new_size as u64).unwrap_or_else(|_| {
            panic!("Unrecoverable error resizing file to {new_size} bytes");
        });
        self.mmap = unsafe {
            MmapMut::map_mut(&self.file).unwrap_or_else(|_| {
                panic!("Unrecoverable error remapping file to {new_size} bytes");
            })
        };
    }

    // Make a new file in the same parent_location with the additional size
    fn grow_new_empty(&self, additional: usize) -> Self {
        let current_size = self.mmap.len();
        let new_size = (current_size + additional).next_power_of_two();

        let parent_path = self
            .path
            .parent()
            .expect("Failed to get parent path of mmap file");

        let path_name = self
            .path
            .file_name()
            .expect("Failed to get file name from mmap file path")
            .to_string_lossy()
            .to_string();

        let file_name = parent_path.join(format!("{}_{}.bin", path_name, self.idx + 1));
        // Create a new file with the new size
        let mut new_file =
            MMapFile::new_inner(file_name, new_size, self.idx + 1).unwrap_or_else(|_| {
                panic!("Unrecoverable error creating new mmap file with {new_size} bytes");
            });

        new_file.resizes = self.resizes + 1;

        new_file
    }

    fn stats(&self) -> u64 {
        self.resizes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use tempfile::NamedTempFile;

    #[test]
    fn test_mmapfile_create_and_write() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut mmapfile = MMapFile::new(path, 1024).unwrap(); // 1 KB, rounded to 1024

        assert_eq!(mmapfile.as_ref().len(), 1024);

        // Write some data
        mmapfile.as_mut()[0..4].copy_from_slice(b"test");
        mmapfile.mmap.flush().unwrap();

        // Reopen and check data
        drop(mmapfile);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };
        assert_eq!(&mmap[0..4], b"test");
    }

    #[test]
    fn test_mmapfile_grow_and_persist() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mut mmapfile = MMapFile::new(path, 1024).unwrap(); // 1 KB, rounded to 1024
        assert_eq!(mmapfile.as_ref().len(), 1024);

        // Write some data at the end
        let len = mmapfile.as_ref().len();
        mmapfile.as_mut()[len - 4..len].copy_from_slice(b"grow");
        mmapfile.mmap.flush().unwrap();

        // Grow the file
        mmapfile.grow(1024);
        assert_eq!(mmapfile.as_ref().len(), 2048);

        // Data should still be at the new end
        assert_eq!(&mmapfile.as_ref()[len - 4..len], b"grow");

        // Write more data after grow
        mmapfile.as_mut()[0..6].copy_from_slice(b"hello!");
        mmapfile.mmap.flush().unwrap();

        // Reopen and check both data regions
        drop(mmapfile);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let mmap = unsafe { memmap2::Mmap::map(&file).unwrap() };
        assert_eq!(&mmap[len - 4..len], b"grow");
        assert_eq!(&mmap[0..6], b"hello!");
    }

    #[test]
    fn test_mmapfile_rounds_to_power_of_2() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path();

        let mmapfile = MMapFile::new(path, 3 * 1024).unwrap(); // 3 KB, should round to 4096
        assert_eq!(mmapfile.as_ref().len(), 4096);
    }
}
