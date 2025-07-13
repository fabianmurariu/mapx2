use std::fs::File;

use memmap2::MmapMut;

pub(crate) trait ByteStore: AsRef<[u8]> + AsMut<[u8]> {
    // doubles the capacity of the store
    fn grow(&mut self);
}

pub(crate) struct MMapFile {
    mmap: MmapMut,
    file: File,
}
