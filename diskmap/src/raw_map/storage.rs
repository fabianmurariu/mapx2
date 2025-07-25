use crate::byte_store::MMapFile;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Manages the set of memory-mapped files for an OpenHashMap.
pub struct MapStorage {
    pub entries: MMapFile,
    pub keys: MMapFile,
    pub values: MMapFile,
}

impl MapStorage {
    /// Creates a new set of files for the map in the specified directory.
    ///
    /// This will create the directory if it doesn't exist.
    /// It will then create `entries_0.bin`, `keys.bin`, and `values.bin` with the given initial capacities.
    pub fn new_in(
        path: &Path,
        entries_cap: usize,
        keys_cap: usize,
        values_cap: usize,
    ) -> io::Result<Self> {
        fs::create_dir_all(path)?;

        let entries_path = path.join("entries_0.bin");
        let keys_path = path.join("keys.bin");
        let values_path = path.join("values.bin");

        let entries = MMapFile::new(&entries_path, entries_cap)?;
        let keys = MMapFile::new(&keys_path, keys_cap)?;
        let values = MMapFile::new(&values_path, values_cap)?;

        Ok(Self {
            entries,
            keys,
            values,
        })
    }

    /// Loads an existing set of map files from a directory.
    ///
    /// This will look for the latest `entries_N.bin` file to determine the current state of the map.
    pub fn load_from(path: &Path) -> io::Result<Self> {
        if !path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "map directory does not exist",
            ));
        }

        let (entries_path, _idx) = Self::find_latest_entries_file(path)?;

        let keys_path = path.join("keys.bin");
        let values_path = path.join("values.bin");

        let entries = MMapFile::from_file(&entries_path)?;
        let keys = MMapFile::from_file(&keys_path)?;
        let values = MMapFile::from_file(&values_path)?;

        Ok(Self {
            entries,
            keys,
            values,
        })
    }

    /// Finds the `entries_N.bin` file with the highest `N`.
    fn find_latest_entries_file(path: &Path) -> io::Result<(PathBuf, usize)> {
        let mut max_idx = None;
        let mut latest_path = None;

        if let Ok(dir) = fs::read_dir(path) {
            for entry in dir {
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                let fpath = entry.path();
                if fpath.is_file() {
                    if let Some(filename) = fpath.file_name().and_then(|s| s.to_str()) {
                        if filename.starts_with("entries_") && filename.ends_with(".bin") {
                            let num_part =
                                &filename["entries_".len()..filename.len() - ".bin".len()];
                            if let Ok(idx) = num_part.parse::<usize>() {
                                if max_idx.is_none() || idx > max_idx.unwrap() {
                                    max_idx = Some(idx);
                                    latest_path = Some(fpath.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        if let (Some(path), Some(idx)) = (latest_path, max_idx) {
            Ok((path, idx))
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "No entries file found in map directory",
            ))
        }
    }
}
