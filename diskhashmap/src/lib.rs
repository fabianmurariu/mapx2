//! # DiskHashMap - Single-threaded Persistent Hash Map
//!
//! A high-performance, persistent hash map implementation with memory-mapped file backing.
//! This crate provides zero-copy deserialization support and is optimized for cache-friendly
//! access patterns using open addressing with linear probing.
//!
//! ## Features
//!
//! - **Persistent Storage**: Memory-mapped files for durability and fast startup
//! - **Zero-Copy**: 8-byte aligned storage compatible with rkyv and other zero-copy frameworks
//! - **Type Safety**: Strong typing with trait-based encoding/decoding
//! - **High Performance**: Open addressing with linear probing for cache-friendly access
//! - **Flexible Storage**: Support for both in-memory (`VecStore`) and persistent (`MMapFile`) backends
//!
//! ## Quick Start
//!
//! ```rust
//! use diskhashmap::{DiskHashMap, Native, Str, MMapFile, Result};
//! use rustc_hash::FxBuildHasher;
//! use tempfile::tempdir;
//!
//! # fn main() -> Result<()> {
//! let dir = tempdir()?;
//!
//! // Create a persistent map with u64 keys and String values
//! let mut map: DiskHashMap<Native<u64>, Str, MMapFile, FxBuildHasher> =
//!     DiskHashMap::new_in(dir.path())?;
//!
//! // Insert and retrieve data
//! map.insert(&42, "Hello, World!")?;
//! assert_eq!(map.get(&42)?, Some("Hello, World!"));
//!
//! // Data persists automatically across program runs
//! drop(map);
//! let map: DiskHashMap<Native<u64>, Str, MMapFile, FxBuildHasher> =
//!     DiskHashMap::load_from(dir.path())?;
//! assert_eq!(map.get(&42)?, Some("Hello, World!"));
//! # Ok(())
//! # }
//! ```

pub mod byte_store;
pub mod disk_map;
pub mod entry;
pub mod error;
mod fixed_buffers;
pub mod heap;
pub mod types;
pub use byte_store::{ByteStore, MMapFile, VecStore};
pub use disk_map::{
    DiskHashMap, MapEntry, OccupiedEntry, StringStringMap, StringU64Map, U64StringMap, VacantEntry,
};
pub use error::{DiskMapError, Result};
pub use heap::{Heap, HeapIdx, Slab};
pub use types::{Bytes, BytesDecode, BytesEncode, Native, Str};
