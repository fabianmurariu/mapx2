# MapX2 🗺️

[![CI](https://github.com/fabianmurariu/mapx2/workflows/CI/badge.svg)](https://github.com/fabianmurariu/mapx2/actions/workflows/ci.yml)
[![Security Audit](https://github.com/fabianmurariu/mapx2/workflows/Security%20Audit/badge.svg)](https://github.com/fabianmurariu/mapx2/actions/workflows/ci.yml)
[![CodeQL](https://github.com/fabianmurariu/mapx2/workflows/CodeQL/badge.svg)](https://github.com/fabianmurariu/mapx2/actions/workflows/codeql.yml)
[![Crates.io](https://img.shields.io/crates/v/diskmap.svg)](https://crates.io/crates/diskmap)
[![Documentation](https://docs.rs/diskmap/badge.svg)](https://docs.rs/diskmap)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/fabianmurariu/mapx2)
[![Rust Version](https://img.shields.io/badge/rust-1.70%2B-blue.svg)](https://github.com/fabianmurariu/mapx2)

High-performance, persistent hash maps for Rust with **zero-copy deserialization** support and **memory-mapped file backing**.

## ✨ Features

- 🚀 **High Performance**: Open addressing with linear probing for cache-friendly access
- 💾 **Persistent Storage**: Memory-mapped files for durability and fast startup
- 🔄 **Zero-Copy**: 8-byte aligned storage compatible with rkyv and other zero-copy frameworks
- 🎯 **Type Safety**: Strong typing with trait-based encoding/decoding
- 🧵 **Memory Safe**: Written in Rust with minimal unsafe code
- 📏 **Configurable**: Custom capacity allocation with power-of-2 sizing
- 🔧 **Flexible**: Support for any serializable key-value types

## 📦 Crates

| Crate                                        | Description                                  | Status            |
| -------------------------------------------- | -------------------------------------------- | ----------------- |
| [`diskmap`](./diskmap)                       | Single-threaded persistent hash map          | ✅ Stable         |
| [`concurrent_diskmap`](./concurrent_diskmap) | Multi-threaded hash map with sharded locking | 🚧 In Development |

## 🚀 Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
diskmap = "0.1"
rkyv = "0.7" # Optional: for zero-copy serialization
```

### Basic Usage with Native Types

```rust
use diskmap::{DiskHashMap, Native, Str};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;

    // Create a persistent map with u64 keys and String values
    let mut map: DiskHashMap<Native<u64>, Str, _, _> =
        DiskHashMap::new_in(dir.path())?;

    // Insert data
    map.insert(&42, "Hello, World!")?;
    map.insert(&100, "Rust is awesome!")?;

    // Retrieve data
    if let Some(value) = map.get(&42)? {
        println!("Found: {}", value); // "Found: Hello, World!"
    }

    println!("Map size: {}", map.len()); // "Map size: 2"

    // Data persists automatically!
    drop(map);

    // Load the same map from disk
    let map: DiskHashMap<Native<u64>, Str, _, _> =
        DiskHashMap::load_from(dir.path())?;

    assert_eq!(map.get(&42)?, Some("Hello, World!"));

    Ok(())
}
```

### Working with Raw Bytes

```rust
use diskmap::{DiskHashMap, Bytes};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;

    let mut map: DiskHashMap<Bytes, Bytes, _, _> =
        DiskHashMap::new_in(dir.path())?;

    // Store raw byte data
    map.insert(b"key1", b"value1")?;
    map.insert(b"key2", b"value2")?;

    // Retrieve as byte slices
    if let Some(value) = map.get(b"key1")? {
        println!("Raw value: {:?}", value); // [118, 97, 108, 117, 101, 49]
        println!("As string: {}", String::from_utf8_lossy(value));
    }

    Ok(())
}
```

### Zero-Copy with rkyv

```rust
use diskmap::{DiskHashMap, Native, types::Arch};
use rkyv::{Archive, Deserialize, Serialize};
use tempfile::tempdir;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
struct UserProfile {
    id: u32,
    name: String,
    email: String,
    scores: Vec<f64>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;

    // Create map with archived values (zero-copy)
    let mut map: DiskHashMap<Native<u64>, Arch<UserProfile>, _, _> =
        DiskHashMap::new_in(dir.path())?;

    let profile = UserProfile {
        id: 1,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        scores: vec![95.5, 87.2, 92.8],
    };

    // Insert user profile (automatically serialized with rkyv)
    map.insert(&1001, &profile)?;

    // Retrieve with zero-copy deserialization
    if let Some(archived_profile) = map.get(&1001)? {
        println!("User ID: {}", archived_profile.id);
        println!("User name: {}", archived_profile.name);
        println!("User email: {}", archived_profile.email);
        println!("First score: {}", archived_profile.scores[0]);

        // The archived_profile is a zero-copy view into memory-mapped data!
        // No deserialization overhead for accessing the data.
    }

    Ok(())
}
```

### Custom Capacity Configuration

```rust
use diskmap::{DiskHashMap, Bytes};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;

    // Create map with specific capacities (rounded to power of 2)
    let mut map: DiskHashMap<Bytes, Bytes, _, _> = DiskHashMap::with_capacity(
        dir.path(),
        1000,    // entries (-> 1024)
        64_000,  // key storage bytes (-> 65536)
        128_000, // value storage bytes (-> 131072)
    )?;

    println!("Capacity: {}", map.capacity()); // 1024

    // Pre-allocated space prevents early resizing
    for i in 0..500 {
        map.insert(format!("key_{}", i).as_bytes(),
                  format!("value_{}", i).as_bytes())?;
    }

    Ok(())
}
```

### Type Aliases for Common Use Cases

```rust
use diskmap::{U64StringMap, StringU64Map, StringStringMap};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;

    // u64 -> String mapping
    let mut id_to_name: U64StringMap<_> = U64StringMap::new_in(dir.path().join("names"))?;
    id_to_name.insert(&1, "Alice")?;
    id_to_name.insert(&2, "Bob")?;

    // String -> u64 mapping
    let mut name_to_id: StringU64Map<_> = StringU64Map::new_in(dir.path().join("ids"))?;
    name_to_id.insert("Alice", &1)?;
    name_to_id.insert("Bob", &2)?;

    // String -> String mapping
    let mut config: StringStringMap<_> = StringStringMap::new_in(dir.path().join("config"))?;
    config.insert("database_url", "postgresql://localhost/mydb")?;
    config.insert("log_level", "info")?;

    // All maps persist independently
    assert_eq!(id_to_name.get(&1)?, Some("Alice"));
    assert_eq!(name_to_id.get("Bob")?, Some(2));
    assert_eq!(config.get("log_level")?, Some("info"));

    Ok(())
}
```

### Entry API for Efficient Updates

```rust
use diskmap::{DiskHashMap, Native, MapEntry};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let mut map: DiskHashMap<Native<u64>, Native<u64>, _, _> =
        DiskHashMap::new_in(dir.path())?;

    // Efficient insert-or-update pattern
    match map.entry(&42)? {
        MapEntry::Vacant(entry) => {
            entry.insert(&100)?; // Insert new value
        }
        MapEntry::Occupied(entry) => {
            let old_value = entry.insert(&200)?; // Update existing
            println!("Updated value from {} to 200", old_value);
        }
    }

    // Or use convenience methods
    match map.entry(&43)? {
        MapEntry::Vacant(entry) => {
            entry.or_insert(&150)?;
        }
        MapEntry::Occupied(entry) => {
            entry.or_insert_with(|| &250)?;
        }
    }

    Ok(())
}
```

## 🏗️ Architecture

### Storage Layout

MapX2 uses a three-file storage system for optimal performance:

```
my_map/
├── entries_0.bin    # Hash table entries (metadata)
├── keys.bin         # Variable-length key storage
└── values.bin       # Variable-length value storage
```

### Memory Layout

Each data entry in keys.bin and values.bin follows this 8-byte aligned format for zero-copy compatibility:

```
[8-byte length prefix][actual data][zero padding to 8-byte boundary]
```

This ensures that zero-copy frameworks like rkyv can directly access the data without additional alignment.

### Hash Table Design

- **Open addressing** with linear probing for cache efficiency
- **Load factor**: Resizes at 40% to maintain performance
- **Power-of-2 sizing**: Capacity is always a power of 2 for fast modulo operations
- **Tombstone deletion**: Deleted entries are marked, not removed

## 🔧 Type System

MapX2 uses a trait-based type system for flexible serialization:

| Type Wrapper | Purpose                    | Example                      |
| ------------ | -------------------------- | ---------------------------- |
| `Native<T>`  | Copy types (numbers, etc.) | `Native<u64>`, `Native<f32>` |
| `Str`        | String types               | String literals and `String` |
| `Bytes`      | Raw byte arrays            | `&[u8]`, `Vec<u8>`           |
| `Arch<T>`    | rkyv archived types        | Zero-copy structs            |

## 📊 Performance

### Benchmarks

```bash
cargo bench
```

Typical performance characteristics:

- **Insert**: ~500-800 ns per operation
- **Lookup**: ~100-300 ns per operation
- **Memory overhead**: ~8 bytes per entry + key/value sizes
- **Load factor**: Maintains 40% for optimal performance

### Memory Usage

- Fixed overhead: ~24 bytes per hash table entry
- Variable data: Exact key + value sizes + 8-byte length prefixes
- Alignment padding: Up to 7 bytes per entry for zero-copy compatibility

## 🛡️ Safety

- **Memory safety**: Rust's ownership system prevents memory errors
- **Crash safety**: Memory-mapped files provide durability
- **Data integrity**: Checksums and validation prevent corruption
- **Thread safety**: `DiskHashMap` is `Send` but not `Sync` (single-threaded)

## 🧪 Testing

Run the comprehensive test suite:

```bash
# Run all tests
cargo test --all-features

# Run with coverage
cargo install cargo-llvm-cov
cargo llvm-cov --all-features --workspace --html

# Property-based testing
PROPTEST_CASES=10000 cargo test

# Memory safety testing
cargo +nightly miri test
```

## 📚 Examples

Explore more examples in the [`examples/`](./diskmap/examples/) directory:

- [`with_capacity_demo.rs`](./diskmap/examples/with_capacity_demo.rs) - Custom capacity allocation
- [`byte_store_demo.rs`](./diskmap/examples/byte_store_demo.rs) - Low-level storage operations

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](.github/CONTRIBUTING.md) for details.

### Development Setup

```bash
git clone https://github.com/fabianmurariu/mapx2.git
cd mapx2

# Run tests
cargo test --all-features

# Check formatting and lints
cargo fmt --all
cargo clippy --all-targets --all-features

# Run benchmarks
cargo bench
```

## 📄 License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## 🏷️ Version History

- **v0.1.0**: Initial release with `DiskHashMap` and zero-copy support
- **Future**: `ConcurrentDiskHashMap` with multi-threading support

---

_MapX2 is designed for applications requiring high-performance, persistent key-value storage with zero-copy access patterns. Perfect for databases, caches, search engines, and any system where fast, durable data access is critical._
