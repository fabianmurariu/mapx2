# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rust workspace containing two disk-backed hash map implementations:

1. **diskhashmap** - Single-threaded hash map with memory-mapped file backing
2. **diskdashmap** - Multi-threaded hash map with sharded locking

Both implementations use an open addressing scheme and can operate either in-memory (with `VecStore`) or persistently (with `MMapFile` backing).

## Common Commands

### Building and Testing

```bash
# Build all workspace members
cargo build

# Build with release optimizations
cargo build --release

# Run all tests
cargo test

# Run tests for specific package
cargo test -p diskhashmap
cargo test -p diskdashmap

# Run tests with output
cargo test -- --nocapture
```

### Benchmarking

```bash
# Run benchmarks in diskhashmap package
cargo bench -p diskhashmap

# Run specific benchmark
cargo bench -p diskhashmap --bench hash_map_comparison
cargo bench -p diskhashmap --bench u64_key_benchmark
```

### Examples

```bash
# Run the byte store demo
cargo run -p diskhashmap --example byte_store_demo
```

## Architecture

### Core Components

**ByteStore Trait** (`diskhashmap/src/byte_store.rs`)

- Abstraction for growable byte storage
- Implementations: `VecStore` (in-memory), `MMapFile` (disk-backed)
- Tracks resize events for performance monitoring

**Buffers** (`diskhashmap/src/buffers.rs`)

- Variable-length data storage built on `ByteStore`
- Manages allocation of byte slices with automatic growth
- Returns indices for accessing stored data

**OpenHashMap** (`diskhashmap/src/raw_map/mod.rs`)

- Main hash map implementation using open addressing with linear probing
- Generic over key/value types and storage backends
- Supports both in-memory and persistent storage via different `ByteStore` implementations
- Load factor threshold of 0.4 triggers resizing

**Entry System** (`diskhashmap/src/raw_map/entry.rs`)

- Compact entry representation using bitfields
- Tracks key/value positions and occupancy state
- Supports tombstone deletion markers

### Storage Architecture

The hash map uses three separate storage areas:

- **Entries**: Fixed-size array of entry metadata
- **Keys**: Variable-length key storage via `Buffers`
- **Values**: Variable-length value storage via `Buffers`

This separation allows efficient memory usage and supports different storage backends for each component.

### Persistence

Maps can be created in two ways:

- `new_in(path)`: Create new persistent map at given directory
- `load_from(path)`: Load existing map from directory

Files are automatically memory-mapped and persist changes immediately.

## Testing

The codebase uses both unit tests and property-based testing with `proptest`. Key test patterns:

- Comparison with `std::HashMap` for correctness validation
- Persistence testing with temporary directories
- Performance regression tests for resize behavior
- Property-based testing with random data

## Dependencies

Key external crates:

- `memmap2`: Memory-mapped file I/O
- `bytemuck`: Safe transmutation between types
- `modular-bitfield`: Compact bitfield representations
- `rustc-hash`: Fast hash function implementation
- `criterion`: Benchmarking framework
- `proptest`: Property-based testing
