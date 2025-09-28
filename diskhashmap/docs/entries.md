# Double Array Entries: Incremental Rehashing

This document explains the incremental rehashing system implemented in `entries.rs` to avoid latency spikes during hash map resize operations.

## Problem Statement

Traditional hash maps suffer from periodic latency spikes when they need to resize. During a resize operation, all existing entries must be rehashed and moved to their new positions in the larger array. For large hash maps, this can cause significant pauses that are unacceptable in latency-sensitive applications.

## Solution: Double Array Architecture

The `DoubleArrayEntries` structure maintains two entry arrays during resize operations:

- **old_entries**: Contains entries that haven't been rehashed yet
- **new_entries**: Contains rehashed entries and all new insertions

This allows the resize work to be distributed across multiple operations instead of happening all at once.

## Key Components

### SlotIdx

```rust
pub(crate) struct SlotIdx(i64);
```

A clever encoding that uses the sign bit to distinguish between old and new array indices:

- Positive values: indices into the new array
- Negative values: indices into the old array (encoded as `-(index + 1)`)

### EntriesState

```rust
pub(crate) struct EntriesState {
    pub reindex_offset: i64,     // Current position in old array for rehashing
    pub reindex_batch: u64,      // Number of entries to rehash per operation
    pub occupied_count: u64,     // Total occupied entries across both arrays
}
```

This state is stored externally by the disk map and tracks the progress of incremental rehashing.

## Operational Phases

### Phase 1: Normal Operation (Single Array)

- Only `new_entries` exists
- `old_entries` is `None`
- `reindex_offset` is -1 (indicates no rehashing in progress)
- All operations work directly on the single array

### Phase 2: Resize Initiation

When the load factor threshold is exceeded:

1. A new, larger array is allocated as `new_entries`
2. The old `new_entries` becomes `old_entries`
3. `reindex_offset` is set to 0 (start of old array)
4. New insertions go directly into `new_entries`

### Phase 3: Incremental Rehashing

During each `set_entry` operation:

1. The new entry is inserted into `new_entries`
2. If `reindex_offset >= 0`, rehash a batch of entries from `old_entries`
3. Move `reindex_batch` entries from the old array to the new array
4. Advance `reindex_offset` by the batch size
5. Mark moved entries to avoid double-processing

### Phase 4: Completion

When `reindex_offset` reaches the end of the old array:

1. Set `reindex_offset` back to -1
2. Drop the `old_entries` array
3. Return to normal single-array operation

## Find Operations During Rehashing

The `find_entry` method searches both arrays appropriately:

1. **New array**: Always search starting from the hash-computed index
2. **Old array**: Only search entries that haven't been rehashed yet (from `reindex_offset` onwards)

This ensures correctness - an entry is found in exactly one location.

## Linear Probing Implementation

The code uses open addressing with linear probing. The `insert_into_entries` function:

1. Tries the hash-computed slot first
2. If occupied, probes linearly through the array
3. Wraps around to the beginning if needed
4. Inserts at the first empty or deleted slot found

## Key Optimizations

### Batch Processing

- Rehashing is done in configurable batches (`reindex_batch = 4`)
- Amortizes the rehashing cost across multiple operations
- Prevents any single operation from taking too long

### Smart State Management

- State is stored externally and passed to operations
- Allows the hash map to be persistent while maintaining rehashing progress
- `occupied_count` tracks total entries across both arrays

### Efficient Search

- `find_entry` returns separate iterators for new and old arrays
- Callers can prioritize searching the new array first
- Avoids unnecessary work when entries are found quickly

## Performance Characteristics

### Space Complexity

- During rehashing: O(old_size + new_size) = O(2 \* new_size)
- Normal operation: O(size)

### Time Complexity

- Insert during rehashing: O(1 + batch_size) amortized
- Search during rehashing: O(probe_distance) in both arrays
- Normal operation: O(1) amortized

## Usage Pattern

This design is particularly beneficial for:

- **Real-time systems** where predictable latency is critical
- **Large hash maps** where traditional resize would cause long pauses
- **Persistent storage** where rehashing progress needs to survive restarts

The incremental approach trades slightly higher memory usage during resize for dramatically better latency characteristics, making it ideal for latency-sensitive applications.
