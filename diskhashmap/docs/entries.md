# Double Vector Entries Mechanism for DiskHashMap

## Overview

This document outlines the design and implementation plan for a double vector entries mechanism in DiskHashMap, inspired by HashBrown's incremental resizing approach. The goal is to avoid large pauses during resize operations by distributing the rehashing work across multiple operations.

## Current Implementation Problems

The current single-array implementation has these issues:
- **Large resize pauses**: When capacity is exceeded, all entries must be rehashed at once
- **Blocking operations**: During resize, the entire map becomes unavailable
- **Memory spikes**: Temporary allocation of a full new entries array during resize
- **Poor performance**: Large maps experience significant latency spikes during resize

## Double Vector Solution

### Core Concept

Instead of rehashing all entries at once, maintain two entry arrays during resize:
1. **Old Array**: Contains entries that haven't been rehashed yet
2. **New Array**: Contains freshly rehashed entries and new insertions

During resize operations, a small number of entries are incrementally moved from the old array to the new array.

### Data Structure Design

```rust
pub struct DoubleArrayEntries<BS: ByteStore> {
    /// The old entries array (present during resize)
    old_entries: Option<FixedVec<Entry, BS>>,
    
    /// The new entries array (always present)
    new_entries: FixedVec<Entry, BS>,
    
    /// Index of the next entry to rehash in old_entries
    /// Once all entries are rehashed, old_entries is dropped
    rehash_progress: usize,
    
    /// Number of occupied entries across both arrays
    occupied_count: usize,
    
    /// Number of entries to rehash per operation
    rehash_batch_size: usize,
}
```

### State Machine

The double array entries can be in one of two states:

#### 1. Normal State
- `old_entries` is `None`
- All operations work on `new_entries`
- Behaves identically to single array

#### 2. Resizing State
- `old_entries` contains the previous entries
- `new_entries` contains the larger capacity array
- `rehash_progress` tracks how many old entries have been moved
- All mutating operations trigger incremental rehashing

### Key Algorithms

#### Lookup Algorithm
```rust
fn lookup(&self, hash: u64, key: &[u8]) -> Option<usize> {
    // First check new array
    if let Some(index) = self.lookup_in_array(&self.new_entries, hash, key) {
        return Some(index);
    }
    
    // If resizing, check old array for unrelocated entries
    if let Some(ref old) = self.old_entries {
        let old_start_index = hash as usize % old.capacity();
        if old_start_index >= self.rehash_progress {
            return self.lookup_in_array(old, hash, key)
                .map(|i| i + self.new_entries.capacity()); // Offset for old array
        }
    }
    
    None
}
```

#### Insertion Algorithm
```rust
fn insert(&mut self, hash: u64, entry: Entry) -> Result<usize> {
    // Always trigger incremental rehashing on mutations
    self.incremental_rehash(self.rehash_batch_size);
    
    // Insert into new array
    let index = self.find_insertion_slot(&mut self.new_entries, hash)?;
    self.new_entries[index] = entry;
    self.occupied_count += 1;
    
    Ok(index)
}
```

#### Incremental Rehashing Algorithm
```rust
fn incremental_rehash(&mut self, max_entries: usize) -> usize {
    let old_entries = match &self.old_entries {
        Some(old) => old,
        None => return 0, // Not resizing
    };
    
    let mut rehashed = 0;
    let old_capacity = old_entries.capacity();
    
    while rehashed < max_entries && self.rehash_progress < old_capacity {
        let entry = old_entries[self.rehash_progress];
        
        if entry.is_occupied() {
            // Calculate hash from key in heap
            let key_bytes = self.get_key_bytes(entry.key_pos())?;
            let hash = self.hash_key(key_bytes);
            
            // Find slot in new array
            let new_index = self.find_insertion_slot(&mut self.new_entries, hash)?;
            self.new_entries[new_index] = entry;
            rehashed += 1;
        }
        
        self.rehash_progress += 1;
    }
    
    // Check if resize is complete
    if self.rehash_progress >= old_capacity {
        self.old_entries = None; // Drop old array
        self.rehash_progress = 0;
    }
    
    rehashed
}
```

### Performance Characteristics

#### Memory Usage
- **Normal State**: Same as single array (`capacity * sizeof(Entry)`)
- **Resizing State**: Up to 2x memory usage during resize
- **Improvement**: Gradual memory release vs. sudden allocation spike

#### Time Complexity
- **Lookup**: O(1) average, O(2) during resize (check both arrays)
- **Insert**: O(1) + O(batch_size) for incremental rehashing
- **Delete**: O(1) + O(batch_size) for incremental rehashing
- **Resize**: Amortized across many operations instead of one large pause

#### Tuning Parameters
- **Batch Size**: Number of entries to rehash per operation
  - Small (1-4): Lower per-operation latency, longer resize duration
  - Large (16-32): Higher per-operation latency, shorter resize duration
  - Recommended: 4-8 entries per operation

### Integration with DiskHashMap

#### Changes Required

1. **Replace entries field**:
   ```rust
   // Before
   entries: FixedVec<Entry, BS>,
   
   // After
   entries: Box<dyn EntriesStorage<BS>>,
   ```

2. **Update all entry access**:
   ```rust
   // Before
   let entry = &self.entries[index];
   
   // After
   let entry = self.entries.get_entry(index);
   ```

3. **Trigger incremental rehashing**:
   ```rust
   // In insert/remove/update operations
   self.entries.incremental_rehash(self.rehash_batch_size);
   ```

#### Configuration Options
```rust
pub struct ResizeConfig {
    /// Load factor threshold to trigger resize (default: 0.75)
    pub load_factor_threshold: f64,
    
    /// Number of entries to rehash per operation (default: 8)
    pub rehash_batch_size: usize,
    
    /// Growth factor for new capacity (default: 2.0)
    pub growth_factor: f64,
}
```

### Persistence Considerations

#### Disk Storage Layout
- **Single Array**: Simple continuous layout
- **Double Array**: Two separate regions during resize
  ```
  [Metadata][New Entries][Old Entries (during resize)]
  ```

#### Recovery from Partial Resize
- Store resize progress in metadata
- On restart, continue incremental rehashing from saved progress
- Ensure crash consistency by updating progress atomically

### Testing Strategy

#### Unit Tests
1. **State Transitions**: Normal ↔ Resizing states
2. **Incremental Progress**: Verify rehashing advances correctly
3. **Data Integrity**: All entries remain accessible during resize
4. **Memory Management**: Old array properly dropped when complete

#### Integration Tests
1. **Concurrent Operations**: Mix of reads/writes during resize
2. **Large Dataset**: Test with maps exceeding single batch rehash
3. **Persistence**: Crash recovery during resize state
4. **Performance**: Measure latency distribution vs single array

#### Benchmarks
1. **Latency Percentiles**: Compare P99 latency during resize
2. **Throughput**: Operations/second during resize vs steady state
3. **Memory Usage**: Peak memory consumption patterns
4. **Recovery Time**: Time to complete interrupted resize

### Migration Path

#### Phase 1: Trait Abstraction (Current)
- ✅ Create `EntriesStorage` trait
- ✅ Implement `SingleArrayEntries` wrapper
- Update DiskHashMap to use trait

#### Phase 2: Double Array Implementation
- Implement `DoubleArrayEntries`
- Add configuration for choosing implementation
- Comprehensive testing suite

#### Phase 3: Performance Optimization
- Tune batch sizes based on benchmarks
- Optimize lookup paths for hot code
- Memory layout optimizations

#### Phase 4: Production Readiness
- Crash recovery testing
- Long-running stability tests
- Documentation and examples

### Expected Benefits

1. **Reduced Latency Spikes**: 90%+ reduction in P99 latency during resize
2. **Better Responsiveness**: No blocking resize operations
3. **Predictable Performance**: Consistent operation times regardless of map size
4. **Improved Scalability**: Better behavior for large datasets
5. **Lower Memory Pressure**: Gradual allocation instead of spikes

### Potential Drawbacks

1. **Increased Complexity**: More complex lookup and mutation logic
2. **Memory Overhead**: Up to 2x memory usage during resize
3. **Code Maintenance**: Additional state management and edge cases
4. **Debugging Difficulty**: More complex internal state to understand

### Conclusion

The double vector entries mechanism provides a significant improvement in resize performance characteristics at the cost of implementation complexity. The trait-based abstraction allows for gradual migration and A/B testing between implementations.

The incremental rehashing approach aligns well with DiskHashMap's goals of predictable performance and scalability, making it particularly valuable for applications with large datasets or strict latency requirements.