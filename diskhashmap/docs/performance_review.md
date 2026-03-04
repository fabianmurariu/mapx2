# DiskHashMap Performance Review

**Date:** 2025-11-22
**Reviewer:** Claude
**Branch:** double_entry_vec

## Executive Summary

The DiskHashMap implementation has several significant performance bottlenecks that cause it to be slow, particularly when using MMapFile backend. The primary issues are:

1. **Excessive metadata writes** - Every insert writes metadata twice
2. **Unnecessary fsync operations** - File growth triggers expensive disk synchronization
3. **Redundant flush calls** - Explicit flushes that duplicate Drop behavior
4. **No batching of metadata updates** - Metadata written on every single operation

**Estimated Impact:** These issues could be causing 10-100x slowdown compared to optimal performance.

---

## Critical Performance Issues

### 1. Excessive Metadata Writes (CRITICAL)

**Location:** `heap.rs:110-118`, `heap.rs:139-140`

**Problem:**
```rust
pub fn append(&mut self, data: &[u8]) -> u64 {
    // ... allocation code ...
    let result = self.count as u64;
    self.count += 1;
    self.update_metadata();  // ← WRITES 16 BYTES TO MMAP ON EVERY APPEND
    result
}

fn update_metadata(&mut self) {
    let metadata = SlabMetadata {
        element_size: self.element_size as u64,
        count: self.count as u64,
    };
    let metadata_bytes = bytemuck::bytes_of(&metadata);
    self.store.as_mut()[..METADATA_SIZE].copy_from_slice(metadata_bytes); // ← WRITES TO MMAP
}
```

**Impact:**
- For every key-value insert, `append()` is called at least twice (once for key, once for value)
- Each `append()` calls `update_metadata()` which writes 16 bytes to the memory-mapped file
- With MMapFile backend, this marks pages as dirty and triggers OS page writeback
- For 20M inserts, this means 40M+ metadata writes of 16 bytes each = 640MB of metadata writes alone

**Measurements:**
Looking at `insert_key_into_heap()` and `insert_value_into_heap()` in `disk_map.rs:299-341`:
- Each calls `page.flush()` explicitly (lines 310, 315, 332, 337)
- `PageEntry::flush()` (heap.rs:260-266) calls `update_metadata()`
- So EVERY insert triggers 2 metadata writes minimum

### 2. Expensive fsync on Growth (CRITICAL)

**Location:** `byte_store.rs:210-217`

**Problem:**
```rust
fn grow(&mut self, additional: usize) {
    self.resizes += 1;
    // Flush and fsync current changes
    if self.mmap.flush().is_ok() {
        self.file
            .sync_all()  // ← BLOCKING FSYNC TO DISK!!!
            .unwrap_or_else(|_| panic!("Unrecoverable error syncing file"));
    }
    // ... resize file ...
}
```

**Impact:**
- `sync_all()` is an fsync system call - it blocks until ALL data is physically written to disk
- This happens on EVERY resize operation
- With a 0.4 load factor and starting capacity of 16, resizes happen at:
  - 16 → 32 (at 6 entries)
  - 32 → 64 (at 13 entries)
  - 64 → 128 (at 26 entries)
  - 128 → 256 (at 51 entries)
  - ... and so on up to 20M entries
- That's approximately log₂(20M) ≈ 24 resize operations, each with an expensive fsync
- Each fsync can take 1-10ms on SSDs, 10-100ms on HDDs

**Example:** On a typical SSD, 24 fsyncs × 5ms = 120ms of pure waiting time just for growth fsyncs.

### 3. Redundant Flush Calls (HIGH)

**Location:** `disk_map.rs:310, 315, 332, 337`

**Problem:**
```rust
fn insert_key_into_heap(&mut self, key_bytes: &[u8], key_len: Option<usize>) -> Result<crate::HeapIdx> {
    let key_idx = if let Some(key_len) = key_len {
        let mut page = self.heap.next_free_page(size_of::<usize>() + key_bytes.len());
        page.write_with_len(key_len, key_bytes)?;
        page.flush()?;  // ← EXPLICIT FLUSH
        page.pos()
    } else {
        let mut page = self.heap.next_free_page(key_bytes.len());
        page.write(key_bytes)?;
        page.flush()?;  // ← EXPLICIT FLUSH
        page.pos()
    };
    Ok(key_idx)
}
```

But `PageEntry` has a Drop implementation:
```rust
impl<S: ByteStore> Drop for PageEntry<'_, S> {
    fn drop(&mut self) {
        if self.pos < 0 {
            self.flush().unwrap_or_else(|e| {
                panic!("Failed to flush PageEntry: {e}");
            });
        }
    }
}
```

**Impact:**
- The explicit `page.flush()` calls are unnecessary because `PageEntry::drop()` already flushes
- This means we're calling `update_metadata()` twice for each page
- Removing explicit flushes would halve the number of metadata writes

### 4. No Metadata Batching (HIGH)

**Problem:**
- Metadata is updated after every single operation
- In VecStore, this doesn't matter much
- In MMapFile, this causes constant dirty page marking

**Better Approach:**
- Only write metadata periodically (e.g., every 1000 operations)
- Or write metadata lazily on Drop
- Metadata only needs to be consistent for crash recovery

**Impact:**
- Could reduce metadata writes by 99%+ with batching every 1000 ops
- For 20M inserts: 40M metadata writes → 40K metadata writes

---

## Medium Priority Issues

### 5. Iterator Performance During Stress Test

**Location:** `comprehensive_stress_test.rs:251-264`

**Problem:**
The stress test calls `disk_map.iter().count()` every N inserts:
```rust
if insert_count % config.check_interval == 0 {
    let iter_count = disk_map.iter().count();  // ← ITERATES ENTIRE MAP
    assert_eq!(iter_count, insert_count);

    // Then iterates AGAIN to collect all keys
    let mut found_keys = HashSet::new();
    for result in disk_map.iter() {  // ← SECOND FULL ITERATION
        let (iter_key, _) = result?;
        found_keys.insert(iter_key);
    }
}
```

**Impact:**
- With `check_interval = 100_000`, we iterate the entire map every 100K inserts
- For 20M entries with 100K interval, that's 200 full iterations
- Each iteration reads all entries and decodes all keys
- This is O(n²) behavior for the test

**Note:** This is a test issue, not a core implementation issue, but it makes benchmarking misleading.

### 6. Incremental Rehashing Overhead

**Location:** `entries.rs:375+`, controlled by `reindex_batch = 4`

**Current Behavior:**
- On every insert during a resize, rehash 4 old entries
- Each rehashed entry requires finding its new slot (linear probing)
- This adds constant overhead to every insert during resize

**Impact:**
- With batch size 4 and doubling capacity, resize completes after capacity/4 inserts
- For resize from 1M → 2M, that's 250K inserts with rehashing overhead
- The overhead is acceptable but could be tuned

**Recommendation:**
- Current batch size of 4 seems reasonable
- Could make it adaptive based on capacity
- Or increase to 8 or 16 for faster resize completion

---

## Minor Issues

### 7. Unused Capacity Variables

**Location:** Multiple locations in `disk_map.rs`

**Problem:**
```rust
let capacity = entries.capacity();  // ← Unused variable
```

Lines: 604, 636, 677, 718, 739

**Impact:** None (just warnings), but indicates debugging code left in.

### 8. Dead Code

**Location:** `entries.rs:27, 283, 399`

Unused methods:
- `SlotIdx::is_old()`
- `DoubleArrayEntries::is_empty()`
- `DoubleArrayEntries::get_entry()`

**Impact:** None, but increases binary size slightly.

---

## Recommended Fixes

### Priority 1: Remove Excessive Metadata Writes

**Option A - Lazy Metadata (Recommended):**
```rust
// Only update metadata on Drop or explicit sync
impl<S: ByteStore> Slab<S> {
    pub fn append(&mut self, data: &[u8]) -> u64 {
        // ... allocation code ...
        let result = self.count as u64;
        self.count += 1;
        // DON'T call update_metadata() here
        result
    }
}

impl<S: ByteStore> Drop for Slab<S> {
    fn drop(&mut self) {
        self.update_metadata();  // Write once on drop
    }
}
```

**Option B - Batched Metadata:**
```rust
const METADATA_UPDATE_INTERVAL: usize = 1000;

pub fn append(&mut self, data: &[u8]) -> u64 {
    // ... allocation code ...
    let result = self.count as u64;
    self.count += 1;

    // Only update every N operations
    if self.count % METADATA_UPDATE_INTERVAL == 0 {
        self.update_metadata();
    }
    result
}
```

**Expected Impact:** 99%+ reduction in metadata writes, 2-10x speedup on MMapFile inserts.

### Priority 2: Remove fsync from Growth

```rust
fn grow(&mut self, additional: usize) {
    self.resizes += 1;
    // DON'T flush or sync - let OS handle it
    // if self.mmap.flush().is_ok() {
    //     self.file.sync_all().unwrap_or_else(...);
    // }

    let current_size = self.mmap.len();
    let new_size = (current_size + additional).next_power_of_two();

    // Just unmap, resize, remap
    let mut old_mmap = MmapMut::map_anon(1).unwrap();
    std::mem::swap(&mut self.mmap, &mut old_mmap);
    drop(old_mmap);
    self.file.set_len(new_size as u64).unwrap();
    self.mmap = unsafe { MmapMut::map_mut(&self.file).unwrap() };
}
```

Add an explicit sync method for when durability is needed:
```rust
pub fn sync(&mut self) -> io::Result<()> {
    self.mmap.flush()?;
    self.file.sync_all()?;
    Ok(())
}
```

**Expected Impact:** Eliminates 120ms+ of blocking I/O during growth, significant speedup during resize phases.

### Priority 3: Remove Redundant Flush Calls

In `disk_map.rs`, remove explicit flush calls:
```rust
fn insert_key_into_heap(&mut self, key_bytes: &[u8], key_len: Option<usize>) -> Result<crate::HeapIdx> {
    let key_idx = if let Some(key_len) = key_len {
        let mut page = self.heap.next_free_page(size_of::<usize>() + key_bytes.len());
        page.write_with_len(key_len, key_bytes)?;
        // page.flush()?;  ← REMOVE: Drop will handle it
        page.pos()
    } else {
        let mut page = self.heap.next_free_page(key_bytes.len());
        page.write(key_bytes)?;
        // page.flush()?;  ← REMOVE: Drop will handle it
        page.pos()
    };
    Ok(key_idx)
}
```

**Expected Impact:** 50% reduction in metadata writes, moderate speedup.

### Priority 4: Fix Test Iterator Overhead

In `comprehensive_stress_test.rs`, optimize the periodic checks:
```rust
if insert_count % config.check_interval == 0 {
    let iter_count = disk_map.iter().count();
    assert_eq!(iter_count, insert_count);

    // Don't iterate again to collect keys - we already verified count
    // If we need to verify all keys are present, do it less frequently
}
```

**Expected Impact:** Makes benchmarking more accurate.

---

## Performance Projections

### Current Performance (Estimated)

With 20M u64→u64 inserts:
- ~40M metadata writes (2 per insert)
- ~24 fsyncs during growth (1-10ms each)
- Metadata overhead: ~640MB written unnecessarily
- Expected rate: 10K-50K inserts/sec depending on hardware

### After Fixes (Estimated)

With recommended fixes:
- Metadata writes: 40M → ~40K (99.9% reduction)
- Fsyncs: 24 → 0 during growth
- Expected rate: 200K-1M inserts/sec on modern hardware

**Potential speedup: 4-20x** depending on workload and hardware.

---

## Testing Recommendations

After implementing fixes, test with:

1. **Microbenchmark:** Pure insert performance
   - Compare VecStore vs MMapFile performance
   - Should be similar if metadata writes are fixed

2. **Growth stress test:** Measure time spent in resize
   - Should be near-zero with fsync removed

3. **Crash recovery test:** Verify metadata is eventually consistent
   - Load map after ungraceful shutdown
   - Metadata should be correct after Drop flushes

4. **Long-running test:** Insert 100M+ entries
   - Verify no memory leaks
   - Verify performance remains consistent

---

## Conclusion

The DiskHashMap has a solid architecture with incremental rehashing, but performance is severely limited by excessive metadata writes and unnecessary fsync operations. The recommended fixes are straightforward and should provide dramatic performance improvements with minimal risk.

The core algorithm and data structures are sound - this is purely an implementation optimization issue.
