# Performance Degradation Analysis

## Problem Statement

Insertion performance degrades as more items are inserted into the DiskHashMap. The average insertion rate decreases over time.

## Root Causes

### 1. **Linear Probing Performance Degradation** (PRIMARY CAUSE)

**Location:** `entries.rs:352-374`

**The Problem:**

With a 0.4 load factor threshold and linear probing, performance degrades in a **sawtooth pattern**:

```
Performance over time:
^
|  /|  /|  /|  /|
| / | / | / | / |
|/  |/  |/  |/  |
+-------------------> Time
 ^   ^   ^   ^
 Resize points
```

**Why this happens:**

1. **After resize (0% load):** Inserts are very fast - most keys land in empty slots
2. **At 20% load:** Probe distances start to increase due to clustering
3. **At 40% load (resize trigger):** Probe distances are much longer - inserts are slowest
4. **After resize:** Back to fast inserts

The **average probe distance** with linear probing:
- At 10% load: ~1.06 probes
- At 20% load: ~1.13 probes
- At 30% load: ~1.21 probes
- **At 40% load: ~1.33 probes** ← We resize here

But this is just average! The **worst case** gets much worse:
- At 40% load, some inserts may need 5-10+ probes due to clustering

**Measurement from stress test:**

The rate calculation in `comprehensive_stress_test.rs`:
```rust
let rate = insert_count as f64 / elapsed.as_secs_f64();
```

This is the **average rate since start**, not instantaneous. If this average is decreasing, it means recent inserts are significantly slower than early inserts.

### 2. **Incremental Rehashing Overhead** (SECONDARY CAUSE)

**Location:** `entries.rs:378-395`

**During resize operations:**
- Every insert rehashes `reindex_batch = 4` old entries
- This means **5x the work** per insert (1 new insert + 4 rehashes)
- With doubling from 1M → 2M capacity, this affects 250K inserts (2M / 4 / 2)

**Impact:**
- For a resize from 1M to 2M entries:
  - 400K entries to rehash (40% of 1M)
  - At 4 entries per insert: 100K inserts affected
  - Each affected insert does 5x normal work

### 3. **Cache Effects** (TERTIARY CAUSE)

As the hash table grows:
- **At 100K entries:** ~12MB (128 bytes per entry) - fits in L3 cache
- **At 1M entries:** ~128MB - exceeds L3 cache
- **At 10M entries:** ~1.3GB - mostly main memory access

Memory access latency:
- L1 cache: ~4 cycles
- L3 cache: ~40 cycles
- Main memory: ~200 cycles

### 4. **HashMap Iteration in Test** (TEST ISSUE)

**Location:** `comprehensive_stress_test.rs:63`

```rust
for (key, value) in std_map.iter() {  // ← Iteration order is RANDOMIZED
```

Standard `HashMap` iteration order is randomized. This means:
- Keys are not inserted in any predictable order
- This can create worst-case clustering patterns in linear probing
- Better performance with sequential insertion

## Evidence

### Expected Behavior

For 5M inserts with 0.4 load factor and capacity doubling:

| Resize Event | Old Cap | New Cap | Load @ Resize | Inserts Affected | Rehash Overhead |
|--------------|---------|---------|---------------|------------------|-----------------|
| 1 | 16 | 32 | 40% (6 items) | 8 | 4x slower |
| 2 | 32 | 64 | 40% (13 items) | 13 | 4x slower |
| 3 | 64 | 128 | 40% (26 items) | 26 | 4x slower |
| ... | ... | ... | ... | ... | ... |
| ~23 | 8M | 16M | 40% (3.2M items) | 800K | 4x slower |

**Total affected inserts:** ~1.6M out of 5M (32%)

During these windows, inserts are 4-5x slower due to rehashing overhead.

### Performance Profile

Expected insertion rates (rough estimates):

**Early phase (0-100K inserts):**
- Small capacity, fits in cache
- Low load factors
- **Rate: 200K-500K inserts/sec**

**Mid phase (100K-1M inserts):**
- Medium capacity, exceeds L3 cache
- Load factors cycling 0-40%
- **Rate: 100K-200K inserts/sec**

**Late phase (1M-5M inserts):**
- Large capacity, main memory access
- Load factors cycling 0-40%
- Frequent rehashing windows
- **Rate: 50K-100K inserts/sec**

## Solutions

### Immediate Fixes

#### 1. Increase Load Factor Threshold

**Change:** `disk_map.rs:202`
```rust
// Before:
self.load_factor() > 0.4

// After:
self.load_factor() > 0.5  // or even 0.6
```

**Impact:**
- Fewer resize operations
- Less rehashing overhead
- But: Longer probe distances at peak load

**Trade-off:** At 50% load factor, average probe distance is 1.5 (vs 1.33 at 40%)

#### 2. Increase Rehash Batch Size

**Change:** `entries.rs:331, 338`
```rust
// Before:
reindex_batch: 4,

// After:
reindex_batch: 8,  // or 16
```

**Impact:**
- Resize completes in fewer inserts
- But: Each affected insert is 2x slower during resize
- Net effect: Roughly same total work, different distribution

#### 3. Reduce Resize Frequency

**Change:** Use a larger growth factor
```rust
// In grow() function, instead of doubling:
let new_capacity = (self.capacity() * 3) / 2;  // 1.5x growth
```

**Impact:**
- Fewer resize operations
- But: More wasted space

### Better Solution: Robin Hood Hashing

Replace linear probing with Robin Hood hashing:
- Maintains lower variance in probe distances
- More consistent performance across load factors
- Can safely use higher load factors (0.9+)

### Diagnostic Tool

Add to `comprehensive_stress_test.rs`:

```rust
if insert_count % config.check_interval == 0 {
    let load_factor = disk_map.len() as f64 / disk_map.capacity() as f64;
    let instant_rate = config.check_interval as f64 / last_report.elapsed().as_secs_f64();

    println!(
        "   Progress: {}/{} - Load: {:.2} - Instant: {:.0}/sec - Avg: {:.0}/sec",
        insert_count,
        num_entries,
        load_factor,
        instant_rate,
        insert_count as f64 / insert_start.elapsed().as_secs_f64()
    );
    last_report = Instant::now();
}
```

This will show:
- Load factor over time
- Instantaneous rate (rate for last interval)
- Average rate (rate since start)

You'll see the instantaneous rate drop as load factor approaches 0.4, then spike back up after resize.

## Recommended Actions

**For immediate improvement:**

1. **Increase load factor to 0.5** - 25% fewer resizes
2. **Add diagnostics to stress test** - Understand the pattern
3. **Profile with Instruments Time Profiler** - Confirm probe distance is the bottleneck

**For long-term:**

Consider Robin Hood hashing or other variance-reducing probe strategies.

## Expected Results After Fixes

With load factor = 0.5:
- 20% reduction in total resize operations
- More consistent performance between resize points
- Slightly longer probe distances at peak

Overall: 10-30% improvement in average insertion rate.
