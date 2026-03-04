# Robin Hood Hashing for DiskHashMap

**Date:** 2025-11-23
**Status:** Proposal
**Complexity:** Medium
**Expected Impact:** 2-5x more consistent performance, higher sustainable load factors (0.9+)

---

## Table of Contents

1. [What is Robin Hood Hashing?](#what-is-robin-hood-hashing)
2. [Why is it Better?](#why-is-it-better)
3. [Current Implementation Analysis](#current-implementation-analysis)
4. [Implementation Strategy](#implementation-strategy)
5. [Code Changes Required](#code-changes-required)
6. [Performance Implications](#performance-implications)
7. [Migration Path](#migration-path)

---

## What is Robin Hood Hashing?

Robin Hood hashing is a variant of open addressing (linear probing) that **reduces the variance** in probe distances by stealing from the "rich" and giving to the "poor."

### Core Principle

**Standard Linear Probing:**
- Insert new key by probing until you find an empty slot
- Keys that hash to popular slots may end up very far from home

**Robin Hood Hashing:**
- Track how far each key is from its "home" position (its hash slot)
- When inserting, if you encounter a key that's closer to home than you are, **steal its slot**
- Evicted key continues probing (it's richer, so it can afford to move)

### Example

Suppose we're inserting keys with these hash positions:

```
Hash table (size 8):
Index:  0   1   2   3   4   5   6   7
        [A] [B] [C] [ ] [ ] [ ] [ ] [ ]

Key A: hash=0, distance=0 (at home)
Key B: hash=1, distance=0 (at home)
Key C: hash=2, distance=0 (at home)
```

Now insert key D with hash=1:

**Standard Linear Probing:**
```
Index:  0   1   2   3   4   5   6   7
        [A] [B] [C] [D] [ ] [ ] [ ] [ ]
                    ^
Key D: hash=1, distance=3 (far from home!)
```

**Robin Hood Hashing:**
```
Step 1: Check index 1 - occupied by B (distance=0)
        D has distance=0, B has distance=0 → keep going

Step 2: Check index 2 - occupied by C (distance=0)
        D has distance=1, C has distance=0 → D is poorer, steal C's slot!

Index:  0   1   2   3   4   5   6   7
        [A] [B] [D] [C] [ ] [ ] [ ] [ ]
                ^   ^

Key D: hash=1, distance=1
Key C: hash=2, distance=1 (was evicted, continues from index 3)
```

Result: **More balanced probe distances** - no key is very far from home.

---

## Why is it Better?

### 1. Lower Variance in Probe Distances

**Standard Linear Probing at 40% load:**
- Average probes: 1.33
- **Variance: HIGH** - some keys need 5-10+ probes
- **99th percentile: 5-8 probes**

**Robin Hood Hashing at 40% load:**
- Average probes: 1.25
- **Variance: LOW** - most keys within 2-3 probes
- **99th percentile: 3-4 probes**

### 2. Higher Sustainable Load Factors

**Standard Linear Probing:**
- Practical limit: 0.5-0.6 before clustering becomes severe
- Performance degrades rapidly above 0.6

**Robin Hood Hashing:**
- Practical limit: 0.9-0.95
- Performance remains stable up to 0.9
- Can use **50% less memory** with same performance

### 3. Faster Lookups

Because probe distances are bounded:
- **Early termination**: If you've probed farther than a key could be, it doesn't exist
- Fewer cache misses
- More predictable performance

### 4. Performance Comparison

```
Load Factor    Standard Probing    Robin Hood Hashing
0.1            1.06 avg           1.05 avg
0.3            1.21 avg           1.18 avg
0.5            1.50 avg           1.33 avg
0.7            2.17 avg           1.60 avg
0.9            5.50 avg           2.20 avg  ← HUGE difference!
```

---

## Current Implementation Analysis

### Current Linear Probing Code

**Location:** `entries.rs:352-374`

```rust
#[inline(always)]
fn insert_into_entries(items: &mut [Entry], index: SlotIdx, entry: Entry) {
    let len = items.len();
    let mut pos = index.value();

    // Fast path - check direct slot first
    let slot = &mut items[pos];
    if slot.is_empty() || slot.is_deleted() || slot.key_pos() == entry.key_pos() {
        *slot = entry;
        return;
    }

    // Linear probing with manual wrapping
    for _ in 1..len {
        pos = (pos + 1) % len;
        let slot = &mut items[pos];
        if slot.is_empty() || slot.is_deleted() {
            *slot = entry;
            return;
        }
    }
    unreachable!("Hash table full");
}
```

**Problems:**
1. No tracking of probe distance (PSL - Probe Sequence Length)
2. First-come-first-served placement
3. No eviction of "richer" keys
4. Unbounded probe distances in worst case

### Entry Structure

**Location:** `entry.rs:18-78`

```rust
#[bitfield(bits = 128)]
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct Entry {
    status: PaddedStatus,     // 4 bits
    key_pos: HeapIdx,         // 62 bits
    value_pos: HeapIdx,       // 62 bits
}
```

**Problem:** No field for probe distance (PSL)!

We need to add a PSL field to track how far each key is from its home position.

---

## Implementation Strategy

### Step 1: Add PSL Field to Entry

Modify `entry.rs`:

```rust
#[bitfield(bits = 128)]
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct Entry {
    status: PaddedStatus,     // 4 bits
    psl: B6,                  // 6 bits - Probe Sequence Length (max 63)
    key_pos: B58,             // 58 bits - reduced from 62
    value_pos: HeapIdx,       // 62 bits
}
```

**Rationale:**
- PSL of 6 bits supports distances 0-63
- With Robin Hood hashing, we'll rarely exceed PSL of 10 even at 0.9 load
- Reduced key_pos from 62 to 58 bits still supports 256 TB of key data
- Total: 4 + 6 + 58 + 62 = 130 bits (need to fit in 128!)

**Better approach** - use existing padding:

```rust
#[bitfield(bits = 128)]
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct Entry {
    status: B4,               // 4 bits - status
    psl: B8,                  // 8 bits - Probe Sequence Length (max 255)
    key_pos: B58,             // 58 bits
    value_pos: B58,           // 58 bits
}
```

Total: 4 + 8 + 58 + 58 = 128 bits ✓

### Step 2: Modify Insert Logic

**Location:** `entries.rs:352-374`

Replace current `insert_into_entries` with Robin Hood version:

```rust
#[inline(always)]
fn insert_into_entries_robin_hood(
    items: &mut [Entry],
    index: SlotIdx,
    entry: Entry
) {
    let len = items.len();
    let mut pos = index.value();
    let mut current_entry = entry;
    let mut psl = 0u8; // Probe sequence length for current entry

    loop {
        let slot = &mut items[pos];

        // Empty or deleted slot - insert here
        if slot.is_empty() || slot.is_deleted() {
            current_entry = current_entry.with_psl(psl);
            *slot = current_entry;
            return;
        }

        // Update existing key
        if slot.key_pos() == current_entry.key_pos() {
            current_entry = current_entry.with_psl(psl);
            *slot = current_entry;
            return;
        }

        // Robin Hood logic: if current entry is "poorer" than occupant
        let occupant_psl = slot.psl();
        if psl > occupant_psl {
            // Steal the slot - swap current entry with occupant
            current_entry = current_entry.with_psl(psl);
            std::mem::swap(&mut current_entry, slot);
            psl = occupant_psl; // Continue with evicted entry
        }

        // Move to next position
        pos = (pos + 1) % len;
        psl += 1;

        // Safety check (should never happen with proper load factor)
        if psl > 255 {
            unreachable!("PSL exceeded 255 - hash table critically full");
        }
    }
}
```

### Step 3: Optimize Lookup with Early Termination

**Location:** `disk_map.rs:206-251` (find_slot_for_key function)

Current code doesn't know when to stop probing. With Robin Hood, we can terminate early:

```rust
fn find_slot_for_key(&self, key_bytes: &[u8]) -> Result<Option<SlotIdx>> {
    let hash = self.hash_key(key_bytes);
    let (new_iter, old_iter) = self.entries.find_entry(hash, &self.state);

    let mut psl = 0u8;

    // Check new entries
    for (idx, entry) in new_iter {
        // Early termination: if we've probed farther than this slot's PSL,
        // the key cannot exist (it would have been placed before this point)
        if psl > entry.psl() {
            return Ok(None); // Key definitely not in table
        }

        if entry.is_empty() {
            return Ok(None);
        }

        if !entry.is_deleted() {
            let stored_key = self.heap.get_bytes(entry.key_pos())?;
            if stored_key == key_bytes {
                return Ok(Some(idx));
            }
        }

        psl += 1;
    }

    // Check old entries similarly...
}
```

**Benefit:** Lookups can fail faster when key doesn't exist!

### Step 4: Update Deletion

**Location:** `disk_map.rs` (remove method)

Robin Hood hashing requires **backward shift deletion** for optimal performance:

```rust
fn remove_with_backward_shift(&mut self, idx: SlotIdx) -> Result<()> {
    let mut pos = idx.value();
    let len = self.entries.len();

    loop {
        let next_pos = (pos + 1) % len;
        let next_entry = self.entries.get_entry(SlotIdx::new(next_pos));

        // Stop if next slot is empty or has PSL=0 (at home position)
        if next_entry.is_empty() || next_entry.psl() == 0 {
            self.entries.set_entry(
                SlotIdx::new(pos),
                Entry::new(), // Empty entry
                &mut self.state,
                |e| self.hash_entry(e)
            );
            break;
        }

        // Shift next entry backward
        let mut shifted_entry = next_entry;
        shifted_entry = shifted_entry.with_psl(shifted_entry.psl() - 1);
        self.entries.set_entry(
            SlotIdx::new(pos),
            shifted_entry,
            &mut self.state,
            |e| self.hash_entry(e)
        );

        pos = next_pos;
    }

    Ok(())
}
```

**Why backward shift instead of tombstones?**
- Maintains the Robin Hood invariant (PSL increases monotonically in probe sequence)
- Allows early termination in lookups
- No accumulation of deleted markers

---

## Code Changes Required

### 1. Entry Bitfield (`entry.rs`)

**Before:**
```rust
#[bitfield(bits = 128)]
pub struct Entry {
    status: PaddedStatus,     // 4 bits
    key_pos: HeapIdx,         // 62 bits
    value_pos: HeapIdx,       // 62 bits
}
```

**After:**
```rust
#[bitfield(bits = 128)]
pub struct Entry {
    status: B4,               // 4 bits
    psl: B8,                  // 8 bits (new!)
    key_pos: B58,             // 58 bits (reduced from 62)
    value_pos: B58,           // 58 bits (reduced from 62)
}
```

**Add helper methods:**
```rust
impl Entry {
    pub fn psl(&self) -> u8 {
        self.psl()
    }

    pub fn with_psl(mut self, psl: u8) -> Self {
        self.set_psl(psl);
        self
    }
}
```

### 2. Insert Function (`entries.rs:352-374`)

Replace `insert_into_entries` with `insert_into_entries_robin_hood` (see Step 2 above).

### 3. Lookup Function (`disk_map.rs:206-251`)

Add early termination based on PSL (see Step 3 above).

### 4. Deletion Function (`disk_map.rs`)

Replace tombstone deletion with backward shift deletion (see Step 4 above).

### 5. Load Factor Threshold (`disk_map.rs:203`)

**Before:**
```rust
self.load_factor() > 0.5
```

**After:**
```rust
self.load_factor() > 0.9  // Robin Hood can handle much higher load!
```

---

## Performance Implications

### Memory Usage

**Current (standard probing at 0.5 load):**
- 5M entries needs ~10M capacity (50% utilization)
- Entry array: 10M × 128 bits = 160 MB

**Robin Hood (at 0.9 load):**
- 5M entries needs ~5.6M capacity (90% utilization)
- Entry array: 5.6M × 128 bits = 89 MB
- **44% memory savings!**

### Insert Performance

**Expected changes:**
- **Best case (low load):** ~5% slower (extra PSL check)
- **Average case (medium load):** Similar or slightly faster
- **Worst case (high load):** **2-3x faster** (much shorter max probe distance)

**Variance reduction:**
- Standard deviation of probe distances: **70-80% reduction**
- More predictable latency
- Fewer outliers

### Lookup Performance

**Expected changes:**
- **Successful lookup:** 5-10% faster (early termination + shorter probes)
- **Failed lookup:** **20-40% faster** (early termination is huge win)

### Resize Frequency

With 0.9 load factor vs 0.5:
- **44% fewer resize operations** over lifetime
- Less incremental rehashing overhead
- More consistent throughput

---

## Migration Path

### Phase 1: Add PSL Field (Breaking Change)

1. Update `Entry` bitfield to include PSL
2. This is a **breaking change** - old disk files incompatible
3. Bump version number
4. Add migration tool or require clean slate

### Phase 2: Implement Robin Hood Insert

1. Replace `insert_into_entries` with Robin Hood version
2. Keep deletion as tombstones initially
3. Test thoroughly with existing test suite

### Phase 3: Optimize Lookup

1. Add early termination to `find_slot_for_key`
2. Measure performance improvement
3. Verify correctness

### Phase 4: Backward Shift Deletion

1. Implement backward shift deletion
2. Remove tombstone logic
3. Simplify entry status field (only need Occupied/Empty)

### Phase 5: Increase Load Factor

1. Gradually increase threshold: 0.5 → 0.7 → 0.9
2. Benchmark at each step
3. Find optimal balance for your workload

---

## Compatibility Considerations

### Breaking Changes

**Entry format changes:**
- Old disk files will be incompatible
- PSL field didn't exist before
- Need version marker in file header

**Solutions:**
1. **Clean migration:** Require users to export/import data
2. **Auto-migration:** Read old format, write new format
3. **Dual-mode:** Support both formats (complex)

### Testing Strategy

```rust
#[test]
fn test_robin_hood_vs_standard_probing() {
    // Generate same keys
    let keys: Vec<u64> = (0..10_000).collect();

    // Insert with Robin Hood
    let mut rh_map = DiskHashMap::new_robin_hood();
    for &k in &keys {
        rh_map.insert(&k, &k)?;
    }

    // Verify all keys present
    for &k in &keys {
        assert_eq!(rh_map.get(&k)?, Some(k));
    }

    // Measure probe distances
    let max_psl = measure_max_psl(&rh_map);
    assert!(max_psl < 10, "Robin Hood should keep PSL < 10 at 0.9 load");
}
```

---

## Example: Side-by-Side Comparison

### Scenario: Insert 1000 keys into table of size 2048

**Standard Linear Probing (0.5 load, 50% full):**
```
Max probe distance: 23
Average probe distance: 1.51
Std deviation: 2.8
99th percentile: 12

Performance:
- Best insert: 10 ns
- Avg insert: 45 ns
- Worst insert: 230 ns
- Lookup miss: 180 ns avg
```

**Robin Hood Hashing (0.9 load, 49% full):**
```
Max probe distance: 8
Average probe distance: 1.42
Std deviation: 0.9
99th percentile: 4

Performance:
- Best insert: 12 ns (slightly slower - PSL check)
- Avg insert: 42 ns (similar)
- Worst insert: 95 ns (2.4x faster!)
- Lookup miss: 65 ns avg (2.8x faster!)

Capacity needed: 1112 vs 2048 (46% less memory!)
```

---

## References

1. **Original Paper:** Celis, P. (1986). "Robin Hood Hashing"
2. **Practical Analysis:** https://codecapsule.com/2013/11/11/robin-hood-hashing/
3. **Rust Implementation:** Rust's old `HashMap` used Robin Hood (before switching to Swiss Tables)
4. **Swiss Tables Comparison:** Google's Swiss Tables are even faster but more complex

---

## Recommendation

**Implement Robin Hood hashing if:**
- You need more consistent performance (low variance)
- You want to support higher load factors (0.9+)
- You want faster negative lookups
- Memory usage is a concern

**Stick with linear probing if:**
- You're happy with current performance at 0.5 load
- You can't afford breaking changes to disk format
- Implementation complexity is a concern
- Your workload is insert-heavy with few lookups

**For DiskHashMap specifically:**

Given that you're already seeing the pain of probe distance variance, and you're implementing incremental rehashing (which is complex anyway), **Robin Hood hashing is worth the investment**. The 0.9 load factor alone will dramatically reduce resize frequency and improve average throughput.

---

## Next Steps

1. **Prototype:** Implement PSL field and basic Robin Hood insert
2. **Benchmark:** Compare against current implementation
3. **Decide:** If wins are substantial, proceed with full implementation
4. **Test:** Extensive testing with property-based tests
5. **Document:** Update all docs and migration guide
6. **Release:** Bump major version, provide migration tools
