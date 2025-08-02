---
name: Performance issue
about: Report performance problems or request optimizations
title: '[PERF] '
labels: 'performance'
assignees: ''

---

**Performance Issue Description**
Describe the performance problem you're experiencing.

**Benchmark Results**
If you have benchmark data, please include it:

```
Current performance: X ops/sec
Expected performance: Y ops/sec (or comparison with std::HashMap)
```

**Environment**
- Hardware: [e.g. M1 MacBook Pro, Intel i7-8700K]
- OS: [e.g. Ubuntu 22.04]
- Rust version: [e.g. 1.70.0]
- Package version: [e.g. 0.1.0]

**Data Characteristics**
- Key size: [e.g. 8 bytes, variable 10-100 bytes]
- Value size: [e.g. 64 bytes, variable 1KB-10KB]
- Number of entries: [e.g. 1M, 100M]
- Access pattern: [e.g. random, sequential, mostly reads]

**Storage Configuration**
- Storage type: [e.g. VecStore, MMapFile]
- If MMapFile: Storage device type [e.g. SSD, NVMe, HDD]

**Reproduction Code**
```rust
// Minimal example showing the performance issue
use diskmap::*;

fn benchmark() {
    // Your benchmark code
}
```

**Profiling Data**
If you have profiling data (flamegraphs, perf output, etc.), please include it or attach files.

**Additional Context**
Any other relevant information about the performance issue.