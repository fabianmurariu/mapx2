use criterion::{Criterion, black_box, criterion_group, criterion_main, BenchmarkId};
use criterion::PlotConfiguration;
use diskhashmap::disk_map::DiskHashMap;
use diskhashmap::types::{Native, Str};
use diskhashmap::VecStore;
use rand::{Rng, rng};
use rustc_hash::FxBuildHasher;
use std::time::{Duration, Instant};

// Type aliases for different configurations
type SingleU64U64 = DiskHashMap<Native<u64>, Native<u64>, VecStore, FxBuildHasher>;
type DoubleU64U64 = DiskHashMap<Native<u64>, Native<u64>, VecStore, FxBuildHasher>;
type SingleU64Str = DiskHashMap<Native<u64>, Str, VecStore, FxBuildHasher>;
type DoubleU64Str = DiskHashMap<Native<u64>, Str, VecStore, FxBuildHasher>;
type SingleStrU64 = DiskHashMap<Str, Native<u64>, VecStore, FxBuildHasher>;
type DoubleStrU64 = DiskHashMap<Str, Native<u64>, VecStore, FxBuildHasher>;
type SingleStrStr = DiskHashMap<Str, Str, VecStore, FxBuildHasher>;
type DoubleStrStr = DiskHashMap<Str, Str, VecStore, FxBuildHasher>;

/// Generate u64 key-value pairs
fn generate_u64_u64_data(size: usize) -> Vec<(u64, u64)> {
    let mut rng = rng();
    (0..size)
        .map(|_| {
            (rng.random::<u64>(), rng.random::<u64>())
        })
        .collect()
}

/// Generate u64 keys with string values
fn generate_u64_str_data(size: usize) -> Vec<(u64, String)> {
    let mut rng = rng();
    (0..size)
        .map(|_| {
            let key = rng.random::<u64>();
            let value_len = rng.random_range(5..50);
            let value: String = (0..value_len)
                .map(|_| rng.random_range(b'a'..=b'z') as char)
                .collect();
            (key, value)
        })
        .collect()
}

/// Generate string keys with u64 values
fn generate_str_u64_data(size: usize) -> Vec<(String, u64)> {
    let mut rng = rng();
    (0..size)
        .map(|_| {
            let key_len = rng.random_range(5..25);
            let key: String = (0..key_len)
                .map(|_| rng.random_range(b'a'..=b'z') as char)
                .collect();
            let value = rng.random::<u64>();
            (key, value)
        })
        .collect()
}

/// Generate string key-value pairs
fn generate_str_str_data(size: usize) -> Vec<(String, String)> {
    let mut rng = rng();
    (0..size)
        .map(|_| {
            let key_len = rng.random_range(5..25);
            let key: String = (0..key_len)
                .map(|_| rng.random_range(b'a'..=b'z') as char)
                .collect();
            let value_len = rng.random_range(5..50);
            let value: String = (0..value_len)
                .map(|_| rng.random_range(b'a'..=b'z') as char)
                .collect();
            (key, value)
        })
        .collect()
}

/// Measure individual insert times to find max latency for single array
fn measure_single_insert_latencies(data: &[(u64, u64)]) -> (Duration, Duration, Duration) {
    let mut map = SingleU64U64::new();
    let mut min_time = Duration::from_secs(u64::MAX);
    let mut max_time = Duration::ZERO;
    let mut total_time = Duration::ZERO;

    for (k, v) in data.iter() {
        let start = Instant::now();
        map.insert(k, v).unwrap();
        let elapsed = start.elapsed();
        
        min_time = min_time.min(elapsed);
        max_time = max_time.max(elapsed);
        total_time += elapsed;
    }

    let avg_time = total_time / data.len() as u32;
    (min_time, max_time, avg_time)
}

/// Measure individual insert times to find max latency for double array
fn measure_double_insert_latencies(data: &[(u64, u64)]) -> (Duration, Duration, Duration) {
    let mut map = DoubleU64U64::new_with_double_array();
    let mut min_time = Duration::from_secs(u64::MAX);
    let mut max_time = Duration::ZERO;
    let mut total_time = Duration::ZERO;

    for (k, v) in data.iter() {
        let start = Instant::now();
        map.insert(k, v).unwrap();
        let elapsed = start.elapsed();
        
        min_time = min_time.min(elapsed);
        max_time = max_time.max(elapsed);
        total_time += elapsed;
    }

    let avg_time = total_time / data.len() as u32;
    (min_time, max_time, avg_time)
}

/// Benchmark u64 -> u64 mappings
fn bench_u64_u64(c: &mut Criterion) {
    let sizes = [1000, 10000, 50000];
    
    for size in sizes {
        let data = generate_u64_u64_data(size);
        let mut group = c.benchmark_group(format!("u64_u64_size_{}", size));
        
        // Configure for latency measurement
        group.sample_size(20);
        group.measurement_time(Duration::from_secs(30));
        group.plot_config(PlotConfiguration::default().summary_scale(criterion::AxisScale::Logarithmic));

        // Single array benchmarks
        group.bench_with_input(BenchmarkId::new("single_array_bulk_insert", size), &data, |b, data| {
            b.iter_with_setup(
                || SingleU64U64::new(),
                |mut map| {
                    for (k, v) in data.iter() {
                        map.insert(black_box(k), black_box(v)).unwrap();
                    }
                }
            );
        });

        // Double array benchmarks
        group.bench_with_input(BenchmarkId::new("double_array_bulk_insert", size), &data, |b, data| {
            b.iter_with_setup(
                || DoubleU64U64::new_with_double_array(),
                |mut map| {
                    for (k, v) in data.iter() {
                        map.insert(black_box(k), black_box(v)).unwrap();
                    }
                }
            );
        });

        // Lookup benchmarks
        let single_map = {
            let mut map = SingleU64U64::new();
            for (k, v) in data.iter() {
                map.insert(k, v).unwrap();
            }
            map
        };

        let double_map = {
            let mut map = DoubleU64U64::new_with_double_array();
            for (k, v) in data.iter() {
                map.insert(k, v).unwrap();
            }
            map
        };

        group.bench_with_input(BenchmarkId::new("single_array_lookup", size), &data, |b, data| {
            b.iter(|| {
                for (k, _) in data.iter() {
                    black_box(single_map.get(black_box(k)).unwrap());
                }
            })
        });

        group.bench_with_input(BenchmarkId::new("double_array_lookup", size), &data, |b, data| {
            b.iter(|| {
                for (k, _) in data.iter() {
                    black_box(double_map.get(black_box(k)).unwrap());
                }
            })
        });

        group.finish();
        
        // Individual insert latency measurement (not through criterion to avoid overhead)
        if size <= 10000 { // Only for smaller sizes to avoid long runs
            println!("\n=== Latency Analysis for u64->u64, size {} ===", size);
            
            let (single_min, single_max, single_avg) = measure_single_insert_latencies(&data);
            let (double_min, double_max, double_avg) = measure_double_insert_latencies(&data);
            
            println!("Single Array - Min: {:?}, Max: {:?}, Avg: {:?}", single_min, single_max, single_avg);
            println!("Double Array - Min: {:?}, Max: {:?}, Avg: {:?}", double_min, double_max, double_avg);
            println!("Max latency improvement: {:.2}x", single_max.as_nanos() as f64 / double_max.as_nanos() as f64);
        }
    }
}

/// Benchmark u64 -> String mappings
fn bench_u64_str(c: &mut Criterion) {
    let sizes = [1000, 10000];
    
    for size in sizes {
        let data = generate_u64_str_data(size);
        let mut group = c.benchmark_group(format!("u64_str_size_{}", size));
        
        group.sample_size(20);
        group.measurement_time(Duration::from_secs(20));

        // Single array benchmarks
        group.bench_with_input(BenchmarkId::new("single_array_bulk_insert", size), &data, |b, data| {
            b.iter_with_setup(
                || SingleU64Str::new(),
                |mut map| {
                    for (k, v) in data.iter() {
                        map.insert(black_box(k), black_box(v.as_str())).unwrap();
                    }
                }
            );
        });

        // Double array benchmarks
        group.bench_with_input(BenchmarkId::new("double_array_bulk_insert", size), &data, |b, data| {
            b.iter_with_setup(
                || DoubleU64Str::new_with_double_array(),
                |mut map| {
                    for (k, v) in data.iter() {
                        map.insert(black_box(k), black_box(v.as_str())).unwrap();
                    }
                }
            );
        });

        group.finish();
    }
}

/// Benchmark String -> u64 mappings  
fn bench_str_u64(c: &mut Criterion) {
    let sizes = [1000, 10000];
    
    for size in sizes {
        let data = generate_str_u64_data(size);
        let mut group = c.benchmark_group(format!("str_u64_size_{}", size));
        
        group.sample_size(20);
        group.measurement_time(Duration::from_secs(20));

        // Single array benchmarks
        group.bench_with_input(BenchmarkId::new("single_array_bulk_insert", size), &data, |b, data| {
            b.iter_with_setup(
                || SingleStrU64::new(),
                |mut map| {
                    for (k, v) in data.iter() {
                        map.insert(black_box(k.as_str()), black_box(v)).unwrap();
                    }
                }
            );
        });

        // Double array benchmarks
        group.bench_with_input(BenchmarkId::new("double_array_bulk_insert", size), &data, |b, data| {
            b.iter_with_setup(
                || DoubleStrU64::new_with_double_array(),
                |mut map| {
                    for (k, v) in data.iter() {
                        map.insert(black_box(k.as_str()), black_box(v)).unwrap();
                    }
                }
            );
        });

        group.finish();
    }
}

/// Benchmark String -> String mappings
fn bench_str_str(c: &mut Criterion) {
    let sizes = [1000, 5000];
    
    for size in sizes {
        let data = generate_str_str_data(size);
        let mut group = c.benchmark_group(format!("str_str_size_{}", size));
        
        group.sample_size(20);
        group.measurement_time(Duration::from_secs(20));

        // Single array benchmarks
        group.bench_with_input(BenchmarkId::new("single_array_bulk_insert", size), &data, |b, data| {
            b.iter_with_setup(
                || SingleStrStr::new(),
                |mut map| {
                    for (k, v) in data.iter() {
                        map.insert(black_box(k.as_str()), black_box(v.as_str())).unwrap();
                    }
                }
            );
        });

        // Double array benchmarks
        group.bench_with_input(BenchmarkId::new("double_array_bulk_insert", size), &data, |b, data| {
            b.iter_with_setup(
                || DoubleStrStr::new_with_double_array(),
                |mut map| {
                    for (k, v) in data.iter() {
                        map.insert(black_box(k.as_str()), black_box(v.as_str())).unwrap();
                    }
                }
            );
        });

        group.finish();
    }
}

criterion_group!(
    benches, 
    bench_u64_u64,
    bench_u64_str, 
    bench_str_u64,
    bench_str_str
);
criterion_main!(benches);