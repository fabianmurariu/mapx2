use bytemuck;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use diskmap::byte_store::MMapFile;
use diskmap::raw_map::OpenHashMap;
use rand::Rng;
use rustc_hash::FxBuildHasher;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::tempdir;

// A transparent wrapper around u64 to implement AsRef<[u8]>
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct U64(u64);

impl AsRef<[u8]> for U64 {
    fn as_ref(&self) -> &[u8] {
        // This is safe because U64 is repr(transparent) and contains a u64.
        // bytemuck ensures that we can safely view the u64 as a byte slice.
        bytemuck::bytes_of(&self.0)
    }
}

// Type alias for our Mmap-backed OpenHashMap with U64 keys
type OpenHashMapMmapU64 = OpenHashMap<U64, Vec<u8>, MMapFile, MMapFile, MMapFile, FxBuildHasher>;

/// Generates a vector of key-value pairs for benchmarking.
/// Keys are random u64 values, and values are random alphanumeric strings.
fn generate_data(size: usize) -> Vec<(U64, Vec<u8>)> {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..size)
        .map(|_| {
            let key = U64(rng.r#gen::<u64>());
            let value_len = rng.gen_range(1..=250);
            let value: Vec<u8> = (0..value_len)
                .map(|_| {
                    let idx = rng.gen_range(0..CHARSET.len());
                    CHARSET[idx]
                })
                .collect();
            (key, value)
        })
        .collect()
}

fn benchmark_u64_key_hash_map(c: &mut Criterion) {
    for &size in &[100_000, 1_000_000] {
        let mut group = c.benchmark_group(format!("u64_key_size={}", size));
        if size >= 1_000_000 {
            // Reduce sample count for large benchmarks to keep them from running too long
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(60));
        }

        let data = generate_data(size);

        // --- std::collections::HashMap ---
        group.bench_function("std::HashMap - insert", |b| {
            b.iter(|| {
                let mut map = HashMap::new();
                for (k, v) in data.iter() {
                    map.insert(black_box(*k), black_box(v.clone()));
                }
            })
        });

        let mut std_map = HashMap::new();
        for (k, v) in data.iter() {
            std_map.insert(*k, v.clone());
        }
        group.bench_function("std::HashMap - get", |b| {
            b.iter(|| {
                for (k, _) in data.iter() {
                    std_map.get(black_box(k));
                }
            })
        });

        // --- OpenHashMap with MmapFile backing ---
        let dir = tempdir().unwrap();
        // Generous sizing: avg 125 bytes/value + 8 bytes/key + overhead
        group.bench_function("OpenHashMap<Mmap> - insert", |b| {
            b.iter_with_setup(
                || {
                    // Recreate files for each iteration to start fresh
                    let path = dir.path();
                    OpenHashMapMmapU64::new_in(path).unwrap()
                },
                |mut map: OpenHashMapMmapU64| {
                    for (k, v) in data.iter() {
                        map.insert(black_box(*k), black_box(v.clone()));
                    }
                },
            );
        });

        // Setup for the get benchmark
        let get_dir = tempdir().unwrap();
        let mut ohm_mmap_map_get = OpenHashMapMmapU64::new_in(get_dir.path()).unwrap();
        for (k, v) in data.iter() {
            ohm_mmap_map_get.insert(*k, v.clone());
        }
        group.bench_function("OpenHashMap<Mmap> - get", |b| {
            b.iter(|| {
                for (k, _) in data.iter() {
                    ohm_mmap_map_get.get(black_box(*k));
                }
            })
        });
    }
}

criterion_group!(benches, benchmark_u64_key_hash_map);
criterion_main!(benches);
