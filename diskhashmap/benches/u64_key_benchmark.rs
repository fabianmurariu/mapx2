use criterion::{Criterion, black_box, criterion_group, criterion_main};
use diskhashmap::disk_map::DiskHashMap;
use diskhashmap::types::{Bytes, Native};
use rand::{Rng, RngCore};
use rustc_hash::FxBuildHasher;
use std::time::Duration;
use tempfile::tempdir;

// Type alias for our Mmap-backed HashMap with u64 keys and Vec<u8> values
type HashMapMmapU64 =
    DiskHashMap<Native<u64>, Bytes, diskhashmap::byte_store::MMapFile, FxBuildHasher>;

/// Generates a vector of key-value pairs for benchmarking.
/// Keys are random u64 values, and values are random byte vectors.
fn generate_data(size: usize) -> Vec<(u64, Vec<u8>)> {
    let mut rng = rand::rng();
    (0..size)
        .map(|_| {
            let key = rng.random::<u64>();
            let value_len = rng.random_range(1..=250);
            let mut value = vec![0u8; value_len];
            rng.fill_bytes(&mut value);
            (key, value)
        })
        .collect()
}

fn benchmark_u64_key_hash_map(c: &mut Criterion) {
    for &size in &[100_000, 1_000_000] {
        let mut group = c.benchmark_group(format!("u64_key_size={size}"));
        if size >= 1_000_000 {
            // Reduce sample count for large benchmarks to keep them from running too long
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(60));
        }

        let data = generate_data(size);

        // --- HashMap with MmapFile backing ---
        group.bench_function("HashMap<Mmap> - insert", |b| {
            b.iter_with_setup(
                || {
                    let dir = tempdir().unwrap();
                    let map = HashMapMmapU64::new_in(dir.path()).unwrap();
                    // Keep the TempDir alive for the duration of the iteration
                    (map, dir)
                },
                |(mut map, _dir)| {
                    // _dir is dropped here, cleaning up the temp directory
                    for (k, v) in data.iter() {
                        // Use the trait-based insert method
                        let _ = map.insert(black_box(k), black_box(v));
                    }
                },
            );
        });

        // Setup for the get benchmark
        let get_dir = tempdir().unwrap();
        let mut hash_map_get = HashMapMmapU64::new_in(get_dir.path()).unwrap();
        for (k, v) in data.iter() {
            let _ = hash_map_get.insert(k, v);
        }
        group.bench_function("HashMap<Mmap> - get", |b| {
            b.iter(|| {
                for (k, _) in data.iter() {
                    let _ = hash_map_get.get(black_box(k));
                }
            })
        });

        // --- Sled DB ---
        group.bench_function("Sled - insert", |b| {
            b.iter_with_setup(
                || {
                    let dir = tempdir().unwrap();
                    let db = sled::open(dir.path()).unwrap();
                    (db, dir)
                },
                |(db, _dir)| {
                    for (k, v) in data.iter() {
                        let key_bytes = k.to_le_bytes();
                        db.insert(black_box(&key_bytes), black_box(v.as_slice()))
                            .unwrap();
                    }
                    db.flush().unwrap();
                },
            )
        });

        let sled_dir_get = tempdir().unwrap();
        let sled_db_get = sled::open(sled_dir_get.path()).unwrap();
        for (k, v) in data.iter() {
            let key_bytes = k.to_le_bytes();
            sled_db_get.insert(key_bytes, v.as_slice()).unwrap();
        }
        sled_db_get.flush().unwrap();
        group.bench_function("Sled - get", |b| {
            b.iter(|| {
                for (k, _) in data.iter() {
                    let key_bytes = k.to_le_bytes();
                    black_box(sled_db_get.get(black_box(&key_bytes)).unwrap());
                }
            })
        });
    }
}

criterion_group!(benches, benchmark_u64_key_hash_map);
criterion_main!(benches);
