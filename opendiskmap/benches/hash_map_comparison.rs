use criterion::{Criterion, black_box, criterion_group, criterion_main};
use opendiskmap::disk_map::DiskHashMap;
use opendiskmap::{Bytes, byte_store::MMapFile};
use rand::{Rng, distr::Alphanumeric};
use rustc_hash::FxBuildHasher;
use tempfile::tempdir;

// Type alias for our Mmap-backed HashMap
type HashMapMmap = DiskHashMap<Bytes, Bytes, MMapFile, FxBuildHasher>;

/// Generates a vector of key-value pairs for benchmarking.
fn generate_data(size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = rand::rng();
    (0..size)
        .map(|_| {
            let key_len = rng.random_range(1..=25);
            let val_len = rng.random_range(1..=250);
            let key: Vec<u8> = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(key_len)
                .collect();
            let value = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(val_len)
                .collect();
            (key, value)
        })
        .collect()
}

use std::time::Duration;

fn benchmark_hash_map_comparisons(c: &mut Criterion) {
    for &size in &[/*10_000, 100_000, */ 1_000_000, 10_000_000] {
        let mut group = c.benchmark_group(format!("size={size}"));
        if size >= 1_000_000 {
            // Reduce sample count for large benchmarks
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(60));
        }

        let data = generate_data(size);

        // --- HashMap with MmapFile backing ---
        group.bench_function("HashMap<Mmap> - insert", |b| {
            b.iter_with_setup(
                || {
                    let dir = tempdir().unwrap();
                    let map: HashMapMmap = DiskHashMap::new_in(dir.path()).unwrap();
                    (map, dir) // Keep dir alive
                },
                |(mut map, _dir)| {
                    for (k, v) in data.iter() {
                        map.insert(k, v).unwrap();
                    }
                },
            );
        });

        // Setup for the get benchmark
        let get_dir = tempdir().unwrap();
        let mut hash_map_get: HashMapMmap = DiskHashMap::new_in(get_dir.path()).unwrap();
        for (k, v) in data.iter() {
            hash_map_get.insert(k, v).unwrap();
        }
        group.bench_function("HashMap<Mmap> - get", |b| {
            b.iter(|| {
                for (k, _) in data.iter() {
                    hash_map_get.get(k).unwrap();
                }
            })
        });

        // --- Sled DB ---
        group.bench_function("Sled - insert", |b| {
            b.iter_with_setup(
                || tempdir().unwrap(),
                |dir| {
                    let db = sled::open(dir.path()).unwrap();
                    for (k, v) in data.iter() {
                        db.insert(black_box(k.as_slice()), black_box(v.as_slice()))
                            .unwrap();
                    }
                    db.flush().unwrap();
                },
            )
        });

        let sled_dir_get = tempdir().unwrap();
        let sled_db_get = sled::open(sled_dir_get.path()).unwrap();
        for (k, v) in data.iter() {
            sled_db_get.insert(k, v.as_slice()).unwrap();
        }
        sled_db_get.flush().unwrap();
        group.bench_function("Sled - get", |b| {
            b.iter(|| {
                for (k, _) in data.iter() {
                    black_box(sled_db_get.get(black_box(k)).unwrap());
                }
            })
        });
    }
}

criterion_group!(benches, benchmark_hash_map_comparisons);
criterion_main!(benches);
