use criterion::{Criterion, black_box, criterion_group, criterion_main};
use diskmap::byte_store::{MMapFile, VecStore};
use diskmap::raw_map::OpenHashMap;
use rand::{Rng, distr::Alphanumeric};
use rustc_hash::FxBuildHasher;
use tempfile::tempdir;

// Type alias for our Vec-backed OpenHashMap
type OpenHashMapVec<K, V> = OpenHashMap<K, V, VecStore, VecStore, VecStore, FxBuildHasher>;
// Type alias for our Mmap-backed OpenHashMap
type OpenHashMapMmap<K, V> = OpenHashMap<K, V, MMapFile, MMapFile, MMapFile, FxBuildHasher>;

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

        // --- OpenHashMap with MmapFile backing ---
        let dir = tempdir().unwrap();
        let entry_path = dir.path().join("entries.mmap");
        let keys_path = dir.path().join("keys.mmap");
        let values_path = dir.path().join("values.mmap");
        let mmap_size = size as u64 * 20 + 1024 * 1024; // Generous sizing

        group.bench_function("OpenHashMap<Mmap> - insert", |b| {
            b.iter_with_setup(
                || {
                    // Recreate files for each iteration to start fresh
                    let entry_store =
                        MMapFile::new(entry_path.clone(), mmap_size.try_into().unwrap()).unwrap();
                    let keys_store =
                        MMapFile::new(keys_path.clone(), mmap_size.try_into().unwrap()).unwrap();
                    let values_store =
                        MMapFile::new(values_path.clone(), mmap_size.try_into().unwrap()).unwrap();
                    OpenHashMap::new(entry_store, keys_store, values_store)
                },
                |mut map: OpenHashMapMmap<Vec<u8>, Vec<u8>>| {
                    for (k, v) in data.iter() {
                        map.insert(black_box(k.clone()), black_box(v.clone()));
                    }
                },
            );
        });

        // Setup for the get benchmark
        let entry_store_get = MMapFile::new(entry_path, mmap_size.try_into().unwrap()).unwrap();
        let keys_store_get = MMapFile::new(keys_path, mmap_size.try_into().unwrap()).unwrap();
        let values_store_get = MMapFile::new(values_path, mmap_size.try_into().unwrap()).unwrap();
        let mut ohm_mmap_map_get: OpenHashMapMmap<Vec<u8>, Vec<u8>> =
            OpenHashMap::new(entry_store_get, keys_store_get, values_store_get);
        for (k, v) in data.iter() {
            ohm_mmap_map_get.insert(k.clone(), v.clone());
        }
        group.bench_function("OpenHashMap<Mmap> - get", |b| {
            b.iter(|| {
                for (k, _) in data.iter() {
                    ohm_mmap_map_get.get(black_box(k));
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
