use criterion::{Criterion, black_box, criterion_group, criterion_main};
use diskmap::byte_store::{MMapFile, VecStore};
use diskmap::raw_map::OpenHashMap;
use rustc_hash::FxBuildHasher;
use sled;
use std::collections::HashMap;
use tempfile::tempdir;

// Type alias for our Vec-backed OpenHashMap
type OpenHashMapVec<K, V> = OpenHashMap<K, V, VecStore, VecStore, VecStore, FxBuildHasher>;
// Type alias for our Mmap-backed OpenHashMap
type OpenHashMapMmap<K, V> = OpenHashMap<K, V, MMapFile, MMapFile, MMapFile, FxBuildHasher>;

/// Generates a vector of key-value pairs for benchmarking.
fn generate_data(size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..size)
        .map(|i| {
            let key = format!("key-{i}").into_bytes();
            let value = format!("value-{i}").into_bytes();
            (key, value)
        })
        .collect()
}

fn benchmark_hash_map_comparisons(c: &mut Criterion) {
    let mut group = c.benchmark_group("HashMap vs OpenHashMap");
    let data = generate_data(10000);

    // --- std::collections::HashMap ---
    // group.bench_function("std::HashMap - insert", |b| {
    //     b.iter(|| {
    //         let mut map = HashMap::new();
    //         for (k, v) in data.iter() {
    //             map.insert(black_box(k.clone()), black_box(v.clone()));
    //         }
    //     })
    // });

    // let mut std_map = HashMap::new();
    // for (k, v) in data.iter() {
    //     std_map.insert(k.clone(), v.clone());
    // }
    // group.bench_function("std::HashMap - get", |b| {
    //     b.iter(|| {
    //         for (k, _) in data.iter() {
    //             std_map.get(black_box(k));
    //         }
    //     })
    // });

    // // --- OpenHashMap with Vec<u8> backing ---
    // group.bench_function("OpenHashMap<Vec> - insert", |b| {
    //     b.iter(|| {
    //         let mut map: OpenHashMapVec<Vec<u8>, Vec<u8>> =
    //             OpenHashMap::new(VecStore::new(), VecStore::new(), VecStore::new());
    //         for (k, v) in data.iter() {
    //             map.insert(black_box(k.clone()), black_box(v.clone()));
    //         }
    //     })
    // });

    // let mut ohm_vec_map: OpenHashMapVec<Vec<u8>, Vec<u8>> =
    //     OpenHashMap::new(VecStore::new(), VecStore::new(), VecStore::new());
    // for (k, v) in data.iter() {
    //     ohm_vec_map.insert(k.clone(), v.clone());
    // }
    // group.bench_function("OpenHashMap<Vec> - get", |b| {
    //     b.iter(|| {
    //         for (k, _) in data.iter() {
    //             ohm_vec_map.get(black_box(k));
    //         }
    //     })
    // });

    // --- OpenHashMap with MmapFile backing ---
    let dir = tempdir().unwrap();
    let entry_path = dir.path().join("entries.mmap");
    let keys_path = dir.path().join("keys.mmap");
    let values_path = dir.path().join("values.mmap");
    let mmap_size = 2048 * 1024 * 100; // 10 MB

    // Recreate files for each iteration to start fresh
    let entry_store = MMapFile::new(entry_path.clone(), mmap_size).unwrap();
    let keys_store = MMapFile::new(keys_path.clone(), mmap_size).unwrap();
    let values_store = MMapFile::new(values_path.clone(), mmap_size).unwrap();

    let mut map: OpenHashMapMmap<Vec<u8>, Vec<u8>> =
        OpenHashMap::new(entry_store, keys_store, values_store);

    group.bench_function("OpenHashMap<Mmap> - insert", |b| {
        b.iter(|| {
            for (k, v) in data.iter() {
                map.insert(black_box(k.clone()), black_box(v.clone()));
            }
        })
    });

    let entry_store = MMapFile::new(entry_path.clone(), mmap_size).unwrap();
    let keys_store = MMapFile::new(keys_path.clone(), mmap_size).unwrap();
    let values_store = MMapFile::new(values_path.clone(), mmap_size).unwrap();
    let mut ohm_mmap_map: OpenHashMapMmap<Vec<u8>, Vec<u8>> =
        OpenHashMap::new(entry_store, keys_store, values_store);
    for (k, v) in data.iter() {
        ohm_mmap_map.insert(k.clone(), v.clone());
    }
    group.bench_function("OpenHashMap<Mmap> - get", |b| {
        b.iter(|| {
            for (k, _) in data.iter() {
                ohm_mmap_map.get(black_box(k));
            }
        })
    });

    let dir = tempdir().unwrap();
    let db = sled::open(dir.path()).unwrap();
    // --- Sled DB ---
    group.bench_function("Sled - insert", |b| {
        b.iter(|| {
            for (k, v) in data.iter() {
                db.insert(black_box(k.as_slice()), black_box(v.as_slice()))
                    .unwrap();
            }
            db.flush().unwrap();
        })
    });

    let sled_dir_get = tempdir().unwrap();
    let sled_db = sled::open(sled_dir_get.path()).unwrap();
    for (k, v) in data.iter() {
        sled_db.insert(k, v.as_slice()).unwrap();
    }
    sled_db.flush().unwrap();
    group.bench_function("Sled - get", |b| {
        b.iter(|| {
            for (k, _) in data.iter() {
                black_box(sled_db.get(black_box(k)).unwrap());
            }
        })
    });

    group.finish();
}

criterion_group!(benches, benchmark_hash_map_comparisons);
criterion_main!(benches);
