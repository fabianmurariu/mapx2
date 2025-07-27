use criterion::{Criterion, black_box, criterion_group, criterion_main};
use diskmap::raw_map::OpenHashMap;
use rand::{Rng, RngCore};
use rustc_hash::FxBuildHasher;
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
type OpenHashMapMmapU64 = OpenHashMap<
    U64,
    Vec<u8>,
    diskmap::byte_store::MMapFile,
    diskmap::byte_store::MMapFile,
    diskmap::byte_store::MMapFile,
    FxBuildHasher,
>;

/// Generates a vector of key-value pairs for benchmarking.
/// Keys are random u64 values, and values are random byte vectors.
fn generate_data(size: usize) -> Vec<(U64, Vec<u8>)> {
    let mut rng = rand::rng();
    (0..size)
        .map(|_| {
            let key = U64(rng.random::<u64>());
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

        // --- OpenHashMap with MmapFile backing ---
        group.bench_function("OpenHashMap<Mmap> - insert", |b| {
            b.iter_with_setup(
                || {
                    let dir = tempdir().unwrap();
                    let map = OpenHashMapMmapU64::new_in(dir.path()).unwrap();
                    // Keep the TempDir alive for the duration of the iteration
                    (map, dir)
                },
                |(mut map, _dir)| {
                    // _dir is dropped here, cleaning up the temp directory
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
                        db.insert(black_box(k.as_ref()), black_box(v.as_slice()))
                            .unwrap();
                    }
                    db.flush().unwrap();
                },
            )
        });

        let sled_dir_get = tempdir().unwrap();
        let sled_db_get = sled::open(sled_dir_get.path()).unwrap();
        for (k, v) in data.iter() {
            sled_db_get.insert(k.as_ref(), v.as_slice()).unwrap();
        }
        sled_db_get.flush().unwrap();
        group.bench_function("Sled - get", |b| {
            b.iter(|| {
                for (k, _) in data.iter() {
                    black_box(sled_db_get.get(black_box(k.as_ref())).unwrap());
                }
            })
        });
    }
}

criterion_group!(benches, benchmark_u64_key_hash_map);
criterion_main!(benches);
