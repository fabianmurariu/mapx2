use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use opendiskmap::{Buffers, Heap, VecStore};
use rand::{Rng, SeedableRng, rngs::StdRng};

fn generate_random_data(count: usize, min_size: usize, max_size: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut data = Vec::with_capacity(count);

    for _ in 0..count {
        let size = rng.random_range(min_size..=max_size);
        let mut buffer = vec![0u8; size];
        rng.fill(&mut buffer[..]);
        data.push(buffer);
    }

    data
}

fn bench_heap_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("heap_append");

    for &count in &[1_000, 10_000, 100_000, 1_000_000] {
        let data = generate_random_data(count, 1, 16 * 1024, 42);
        let total_bytes: usize = data.iter().map(|d| d.len()).sum();

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(BenchmarkId::new("items", count), &data, |b, data| {
            b.iter(|| {
                let mut heap = Heap::new_in_memory();
                let mut indices = Vec::with_capacity(data.len());

                for item in data {
                    let index = heap.append(black_box(item));
                    indices.push(index);
                }

                black_box(indices);
            });
        });
    }

    group.finish();
}

fn bench_buffers_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffers_append");

    for &count in &[1_000, 10_000, 100_000, 1_000_000] {
        let data = generate_random_data(count, 1, 16 * 1024, 42);
        let total_bytes: usize = data.iter().map(|d| d.len()).sum();

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(BenchmarkId::new("items", count), &data, |b, data| {
            b.iter(|| {
                let store = VecStore::new();
                let mut buffers = Buffers::new(store);
                let mut indices = Vec::with_capacity(data.len());

                for item in data {
                    let index = buffers.append(black_box(item));
                    indices.push(index);
                }

                black_box(indices);
            });
        });
    }

    group.finish();
}

fn bench_heap_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("heap_get");

    for &count in &[1_000, 10_000, 100_000, 1_000_000] {
        let data = generate_random_data(count, 1, 16 * 1024, 42);
        let total_bytes: usize = data.iter().map(|d| d.len()).sum();

        // Pre-populate heap
        let mut heap = Heap::new_in_memory();
        let mut indices = Vec::with_capacity(data.len());
        for item in &data {
            let index = heap.append(item);
            indices.push(index);
        }

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("items", count),
            &(heap, indices),
            |b, (heap, indices)| {
                b.iter(|| {
                    for &index in indices {
                        let _data = heap.get(black_box(index));
                        black_box(_data);
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_buffers_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffers_get");

    for &count in &[1_000, 10_000, 100_000, 1_000_000] {
        let data = generate_random_data(count, 1, 16 * 1024, 42);
        let total_bytes: usize = data.iter().map(|d| d.len()).sum();

        // Pre-populate buffers
        let store = VecStore::new();
        let mut buffers = Buffers::new(store);
        let mut indices = Vec::with_capacity(data.len());
        for item in &data {
            let index = buffers.append(item);
            indices.push(index);
        }

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("items", count),
            &(buffers, indices),
            |b, (buffers, indices): &(Buffers<VecStore>, Vec<usize>)| {
                b.iter(|| {
                    for &index in indices {
                        let _data = buffers.get(black_box(index));
                        black_box(_data);
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_append_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_comparison");

    for &count in &[1_000, 10_000, 100_000] {
        let data = generate_random_data(count, 1, 16 * 1024, 42);
        let total_bytes: usize = data.iter().map(|d| d.len()).sum();

        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_with_input(BenchmarkId::new("heap", count), &data, |b, data| {
            b.iter(|| {
                let mut heap = Heap::new_in_memory();
                for item in data {
                    let _index = heap.append(black_box(item));
                    black_box(_index);
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("buffers", count), &data, |b, data| {
            b.iter(|| {
                let store = VecStore::new();
                let mut buffers = Buffers::new(store);
                for item in data {
                    let _index = buffers.append(black_box(item));
                    black_box(_index);
                }
            });
        });
    }

    group.finish();
}

fn bench_get_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_comparison");

    for &count in &[1_000, 10_000, 100_000] {
        let data = generate_random_data(count, 1, 16 * 1024, 42);
        let total_bytes: usize = data.iter().map(|d| d.len()).sum();

        // Pre-populate heap
        let mut heap = Heap::new_in_memory();
        let mut heap_indices = Vec::with_capacity(data.len());
        for item in &data {
            let index = heap.append(item);
            heap_indices.push(index);
        }

        // Pre-populate buffers
        let store = VecStore::new();
        let mut buffers = Buffers::new(store);
        let mut buffer_indices = Vec::with_capacity(data.len());
        for item in &data {
            let index = buffers.append(item);
            buffer_indices.push(index);
        }

        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("heap", count),
            &(heap, heap_indices),
            |b, (heap, indices)| {
                b.iter(|| {
                    for &index in indices {
                        let _data = heap.get(black_box(index));
                        black_box(_data);
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("buffers", count),
            &(buffers, buffer_indices),
            |b, (buffers, indices): &(Buffers<VecStore>, Vec<usize>)| {
                b.iter(|| {
                    for &index in indices {
                        let _data = buffers.get(black_box(index));
                        black_box(_data);
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_memory_efficiency(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_efficiency");

    // Test with different size distributions
    let size_distributions = [
        ("small", 1, 64),
        ("medium", 64, 1024),
        ("large", 1024, 16384),
        ("mixed", 1, 16384),
    ];

    for (name, min_size, max_size) in size_distributions.iter() {
        let data = generate_random_data(10_000, *min_size, *max_size, 42);
        let total_bytes: usize = data.iter().map(|d| d.len()).sum();

        group.throughput(Throughput::Bytes(total_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new(format!("heap_{}", name), 10_000),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut heap = Heap::new_in_memory();
                    for item in data {
                        let _index = heap.append(black_box(item));
                        black_box(_index);
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new(format!("buffers_{}", name), 10_000),
            &data,
            |b, data| {
                b.iter(|| {
                    let store = VecStore::new();
                    let mut buffers = Buffers::new(store);
                    for item in data {
                        let _index = buffers.append(black_box(item));
                        black_box(_index);
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_heap_append,
    bench_buffers_append,
    bench_heap_get,
    bench_buffers_get,
    bench_append_comparison,
    bench_get_comparison,
    bench_memory_efficiency
);
criterion_main!(benches);
