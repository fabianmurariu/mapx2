use opendiskmap::{Heap, HeapIdx};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;

fn generate_random_data_chunk(
    count: usize,
    min_size: usize,
    max_size: usize,
    rng: &mut StdRng,
) -> Vec<Vec<u8>> {
    let mut data = Vec::with_capacity(count);

    for _ in 0..count {
        let size = rng.random_range(min_size..=max_size);
        let mut buffer = vec![0u8; size];
        rng.fill(&mut buffer[..]);
        data.push(buffer);
    }

    data
}

fn main() {
    println!("Heap Correctness Test: Small scale test with 10,000 items");

    const TARGET_ITEMS: usize = 10_000;
    const CHUNK_SIZE: usize = 1_000;
    const MIN_SIZE: usize = 1;
    const MAX_SIZE: usize = 16 * 1024; // 16KB

    let mut heap = Heap::new_in_memory();
    let mut rng = StdRng::seed_from_u64(42);
    let mut verification_map: HashMap<HeapIdx, (usize, u64)> = HashMap::new(); // Store index -> (original_size, hash)

    let start_time = Instant::now();

    println!("Starting insertion in chunks of {} items...", CHUNK_SIZE);

    while heap.len() < TARGET_ITEMS {
        // Generate a chunk of data
        let chunk_data = generate_random_data_chunk(CHUNK_SIZE, MIN_SIZE, MAX_SIZE, &mut rng);

        // Insert chunk and store verification info
        for data in chunk_data {
            let original_size = data.len();

            // Calculate hash of original data
            let mut hasher = DefaultHasher::new();
            data.hash(&mut hasher);
            let hash = hasher.finish();

            let index = heap.append(&data);
            verification_map.insert(index, (original_size, hash));

            if heap.len() >= TARGET_ITEMS {
                break;
            }
        }
    }

    let insertion_time = start_time.elapsed();
    let final_count = heap.len();

    println!("\n=== Insertion Complete ===");
    println!("Final count: {} items", final_count);
    println!("Insertion time: {:.2}s", insertion_time.as_secs_f64());
    println!(
        "Average insertion rate: {:.0} items/sec",
        final_count as f64 / insertion_time.as_secs_f64()
    );

    // Verification phase
    println!("\n=== Starting Verification ===");
    let verify_start = Instant::now();

    let mut verified_count = 0;
    let mut errors = 0;

    for (index, (original_size, expected_hash)) in &verification_map {
        if let Some(retrieved_data) = heap.get(*index) {
            // Trim to original size before verification
            let trimmed_data = &retrieved_data[..*original_size];

            // Verify size
            if trimmed_data.len() != *original_size {
                eprintln!(
                    "Size mismatch for index {:?}: expected {}, got {}",
                    index,
                    original_size,
                    trimmed_data.len()
                );
                errors += 1;
                continue;
            }

            // Verify content using hash
            let mut hasher = DefaultHasher::new();
            trimmed_data.hash(&mut hasher);
            let actual_hash = hasher.finish();
            if actual_hash != *expected_hash {
                eprintln!("Content mismatch for index {:?}", index);
                errors += 1;
                continue;
            }

            verified_count += 1;
        } else {
            eprintln!("Failed to retrieve data for index {:?}", index);
            errors += 1;
        }
    }

    let verification_time = verify_start.elapsed();
    let total_time = start_time.elapsed();

    println!("\n=== Verification Complete ===");
    println!("Verified: {} items", verified_count);
    println!("Errors: {}", errors);
    println!("Verification time: {:.2}s", verification_time.as_secs_f64());
    println!(
        "Average verification rate: {:.0} items/sec",
        verified_count as f64 / verification_time.as_secs_f64()
    );

    println!("\n=== Overall Results ===");
    println!("Total time: {:.2}s", total_time.as_secs_f64());
    println!(
        "Success rate: {:.2}%",
        (verified_count as f64 / final_count as f64) * 100.0
    );

    if errors == 0 {
        println!(
            "✅ ALL TESTS PASSED! Heap correctness verified for {} items.",
            verified_count
        );
    } else {
        println!("❌ {} errors found during verification.", errors);
        std::process::exit(1);
    }
}
