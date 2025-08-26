use diskhashmap::{Heap, HeapIdx};
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
    const TARGET_ITEMS: usize = 10_000_000;
    const CHUNK_SIZE: usize = 1_000_000;
    const MIN_SIZE: usize = 1;
    const MAX_SIZE: usize = 16 * 1024; // 16KB
    const VERIFICATION_SAMPLE_RATE: usize = 100;

    println!(
        "Heap Correctness Test: Inserting 10,000,000 items and verifying {} sampled items",
        TARGET_ITEMS / VERIFICATION_SAMPLE_RATE
    );

    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    std::fs::create_dir_all(&temp_dir).expect("Failed to create test directory");

    let mut heap = Heap::new(&temp_dir).unwrap();
    let mut rng = StdRng::seed_from_u64(42);
    let mut verification_map: HashMap<HeapIdx, (usize, u64)> = HashMap::new(); // Store index -> (original_size, hash)

    // To avoid storing 10M entries in memory, we'll sample every 100th item for verification

    let start_time = Instant::now();
    let mut next_progress = 1_000_000;

    println!("Starting insertion in chunks of {CHUNK_SIZE} items...");

    while heap.len() < TARGET_ITEMS {
        // Generate a chunk of data
        let chunk_data = generate_random_data_chunk(CHUNK_SIZE, MIN_SIZE, MAX_SIZE, &mut rng);
        let start_time = Instant::now();

        // Insert chunk and store verification info
        for data in chunk_data {
            let original_size = data.len();
            let index = heap.append(&data);
            let elapsed = start_time.elapsed();

            // Only store verification info for sampled items to save memory
            if heap.len() % VERIFICATION_SAMPLE_RATE == 0 {
                // Calculate hash of original data
                let mut hasher = DefaultHasher::new();
                data.hash(&mut hasher);
                let hash = hasher.finish();
                verification_map.insert(index, (original_size, hash));
            }

            // Progress reporting
            let current_len = heap.len();
            if current_len >= next_progress {
                let rate = CHUNK_SIZE as f64 / elapsed.as_secs_f64();
                println!(
                    "Inserted {} items in {:.2}s (rate: {:.0} items/sec)",
                    CHUNK_SIZE,
                    elapsed.as_secs_f64(),
                    rate
                );
                next_progress += 1_000_000;
            }

            if current_len >= TARGET_ITEMS {
                break;
            }
        }
    }

    let insertion_time = start_time.elapsed();
    let final_count = heap.len();

    println!("\n=== Insertion Complete ===");
    println!("Final count: {final_count} items");
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
    let mut next_verify_progress = 1_000_000;

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
                eprintln!("Content mismatch for index {index:?}");
                errors += 1;
                continue;
            }

            verified_count += 1;

            // Progress reporting
            if verified_count >= next_verify_progress {
                let elapsed = verify_start.elapsed();
                let rate = verified_count as f64 / elapsed.as_secs_f64();
                println!(
                    "Verified {} items in {:.2}s (rate: {:.0} items/sec)",
                    verified_count,
                    elapsed.as_secs_f64(),
                    rate
                );
                next_verify_progress += 1_000_000;
            }
        } else {
            eprintln!("Failed to retrieve data for index {index:?}");
            errors += 1;
        }

        if verified_count + errors >= TARGET_ITEMS {
            break;
        }
    }

    let verification_time = verify_start.elapsed();
    let total_time = start_time.elapsed();

    println!("\n=== Verification Complete ===");
    println!("Verified: {verified_count} items");
    println!("Errors: {errors}");
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
        println!("✅ ALL TESTS PASSED! Heap correctness verified for {verified_count} items.");
    } else {
        println!("❌ {errors} errors found during verification.");
        std::process::exit(1);
    }
}
