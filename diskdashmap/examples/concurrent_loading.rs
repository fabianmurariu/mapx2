use diskdashmap::DiskDashMap;
use opendiskmap::{
    DiskMapError, MMapFile,
    types::{Native, Str},
};
use rand::distr::Alphanumeric;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;
use rustc_hash::FxBuildHasher;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

fn generate_random_string(rng: &mut ChaCha8Rng, length: usize) -> String {
    (0..length)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line argument for number of items
    let args: Vec<String> = env::args().collect();
    let n_items = if args.len() >= 2 {
        args[1].parse::<usize>().unwrap_or(10000)
    } else {
        10000
    };

    println!("Generating {n_items} random key-value pairs...");

    // Generate random HashMap with N items
    let mut rng = ChaCha8Rng::seed_from_u64(42); // Use deterministic seed for reproducibility
    let mut std_map = HashMap::<String, usize>::new();

    let start = Instant::now();
    while std_map.len() < n_items {
        let i = rng.random_range(0..100_000_000); // Random values between 0 and 99,999
        let key = generate_random_string(&mut rng, 36); // 20 character keys
        std_map.insert(key, i);
    }
    let generation_time = start.elapsed();
    println!("Generated {} items in {:?}", std_map.len(), generation_time);

    // Create temporary directory for DiskDashMap
    let temp_dir = TempDir::new()?;
    println!("Using temporary directory: {:?}", temp_dir.path());

    // Create DiskDashMap
    let disk_map: Arc<DiskDashMap<Str, Native<usize>, MMapFile, FxBuildHasher>> = Arc::new(
        DiskDashMap::new_with_capacity(temp_dir.path(), 2_000_000, 32768, Some(100 * 1024 * 1024))?,
    );

    // Convert std_map to Vec for concurrent access
    let items: Vec<(String, usize)> = std_map.iter().map(|(k, v)| (k.clone(), *v)).collect();

    let start = Instant::now();

    items.par_iter().try_for_each(|(k, v)| {
        disk_map.insert(k, v)?;
        let act = disk_map.get(k)?.map(|x| x.value().unwrap()).unwrap();
        if act != *v {
            return Err(DiskMapError::KeyNotFound);
        }
        Ok(())
    })?;
    let shard_lens = disk_map
        .shards()
        .iter()
        .map(|shard| shard.read().len())
        .collect::<Vec<_>>();
    println!("Concurrent loading complete. Shard lengths: {shard_lens:?}");

    let loading_time = start.elapsed();
    println!("Loaded {}, items in {:?}", disk_map.len(), loading_time);
    let items_per_micro = disk_map.len() as u128 / loading_time.as_micros();
    let items_per_second = items_per_micro * 1_000_000;
    println!("Loading throughput: {items_per_second} items/second");

    // Final verification: go over all values from std HashMap and check they exist in DiskDashMap
    println!("Performing final verification...");
    let start = Instant::now();

    let mut verified_final = 0;
    let mut missing_keys = Vec::new();
    let mut value_mismatches = Vec::new();

    for (key, expected_value) in &std_map {
        match disk_map.get(key) {
            Ok(Some(retrieved_ref)) => match retrieved_ref.value() {
                Ok(retrieved_value) => {
                    if retrieved_value == *expected_value {
                        verified_final += 1;
                    } else {
                        value_mismatches.push((key.clone(), *expected_value, retrieved_value));
                    }
                }
                Err(e) => {
                    eprintln!("Error retrieving value for key '{key}': {e}");
                }
            },
            Ok(None) => {
                missing_keys.push(key.clone());
            }
            Err(e) => {
                eprintln!("Error looking up key '{key}': {e}");
            }
        }
    }

    let verification_time = start.elapsed();

    // Report results
    println!("\n=== Final Results ===");
    println!("Original HashMap size: {}", std_map.len());
    println!("DiskDashMap size: {}", disk_map.len());
    println!("Successfully verified: {verified_final}");
    println!("Missing keys: {}", missing_keys.len());
    println!("Value mismatches: {}", value_mismatches.len());
    println!("Final verification time: {verification_time:?}");

    if !missing_keys.is_empty() {
        println!(
            "First 10 missing keys: {:?}",
            &missing_keys[..missing_keys.len().min(10)]
        );
    }

    if !value_mismatches.is_empty() {
        println!("First 10 value mismatches:");
        for (key, expected, actual) in value_mismatches.iter().take(10) {
            println!("  Key '{key}': expected {expected}, got {actual}");
        }
    }

    // Performance summary
    println!("\n=== Performance Summary ===");
    println!("HashMap generation: {generation_time:?}");
    println!("Concurrent loading: {loading_time:?}");
    println!("Final verification: {verification_time:?}");
    println!(
        "Total time: {:?}",
        generation_time + loading_time + verification_time
    );

    let items_per_second = n_items as f64 / loading_time.as_secs_f64();
    println!("Loading throughput: {items_per_second:.0} items/second");

    if verified_final == std_map.len() && missing_keys.is_empty() && value_mismatches.is_empty() {
        println!("\n✅ SUCCESS: All items were correctly loaded and verified!");
    } else {
        println!("\n❌ FAILURE: Some items were not correctly loaded or verified.");
        return Err("Verification failed".into());
    }
    Ok(())
}
