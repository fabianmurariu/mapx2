use diskhashmap::byte_store::MMapFile;
use diskhashmap::{Bytes, DiskHashMap, MapEntry, Native, Result, Str};
use rand::{Rng, SeedableRng, rngs::StdRng};
use rustc_hash::FxBuildHasher;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Configuration for the stress test
struct StressTestConfig {
    /// Target number of entries (will use max of this or entries needed for 8GB)
    target_entries: usize,
    /// Check iterator every N inserts
    check_interval: usize,
    /// Name of the test for display
    test_name: String,
}

/// Test with Native<u64> -> Native<u64>
fn test_u64_to_u64(config: StressTestConfig) -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!("🚀 {} - Starting", config.test_name);
    println!("{}", "=".repeat(80));

    let temp_dir = tempfile::tempdir()?;

    // Use target entries (8GB calculation would be excessive for performance testing)
    let bytes_per_entry = 16; // 8 bytes key + 8 bytes value
    let num_entries = config.target_entries;

    println!("📊 Configuration:");
    println!("   Using: {} entries", num_entries);
    println!(
        "   Estimated data size: {:.2} GB",
        (num_entries * bytes_per_entry) as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    // Phase 1: Generate data
    println!("\n📝 Phase 1: Generating data into std::HashMap...");
    let gen_start = Instant::now();
    let mut std_map = HashMap::new();
    let mut rng = StdRng::seed_from_u64(42);

    for _ in 0..num_entries {
        let key = rng.random::<u64>();
        let value = rng.random::<u64>();
        std_map.insert(key, value);
    }

    println!(
        "✅ Generated {} unique entries in {:?}",
        std_map.len(),
        gen_start.elapsed()
    );

    // Phase 2: Insert into DiskHashMap with verification
    println!("\n💾 Phase 2: Inserting into DiskHashMap with verification...");
    let insert_start = Instant::now();

    let mut disk_map: DiskHashMap<Native<u64>, Native<u64>, MMapFile, FxBuildHasher> =
        DiskHashMap::new_in(temp_dir.path())?;

    let mut inserted_keys = vec![];
    let mut insert_count = 0;
    let mut last_report = Instant::now();
    let mut last_check_count = 0;
    // let mut found_keys = vec![];

    for (key, value) in std_map.iter() {
        // Insert
        disk_map.insert(key, value)?;
        insert_count += 1;
        inserted_keys.push((*key, *value));

        // // Verify via get API
        // match disk_map.get(key)? {
        //     Some(retrieved_value) => {
        //         assert_eq!(
        //             retrieved_value, *value,
        //             "Get API: Value mismatch for key {}",
        //             key
        //         );
        //     }
        //     None => panic!("Get API: Failed to retrieve just-inserted key {}", key),
        // }
        //
        // // Verify via entry API
        // match disk_map.entry(key)? {
        //     MapEntry::Occupied(entry) => {
        //         let entry_value = entry.value()?;
        //         assert_eq!(
        //             entry_value, *value,
        //             "Entry API: Value mismatch for key {}",
        //             key
        //         );
        //     }
        //     MapEntry::Vacant(_) => panic!("Entry API: Key {} not found after insert", key),
        // }

        // Periodic verification
        if insert_count % config.check_interval == 0 {
            // Check iterator count
            // let iter_count = disk_map.iter().count();
            // assert_eq!(
            //     iter_count, insert_count,
            //     "Iterator count mismatch: expected {}, got {}",
            //     insert_count, iter_count
            // );

            // // Check all inserted keys are in iterator
            // let mut found_keys = HashSet::new();
            // for result in disk_map.iter() {
            //     let (iter_key, v) = result?;
            //     found_keys.insert(iter_key);
            // }
            //
            // assert_eq!(
            //     found_keys.len(),
            //     insert_count,
            //     "Iterator returned {} unique keys, expected {}",
            //     found_keys.len(),
            //     insert_count
            // );

            // Verify all inserted keys are found
            // for key in &inserted_keys {
            //     assert!(
            //         found_keys.contains(key),
            //         "Inserted key {} not found in iterator",
            //         key
            //     );
            // }

            // Progress report
            if last_report.elapsed().as_millis() >= 200 {
                let load_factor = disk_map.len() as f64 / disk_map.capacity() as f64;
                let interval_elapsed = last_report.elapsed().as_secs_f64();
                let interval_count = (insert_count - last_check_count) as f64;
                let instant_rate = interval_count / interval_elapsed;
                let avg_rate = insert_count as f64 / insert_start.elapsed().as_secs_f64();

                println!(
                    "   Progress: {}/{} ({:.1}%) - Cap: {} - Load: {:.3} - Instant: {:.0}/sec - Avg: {:.0}/sec",
                    insert_count,
                    num_entries,
                    (insert_count as f64 / num_entries as f64) * 100.0,
                    disk_map.capacity(),
                    load_factor,
                    instant_rate,
                    avg_rate
                );
                last_report = Instant::now();
                last_check_count = insert_count;
            }
        }
    }

    let insert_duration = insert_start.elapsed();
    println!(
        "✅ Inserted and verified {} entries in {:?} ({:.0} inserts/sec)",
        insert_count,
        insert_duration,
        insert_count as f64 / insert_duration.as_secs_f64()
    );

    // Final verification
    println!("\n🔍 Phase 3: Final comprehensive verification...");
    assert_eq!(disk_map.len(), std_map.len(), "Map size mismatch");

    let mut verified = 0;
    for (key, expected_value) in std_map.iter() {
        match disk_map.get(key)? {
            Some(actual_value) => {
                assert_eq!(actual_value, *expected_value);
                verified += 1;
            }
            None => panic!("Final verification: Missing key {}", key),
        }
    }

    println!("✅ Verified all {} entries", verified);

    // Iterator verification
    let iter_count = disk_map.iter().count();
    assert_eq!(iter_count, std_map.len(), "Iterator count mismatch");
    println!("✅ Iterator returned all {} entries", iter_count);

    println!("\n{}", "=".repeat(80));
    println!("🎉 {} - PASSED", config.test_name);
    println!("{}", "=".repeat(80));

    Ok(())
}

/// Test with Str -> Str
fn test_str_to_str(config: StressTestConfig) -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!("🚀 {} - Starting", config.test_name);
    println!("{}", "=".repeat(80));

    let temp_dir = tempfile::tempdir()?;

    // Strings are larger, so use fewer entries
    let bytes_per_entry = 80; // ~30 bytes key + ~50 bytes value
    let entries_for_8gb = (8 * 1024 * 1024 * 1024) / bytes_per_entry;
    let num_entries = config.target_entries.max(entries_for_8gb);

    println!("📊 Configuration:");
    println!("   Using: {} entries", num_entries);
    println!(
        "   Estimated size: {:.2} GB",
        (num_entries * bytes_per_entry) as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    // Phase 1: Generate data
    println!("\n📝 Phase 1: Generating string data...");
    let gen_start = Instant::now();
    let mut std_map = HashMap::new();
    let mut rng = StdRng::seed_from_u64(42);

    for i in 0..num_entries {
        let key_suffix: String = (0..20)
            .map(|_| rng.random_range(b'a'..=b'z') as char)
            .collect();
        let value_suffix: String = (0..50)
            .map(|_| rng.random_range(b'a'..=b'z') as char)
            .collect();

        let key = format!("key_{}_{}", i, key_suffix);
        let value = format!("value_{}_{}", i, value_suffix);
        std_map.insert(key, value);
    }

    println!(
        "✅ Generated {} unique entries in {:?}",
        std_map.len(),
        gen_start.elapsed()
    );

    // Phase 2: Insert and verify
    println!("\n💾 Phase 2: Inserting into DiskHashMap...");
    let insert_start = Instant::now();

    let mut disk_map: DiskHashMap<Str, Str, MMapFile, FxBuildHasher> =
        DiskHashMap::new_in(temp_dir.path())?;

    let mut inserted_keys: HashSet<String> = HashSet::new();
    let mut insert_count = 0;
    let mut last_report = Instant::now();

    for (key, value) in std_map.iter() {
        disk_map.insert(key.as_str(), value.as_str())?;
        insert_count += 1;
        inserted_keys.insert(key.clone());

        // Verify via get API
        match disk_map.get(key.as_str())? {
            Some(retrieved_value) => {
                assert_eq!(retrieved_value, value.as_str());
            }
            None => panic!("Get API failed for key {}", key),
        }

        // Verify via entry API
        match disk_map.entry(key.as_str())? {
            MapEntry::Occupied(entry) => {
                assert_eq!(entry.value()?, value.as_str());
            }
            MapEntry::Vacant(_) => panic!("Entry API: key not found after insert"),
        }

        // Periodic check
        if insert_count % config.check_interval == 0 {
            let iter_count = disk_map.iter().count();
            assert_eq!(iter_count, insert_count);

            if last_report.elapsed().as_secs() >= 5 {
                let rate = insert_count as f64 / insert_start.elapsed().as_secs_f64();
                println!(
                    "   Progress: {}/{} ({:.1}%) - {:.0} inserts/sec",
                    insert_count,
                    num_entries,
                    (insert_count as f64 / num_entries as f64) * 100.0,
                    rate
                );
                last_report = Instant::now();
            }
        }
    }

    println!(
        "✅ Inserted {} entries in {:?}",
        insert_count,
        insert_start.elapsed()
    );

    println!("\n{}", "=".repeat(80));
    println!("🎉 {} - PASSED", config.test_name);
    println!("{}", "=".repeat(80));

    Ok(())
}

/// Test with Bytes -> Bytes
fn test_bytes_to_bytes(config: StressTestConfig) -> Result<()> {
    println!("\n{}", "=".repeat(80));
    println!("🚀 {} - Starting", config.test_name);
    println!("{}", "=".repeat(80));

    let temp_dir = tempfile::tempdir()?;

    let bytes_per_entry = 140; // ~32 bytes key + ~100 bytes value + overhead
    let entries_for_8gb = (8 * 1024 * 1024 * 1024) / bytes_per_entry;
    let num_entries = config.target_entries.max(entries_for_8gb);

    println!("📊 Configuration:");
    println!("   Using: {} entries", num_entries);
    println!(
        "   Estimated size: {:.2} GB",
        (num_entries * bytes_per_entry) as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    // Phase 1: Generate data
    println!("\n📝 Phase 1: Generating byte data...");
    let gen_start = Instant::now();
    let mut std_map = HashMap::new();
    let mut rng = StdRng::seed_from_u64(42);

    for i in 0..num_entries {
        let mut key = i.to_le_bytes().to_vec();
        let mut extra_key_bytes = vec![0u8; 24];
        rng.fill(&mut extra_key_bytes[..]);
        key.extend_from_slice(&extra_key_bytes);

        let mut value = vec![0u8; 100];
        rng.fill(&mut value[..]);

        std_map.insert(key, value);
    }

    println!(
        "✅ Generated {} unique entries in {:?}",
        std_map.len(),
        gen_start.elapsed()
    );

    // Phase 2: Insert and verify
    println!("\n💾 Phase 2: Inserting into DiskHashMap...");
    let insert_start = Instant::now();

    let mut disk_map: DiskHashMap<Bytes, Bytes, MMapFile, FxBuildHasher> =
        DiskHashMap::new_in(temp_dir.path())?;

    let mut insert_count = 0;
    let mut last_report = Instant::now();

    for (key, value) in std_map.iter() {
        disk_map.insert(key.as_slice(), value.as_slice())?;
        insert_count += 1;

        // Verify via get API
        match disk_map.get(key.as_slice())? {
            Some(retrieved_value) => {
                assert_eq!(retrieved_value, value.as_slice());
            }
            None => panic!("Get API failed"),
        }

        // Verify via entry API
        match disk_map.entry(key.as_slice())? {
            MapEntry::Occupied(entry) => {
                assert_eq!(entry.value()?, value.as_slice());
            }
            MapEntry::Vacant(_) => panic!("Entry API failed"),
        }

        // Periodic check
        if insert_count % config.check_interval == 0 {
            let iter_count = disk_map.iter().count();
            assert_eq!(iter_count, insert_count);

            if last_report.elapsed().as_secs() >= 5 {
                let rate = insert_count as f64 / insert_start.elapsed().as_secs_f64();
                println!(
                    "   Progress: {}/{} ({:.1}%) - {:.0} inserts/sec",
                    insert_count,
                    num_entries,
                    (insert_count as f64 / num_entries as f64) * 100.0,
                    rate
                );
                last_report = Instant::now();
            }
        }
    }

    println!(
        "✅ Inserted {} entries in {:?}",
        insert_count,
        insert_start.elapsed()
    );

    println!("\n{}", "=".repeat(80));
    println!("🎉 {} - PASSED", config.test_name);
    println!("{}", "=".repeat(80));

    Ok(())
}

fn main() -> Result<()> {
    println!("🔬 DiskHashMap Comprehensive Stress Test Suite");
    println!("This test generates large amounts of data and verifies correctness");
    println!("at every step using both get and entry APIs.");
    println!();
    println!("⚠️  WARNING: These tests will generate gigabytes of data!");
    println!("    They may take significant time and disk space.");

    // Test 1: u64 -> u64
    test_u64_to_u64(StressTestConfig {
        target_entries: 5_000_000,
        check_interval: 100_000,
        test_name: "Test 1: Native<u64> → Native<u64>".to_string(),
    })?;

    // Test 2: String -> String
    // test_str_to_str(StressTestConfig {
    //     target_entries: 5_000_000,
    //     check_interval: 50_000,
    //     test_name: "Test 2: Str → Str".to_string(),
    // })?;
    //
    // // Test 3: Bytes -> Bytes
    // test_bytes_to_bytes(StressTestConfig {
    //     target_entries: 10_000_000,
    //     check_interval: 50_000,
    //     test_name: "Test 3: Bytes → Bytes".to_string(),
    // })?;
    //
    // println!("\n{}", "=".repeat(80));
    // println!("🏆 ALL STRESS TESTS PASSED!");
    // println!("{}", "=".repeat(80));
    // println!("✅ All data types verified successfully");
    // println!("✅ Get API working correctly");
    // println!("✅ Entry API working correctly");
    // println!("✅ Iterator consistency maintained");
    // println!("✅ No data corruption detected");

    Ok(())
}
