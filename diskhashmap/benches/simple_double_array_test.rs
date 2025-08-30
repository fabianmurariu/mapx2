use diskhashmap::disk_map::DiskHashMap;
use diskhashmap::types::Native;
use diskhashmap::VecStore;
use rustc_hash::FxBuildHasher;
use std::time::Instant;

type SingleU64U64 = DiskHashMap<Native<u64>, Native<u64>, VecStore, FxBuildHasher>;
type DoubleU64U64 = DiskHashMap<Native<u64>, Native<u64>, VecStore, FxBuildHasher>;

fn main() {
    println!("=== Double Array vs Single Array Performance Test ===");
    
    let test_sizes = [1000, 5000, 10000];
    
    for size in test_sizes {
        println!("\n--- Testing with {} entries ---", size);
        
        // Generate test data
        let data: Vec<(u64, u64)> = (0..size as u64)
            .map(|i| (i, i * 2))
            .collect();
        
        // Test single array
        let start = Instant::now();
        let mut single_map = SingleU64U64::new();
        let mut single_max_insert = std::time::Duration::ZERO;
        
        for (k, v) in &data {
            let insert_start = Instant::now();
            single_map.insert(k, v).unwrap();
            let insert_duration = insert_start.elapsed();
            single_max_insert = single_max_insert.max(insert_duration);
        }
        let single_total = start.elapsed();
        
        // Test double array
        let start = Instant::now();
        let mut double_map = DoubleU64U64::new_with_double_array();
        let mut double_max_insert = std::time::Duration::ZERO;
        
        for (k, v) in &data {
            let insert_start = Instant::now();
            double_map.insert(k, v).unwrap();
            let insert_duration = insert_start.elapsed();
            double_max_insert = double_max_insert.max(insert_duration);
        }
        let double_total = start.elapsed();
        
        println!("Single Array:");
        println!("  Total time: {:?}", single_total);
        println!("  Max insert: {:?}", single_max_insert);
        println!("  Avg insert: {:?}", single_total / size);
        
        println!("Double Array:");
        println!("  Total time: {:?}", double_total);
        println!("  Max insert: {:?}", double_max_insert);  
        println!("  Avg insert: {:?}", double_total / size);
        
        println!("Improvements:");
        println!("  Total time: {:.2}x faster", single_total.as_nanos() as f64 / double_total.as_nanos() as f64);
        println!("  Max insert: {:.2}x faster", single_max_insert.as_nanos() as f64 / double_max_insert.as_nanos() as f64);
        
        // Test lookups
        println!("\nLookup Performance:");
        
        let start = Instant::now();
        for (k, _) in &data {
            single_map.get(k).unwrap();
        }
        let single_lookup = start.elapsed();
        
        let start = Instant::now();
        for (k, _) in &data {
            double_map.get(k).unwrap();
        }
        let double_lookup = start.elapsed();
        
        println!("  Single array lookup: {:?}", single_lookup);
        println!("  Double array lookup: {:?}", double_lookup);
        println!("  Lookup ratio: {:.2}x", double_lookup.as_nanos() as f64 / single_lookup.as_nanos() as f64);
    }
}