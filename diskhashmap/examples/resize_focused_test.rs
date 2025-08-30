use diskhashmap::disk_map::DiskHashMap;
use diskhashmap::types::Native;
use diskhashmap::VecStore;
use rustc_hash::FxBuildHasher;
use std::time::Instant;

type SingleU64U64 = DiskHashMap<Native<u64>, Native<u64>, VecStore, FxBuildHasher>;
type DoubleU64U64 = DiskHashMap<Native<u64>, Native<u64>, VecStore, FxBuildHasher>;

fn measure_resize_behavior(size: usize, map_type: &str, create_map: impl Fn() -> Box<dyn FnMut(u64, u64) -> Result<(), Box<dyn std::error::Error>>>) {
    println!("\n=== {} with {} entries ===", map_type, size);
    
    let mut insert_fn = create_map();
    let mut insert_times = Vec::new();
    let mut resize_count = 0;
    let _last_capacity = 0u64;
    
    let start = Instant::now();
    
    for i in 0..size as u64 {
        let insert_start = Instant::now();
        insert_fn(i, i * 2).unwrap();
        let insert_duration = insert_start.elapsed();
        insert_times.push(insert_duration);
        
        // Track capacity changes (resize events)
        // Note: We can't easily get capacity from the closure, so we'll estimate
        if insert_duration.as_micros() > 100 {  // Assume slow inserts indicate resize
            resize_count += 1;
        }
    }
    
    let total_time = start.elapsed();
    
    // Sort insert times to find percentiles
    insert_times.sort();
    let len = insert_times.len();
    let p50 = insert_times[len / 2];
    let p95 = insert_times[(len as f64 * 0.95) as usize];
    let p99 = insert_times[(len as f64 * 0.99) as usize];
    let max = insert_times[len - 1];
    
    println!("Total time: {:?}", total_time);
    println!("Avg insert: {:?}", total_time / size as u32);
    println!("P50 insert: {:?}", p50);
    println!("P95 insert: {:?}", p95);
    println!("P99 insert: {:?}", p99);
    println!("Max insert: {:?}", max);
    println!("Estimated resizes: {}", resize_count);
}

fn main() {
    println!("=== Resize-Focused Performance Comparison ===");
    println!("Testing with larger datasets to trigger multiple resize operations");
    
    let test_sizes = [50_000, 100_000];
    
    for size in test_sizes {
        // Test single array
        measure_resize_behavior(size, "Single Array", || {
            let mut map = SingleU64U64::new();
            Box::new(move |k: u64, v: u64| -> Result<(), Box<dyn std::error::Error>> {
                map.insert(&k, &v)?;
                Ok(())
            })
        });
        
        // Test double array  
        measure_resize_behavior(size, "Double Array", || {
            let mut map = DoubleU64U64::new_with_double_array();
            Box::new(move |k: u64, v: u64| -> Result<(), Box<dyn std::error::Error>> {
                map.insert(&k, &v)?;
                Ok(())
            })
        });
        
        println!("\n{}", "-".repeat(60));
    }
    
    println!("\n=== String Key Performance Test ===");
    println!("Testing with string keys to add allocation overhead");
    
    use diskhashmap::types::Str;
    type SingleStrU64 = DiskHashMap<Str, Native<u64>, VecStore, FxBuildHasher>;
    type DoubleStrU64 = DiskHashMap<Str, Native<u64>, VecStore, FxBuildHasher>;
    
    let string_size = 10_000;
    let test_data: Vec<String> = (0..string_size)
        .map(|i| format!("test_key_{:06}", i))
        .collect();
    
    // Single array with strings
    let start = Instant::now();
    let mut single_str_map = SingleStrU64::new();
    let mut single_max = std::time::Duration::ZERO;
    
    for (i, key) in test_data.iter().enumerate() {
        let insert_start = Instant::now();
        single_str_map.insert(key.as_str(), &(i as u64)).unwrap();
        let duration = insert_start.elapsed();
        single_max = single_max.max(duration);
    }
    let single_str_total = start.elapsed();
    
    // Double array with strings
    let start = Instant::now();
    let mut double_str_map = DoubleStrU64::new_with_double_array();
    let mut double_max = std::time::Duration::ZERO;
    
    for (i, key) in test_data.iter().enumerate() {
        let insert_start = Instant::now();
        double_str_map.insert(key.as_str(), &(i as u64)).unwrap();
        let duration = insert_start.elapsed();
        double_max = double_max.max(duration);
    }
    let double_str_total = start.elapsed();
    
    println!("String Key Results:");
    println!("Single Array - Total: {:?}, Max: {:?}", single_str_total, single_max);
    println!("Double Array - Total: {:?}, Max: {:?}", double_str_total, double_max);
    
    if double_max.as_nanos() > 0 {
        println!("Max insert improvement: {:.2}x", single_max.as_nanos() as f64 / double_max.as_nanos() as f64);
    }
    if double_str_total.as_nanos() > 0 {
        println!("Total time ratio: {:.2}x", single_str_total.as_nanos() as f64 / double_str_total.as_nanos() as f64);
    }
}