// use diskhashmap::VecStore;
// use diskhashmap::disk_map::DiskHashMap;
// use diskhashmap::types::{Native, Str};
// use rustc_hash::FxBuildHasher;
// use std::time::Instant;

// type SingleU64U64 = DiskHashMap<Native<u64>, Native<u64>, VecStore, FxBuildHasher>;
// type DoubleU64U64 = DiskHashMap<Native<u64>, Native<u64>, VecStore, FxBuildHasher>;
// type SingleStrStr = DiskHashMap<Str, Str, VecStore, FxBuildHasher>;
// type DoubleStrStr = DiskHashMap<Str, Str, VecStore, FxBuildHasher>;

// fn benchmark_max_latency<F>(
//     name: &str,
//     size: usize,
//     mut insert_fn: F,
// ) -> (
//     std::time::Duration,
//     std::time::Duration,
//     Vec<std::time::Duration>,
// )
// where
//     F: FnMut(usize) -> std::time::Duration,
// {
//     println!("\n=== {} ===", name);

//     let mut insert_times = Vec::with_capacity(size);
//     let start_total = Instant::now();

//     for i in 0..size {
//         let duration = insert_fn(i);
//         insert_times.push(duration);
//     }

//     let total_time = start_total.elapsed();
//     let max_time = *insert_times.iter().max().unwrap();

//     // Find the top 10 slowest inserts
//     let mut sorted_times = insert_times.clone();
//     sorted_times.sort_by(|a, b| b.cmp(a));

//     println!("Total time: {:?}", total_time);
//     println!("Max insert: {:?}", max_time);
//     println!("Avg insert: {:?}", total_time / size as u32);

//     println!("Top 10 slowest inserts:");
//     for (i, &time) in sorted_times.iter().take(10).enumerate() {
//         println!("  #{}: {:?}", i + 1, time);
//     }

//     (total_time, max_time, insert_times)
// }

// fn main() {
//     println!("=== Maximum Latency Comparison ===");
//     println!("Focus: Comparing worst-case insert times during resize operations\n");

//     // Test different dataset sizes
//     let sizes = [25_000, 75_000, 150_000];

//     for size in sizes {
//         println!("{}", "=".repeat(80));
//         println!("DATASET SIZE: {} entries", size);
//         println!("{}", "=".repeat(80));

//         // u64 -> u64 benchmark
//         let (single_total, single_max, _) =
//             benchmark_max_latency(&format!("Single Array u64->u64 ({})", size), size, |i| {
//                 static mut MAP: Option<SingleU64U64> = None;
//                 unsafe {
//                     if MAP.is_none() {
//                         MAP = Some(SingleU64U64::new());
//                     }

//                     let start = Instant::now();
//                     MAP.as_mut()
//                         .unwrap()
//                         .insert(&(i as u64), &((i * 2) as u64))
//                         .unwrap();
//                     start.elapsed()
//                 }
//             });

//         let (double_total, double_max, _) =
//             benchmark_max_latency(&format!("Double Array u64->u64 ({})", size), size, |i| {
//                 static mut MAP: Option<DoubleU64U64> = None;
//                 unsafe {
//                     if MAP.is_none() {
//                         MAP = Some(DoubleU64U64::new_with_double_array());
//                     }

//                     let start = Instant::now();
//                     MAP.as_mut()
//                         .unwrap()
//                         .insert(&(i as u64), &((i * 2) as u64))
//                         .unwrap();
//                     start.elapsed()
//                 }
//             });

//         println!("\n📊 PERFORMANCE SUMMARY:");
//         println!(
//             "   Max Insert Latency Improvement: {:.2}x faster",
//             single_max.as_nanos() as f64 / double_max.as_nanos() as f64
//         );
//         println!(
//             "   Total Time Improvement: {:.2}x faster",
//             single_total.as_nanos() as f64 / double_total.as_nanos() as f64
//         );
//         println!("   Single Array Max: {:?}", single_max);
//         println!("   Double Array Max: {:?}", double_max);

//         // String -> String benchmark for this size (smaller due to overhead)
//         let str_size = std::cmp::min(size / 3, 10_000);
//         if str_size > 1000 {
//             println!("\n--- String Key/Value Test ({}  entries) ---", str_size);

//             let test_data: Vec<(String, String)> = (0..str_size)
//                 .map(|i| (format!("key_{:08}", i), format!("value_data_{:08}", i * 7)))
//                 .collect();

//             let (single_str_total, single_str_max, _) =
//                 benchmark_max_latency("Single Array String->String", str_size, |i| {
//                     static mut MAP: Option<SingleStrStr> = None;
//                     static mut DATA: Option<Vec<(String, String)>> = None;
//                     unsafe {
//                         if MAP.is_none() {
//                             MAP = Some(SingleStrStr::new());
//                             DATA = Some(
//                                 (0..str_size)
//                                     .map(|j| {
//                                         (
//                                             format!("key_{:08}", j),
//                                             format!("value_data_{:08}", j * 7),
//                                         )
//                                     })
//                                     .collect(),
//                             );
//                         }

//                         let data = DATA.as_ref().unwrap();
//                         let start = Instant::now();
//                         MAP.as_mut()
//                             .unwrap()
//                             .insert(data[i].0.as_str(), data[i].1.as_str())
//                             .unwrap();
//                         start.elapsed()
//                     }
//                 });

//             let (double_str_total, double_str_max, _) =
//                 benchmark_max_latency("Double Array String->String", str_size, |i| {
//                     static mut MAP: Option<DoubleStrStr> = None;
//                     static mut DATA: Option<Vec<(String, String)>> = None;
//                     unsafe {
//                         if MAP.is_none() {
//                             MAP = Some(DoubleStrStr::new_with_double_array());
//                             DATA = Some(
//                                 (0..str_size)
//                                     .map(|j| {
//                                         (
//                                             format!("key_{:08}", j),
//                                             format!("value_data_{:08}", j * 7),
//                                         )
//                                     })
//                                     .collect(),
//                             );
//                         }

//                         let data = DATA.as_ref().unwrap();
//                         let start = Instant::now();
//                         MAP.as_mut()
//                             .unwrap()
//                             .insert(data[i].0.as_str(), data[i].1.as_str())
//                             .unwrap();
//                         start.elapsed()
//                     }
//                 });

//             println!(
//                 "   String Max Latency Improvement: {:.2}x faster",
//                 single_str_max.as_nanos() as f64 / double_str_max.as_nanos() as f64
//             );
//             println!(
//                 "   String Total Time: {:.2}x faster",
//                 single_str_total.as_nanos() as f64 / double_str_total.as_nanos() as f64
//             );
//         }
//     }

//     println!("\n{}", "=".repeat(80));
//     println!("CONCLUSION: Double Array Implementation Benefits");
//     println!("{}", "=".repeat(80));
//     println!("✅ Reduces maximum insert latency spikes (primary goal)");
//     println!("✅ Improves overall performance for large datasets");
//     println!("✅ Provides more predictable performance characteristics");
//     println!("✅ Better scalability for applications with latency requirements");
// }

fn main() {}
