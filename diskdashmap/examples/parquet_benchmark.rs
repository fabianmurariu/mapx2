//! Parquet file processing benchmark for DiskDashMap
//!
//! This example demonstrates concurrent reading of a Parquet file and storing
//! id->row_number mappings in a DiskDashMap. Each thread processes a subset
//! of rows based on modulo distribution.
//!
//! Usage:
//!   cargo run --example parquet_benchmark --release -- \
//!     --parquet-file /path/to/file.parquet \
//!     --output-dir /tmp/diskdashmap_benchmark \
//!     --threads 8

use clap::Parser;
use diskdashmap::DiskDashMap;
use diskhashmap::MMapFile;
use diskhashmap::types::Native;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use rustc_hash::FxBuildHasher;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the input Parquet file
    #[arg(short, long)]
    parquet_file: PathBuf,

    /// Output directory for DiskDashMap
    #[arg(short, long)]
    output_dir: PathBuf,

    /// Number of threads to use for parallel processing
    #[arg(short, long, default_value_t = 4)]
    threads: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("=== Parquet to DiskDashMap Benchmark ===");
    println!("Parquet file: {:?}", args.parquet_file);
    println!("Output directory: {:?}", args.output_dir);
    println!("Threads: {}", args.threads);

    // Open parquet file to get metadata
    let file = File::open(&args.parquet_file)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let total_rows = metadata.file_metadata().num_rows() as usize;

    println!("Total rows in Parquet file: {}", total_rows);

    // Find the "id" column index
    let schema = metadata.file_metadata().schema_descr();
    let id_col_idx = schema
        .columns()
        .iter()
        .position(|col| col.name() == "id")
        .ok_or("Column 'id' not found in Parquet schema")?;

    println!("Found 'id' column at index: {}", id_col_idx);

    // Create DiskDashMap with i64 keys (IDs) and u64 values (row numbers)
    if args.output_dir.exists() {
        std::fs::remove_dir_all(&args.output_dir)?;
    }
    let map: Arc<DiskDashMap<Native<i64>, Native<u64>, MMapFile, FxBuildHasher>> =
        Arc::new(DiskDashMap::new_in(&args.output_dir)?);

    println!("\nStarting parallel insertion...");
    let insert_start = Instant::now();

    // Shared counter for progress tracking
    let inserted_total = Arc::new(AtomicUsize::new(0));

    // Spawn worker threads
    let handles: Vec<_> = (0..args.threads)
        .map(|thread_id| {
            let map = Arc::clone(&map);
            let parquet_file = args.parquet_file.clone();
            let num_threads = args.threads;
            let inserted_total = Arc::clone(&inserted_total);

            thread::spawn(
                move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    // Each thread opens its own file reader
                    let file = File::open(&parquet_file)?;
                    let reader = SerializedFileReader::new(file)?;
                    let mut row_iter = reader.get_row_iter(None)?;

                    let mut local_inserted = 0;
                    let mut row_num = 0u64;

                    // Process rows using modulo distribution
                    loop {
                        let record = match row_iter.next() {
                            Some(Ok(rec)) => rec,
                            Some(Err(e)) => {
                                eprintln!(
                                    "Thread {}: Error reading row {}: {}",
                                    thread_id, row_num, e
                                );
                                row_num += 1;
                                continue;
                            }
                            None => break,
                        };

                        if row_num % num_threads as u64 == thread_id as u64 {
                            // Extract the id value from the Row
                            let id_value = match record.get_long(id_col_idx) {
                                Ok(val) => val,
                                Err(e) => {
                                    eprintln!(
                                        "Thread {}: Error getting id at row {}: {}",
                                        thread_id, row_num, e
                                    );
                                    row_num += 1;
                                    continue;
                                }
                            };

                            // Insert into map: id -> row_number
                            map.insert(&id_value, &row_num)?;
                            local_inserted += 1;

                            // Progress update every 10000 rows
                            if local_inserted % 10000 == 0 {
                                let total =
                                    inserted_total.fetch_add(10000, Ordering::Relaxed) + 10000;
                                println!(
                                    "Thread {}: Inserted {} entries (total: {})",
                                    thread_id, local_inserted, total
                                );
                            }
                        }
                        row_num += 1;
                    }

                    // Final progress update
                    let remainder = local_inserted % 10000;
                    if remainder > 0 {
                        inserted_total.fetch_add(remainder, Ordering::Relaxed);
                    }

                    println!(
                        "Thread {} completed: inserted {} entries",
                        thread_id, local_inserted
                    );
                    Ok(())
                },
            )
        })
        .collect();

    // Wait for all threads to complete
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("Thread {} failed: {}", i, e),
            Err(_) => eprintln!("Thread {} panicked", i),
        }
    }

    let insert_duration = insert_start.elapsed();
    let final_inserted = inserted_total.load(Ordering::Relaxed);
    println!(
        "\nInsertion completed in {:.2?} ({:.0} inserts/sec)",
        insert_duration,
        final_inserted as f64 / insert_duration.as_secs_f64()
    );

    // Verify map size
    let map_len = map.len();
    println!("DiskDashMap contains {} entries", map_len);

    // Validation phase
    println!("\nStarting validation...");
    let validate_start = Instant::now();

    let file = File::open(&args.parquet_file)?;
    let reader = SerializedFileReader::new(file)?;
    let mut row_iter = reader.get_row_iter(None)?;

    let mut validated = 0;
    let mut mismatches = 0;
    let mut missing = 0;
    let mut row_num = 0u64;

    loop {
        let record = match row_iter.next() {
            Some(Ok(rec)) => rec,
            Some(Err(e)) => {
                eprintln!("Validation: Error reading row {}: {}", row_num, e);
                row_num += 1;
                continue;
            }
            None => break,
        };

        let id_value = match record.get_long(id_col_idx) {
            Ok(val) => val,
            Err(e) => {
                eprintln!("Validation: Error getting id at row {}: {}", row_num, e);
                row_num += 1;
                continue;
            }
        };

        match map.get(&id_value) {
            Ok(Some(stored_ref)) => {
                let stored_row = stored_ref.value()?;
                if stored_row == row_num {
                    validated += 1;
                } else {
                    eprintln!(
                        "Mismatch: id {} -> expected row {}, got row {}",
                        id_value, row_num, stored_row
                    );
                    mismatches += 1;
                }
            }
            Ok(None) => {
                eprintln!("Missing: id {} (row {})", id_value, row_num);
                missing += 1;
            }
            Err(e) => {
                eprintln!("Error getting id {} (row {}): {}", id_value, row_num, e);
            }
        }

        row_num += 1;

        if row_num % 100000 == 0 {
            println!("Validated {} rows...", row_num);
        }
    }

    let validate_duration = validate_start.elapsed();

    println!("\n=== Validation Results ===");
    println!("Total rows validated: {}", row_num);
    println!("Correct entries: {}", validated);
    println!("Mismatches: {}", mismatches);
    println!("Missing entries: {}", missing);
    println!("Validation time: {:.2?}", validate_duration);

    if validated == total_rows && mismatches == 0 && missing == 0 {
        println!("\n✓ SUCCESS: All entries validated correctly!");
    } else {
        println!("\n✗ FAILURE: Validation found errors!");
    }

    println!("\n=== Performance Summary ===");
    println!(
        "Insert throughput: {:.0} rows/sec",
        total_rows as f64 / insert_duration.as_secs_f64()
    );
    println!(
        "Validate throughput: {:.0} rows/sec",
        total_rows as f64 / validate_duration.as_secs_f64()
    );

    Ok(())
}
