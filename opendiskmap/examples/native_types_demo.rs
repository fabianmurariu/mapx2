use opendiskmap::byte_store::MMapFile;
use opendiskmap::{DiskHashMap, Native, Str, Result};
use rustc_hash::FxBuildHasher;
use tempfile::tempdir;

fn main() -> Result<()> {
    let dir = tempdir()?;

    // Create a persistent map with u32 keys and String values
    let mut map: DiskHashMap<Native<u32>, Str, MMapFile, FxBuildHasher> =
        DiskHashMap::new_in(dir.path())?;

    // Insert various native types and strings
    map.insert(&1, "Alice")?;
    map.insert(&2, "Bob")?;
    map.insert(&3, "Charlie")?;
    map.insert(&100, "Special user")?;

    // Retrieve data
    for id in [1, 2, 3, 100, 999] {
        match map.get(&id)? {
            Some(name) => println!("User {id}: {name}"),
            None => println!("User {id} not found"),
        }
    }

    println!("Map contains {} users", map.len());

    // Update an existing entry
    let old_name = map.insert(&2, "Robert")?;
    println!("Updated user 2: {old_name:?} -> Robert");

    // Demonstrate persistence
    println!("\nSaving to disk and reloading...");
    drop(map);

    let map: DiskHashMap<Native<u32>, Str, MMapFile, FxBuildHasher> =
        DiskHashMap::load_from(dir.path())?;

    println!("Reloaded map with {} users", map.len());
    if let Some(name) = map.get(&2)? {
        println!("User 2 after reload: {name}");
    }

    Ok(())
}
