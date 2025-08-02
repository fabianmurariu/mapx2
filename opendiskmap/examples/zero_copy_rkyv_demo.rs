use opendiskmap::byte_store::MMapFile;
use opendiskmap::{DiskHashMap, Native, types::Arch};
use rkyv::{Archive, Deserialize, Serialize};
use rustc_hash::FxBuildHasher;
use tempfile::tempdir;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
struct UserProfile {
    id: u32,
    name: String,
    email: String,
    scores: Vec<f64>,
    metadata: Vec<(String, String)>,
}

impl UserProfile {
    fn new(id: u32, name: &str, email: &str) -> Self {
        Self {
            id,
            name: name.to_string(),
            email: email.to_string(),
            scores: vec![85.5, 92.1, 78.3, 95.0],
            metadata: vec![
                ("department".to_string(), "Engineering".to_string()),
                ("level".to_string(), "Senior".to_string()),
                ("location".to_string(), "Remote".to_string()),
            ],
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempdir()?;

    // Create map with archived values for zero-copy access
    let mut map: DiskHashMap<Native<u64>, Arch<UserProfile>, MMapFile, FxBuildHasher> =
        DiskHashMap::new_in(dir.path())?;

    // Create some user profiles
    let alice = UserProfile::new(1, "Alice Smith", "alice@company.com");
    let bob = UserProfile::new(2, "Bob Johnson", "bob@company.com");
    let charlie = UserProfile::new(3, "Charlie Brown", "charlie@company.com");

    // Insert profiles (automatically serialized with rkyv)
    map.insert(&1001, &alice)?;
    map.insert(&1002, &bob)?;
    map.insert(&1003, &charlie)?;

    println!("Inserted {} user profiles", map.len());

    // Retrieve with zero-copy deserialization
    for user_id in [1001, 1002, 1003, 1004] {
        if let Some(profile) = map.get(&user_id)? {
            println!("\n=== User {user_id} ===");
            println!("ID: {}", profile.id);
            println!("Name: {}", profile.name);
            println!("Email: {}", profile.email);
            println!(
                "Scores: {:?}",
                profile
                    .scores
                    .iter()
                    .map(|s| s.to_native())
                    .collect::<Vec<_>>()
            );
            println!("Metadata:");
            for item in profile.metadata.iter() {
                println!("  {}: {}", item.0, item.1);
            }

            // Calculate average score with zero-copy access
            let avg_score: f64 = profile.scores.iter().map(|s| s.to_native()).sum::<f64>()
                / profile.scores.len() as f64;
            println!("Average score: {avg_score:.2}");
        } else {
            println!("User {user_id} not found");
        }
    }

    // Demonstrate persistence with zero-copy data
    println!("\n=== Testing Persistence ===");
    drop(map);

    let map: DiskHashMap<Native<u64>, Arch<UserProfile>, MMapFile, FxBuildHasher> =
        DiskHashMap::load_from(dir.path())?;

    println!("Reloaded map with {} profiles", map.len());

    // Access persisted zero-copy data
    if let Some(profile) = map.get(&1001)? {
        println!(
            "First user after reload: {} ({})",
            profile.name, profile.email
        );

        // The profile data is accessed with zero deserialization overhead !
        // It's a direct view into the memory-mapped file data.
    }

    Ok(())
}
