use diskhashmap::Buffers;
use diskhashmap::byte_store::VecStore;
use std::collections::HashMap;

fn main() {
    println!("=== ByteStore Demo ===\n");

    // Example 1: Using Vec as backing storage
    demo_with_vec();

    // Example 2: Using fixed-size array
    demo_with_array();

    // Example 3: Using boxed slice
    demo_with_boxed_slice();

    // Example 4: Stress test with capacity limits
    demo_capacity_limits();

    // Example 5: Building a simple key-value store
    demo_kv_store();
}

fn demo_with_vec() {
    println!("1. Demo with VecStore backing:");
    let mut store = Buffers::new(VecStore::with_capacity(256));

    let texts = ["Hello", "World", "Rust", "ByteStore"];
    let mut indices = Vec::new();

    // Store some text data
    for text in &texts {
        let idx = store.append(text.as_bytes());
        indices.push(idx);
        println!("   Stored '{text}' at index {idx}");
    }

    println!("   Store length: {}", store.len());
    println!("   Free space: {} bytes", store.free_space());

    // Retrieve and verify
    for (i, &idx) in indices.iter().enumerate() {
        let retrieved = store.get(idx).unwrap();
        let text = std::str::from_utf8(retrieved).unwrap();
        println!("   Retrieved index {idx}: '{text}'");
        assert_eq!(text, texts[i]);
    }
    println!();
}

fn demo_with_array() {
    println!("2. Demo with fixed array [u8; 128]:");
    let mut store = Buffers::new([0u8; 128]);

    // Store binary data
    let binary_data = [
        vec![0x01, 0x02, 0x03],
        vec![0xFF, 0xFE, 0xFD, 0xFC],
        vec![0x42],
        vec![], // Empty data
    ];

    for data in binary_data.iter() {
        let idx = store.append(data);
        println!("   Stored {} bytes at index {}", data.len(), idx);
    }

    // Verify retrieval
    for (i, retrieved) in store.iter().enumerate() {
        // let retrieved = store.get(i).unwrap();
        println!(
            "   Index {}: {:?} ({} bytes)",
            i,
            retrieved,
            retrieved.len()
        );
        assert_eq!(retrieved, binary_data[i].as_slice());
    }
    println!();
}

fn demo_with_boxed_slice() {
    println!("3. Demo with Box<[u8]>:");
    let backing = vec![0u8; 512].into_boxed_slice();
    let mut store = Buffers::new(backing);

    // Store varying size data
    for size in [1, 10, 50, 100, 200] {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        let idx = store.append(&data);
        println!("   Stored {size} byte pattern at index {idx}");
    }

    println!(
        "   Final store state: {} entries, {} bytes free",
        store.len(),
        store.free_space()
    );
    println!();
}

fn demo_capacity_limits() {
    println!("4. Capacity limits demo:");
    let mut store = Buffers::new(VecStore::with_capacity(64)); // Small buffer

    let mut count = 0;
    for i in 0..20 {
        let data = format!("entry_{i}");
        let idx = store.append(data.as_bytes());
        count += 1;
        println!(
            "   Stored entry {} at index {} ({} bytes free)",
            count,
            idx,
            store.free_space()
        );
    }

    // Store one more to demonstrate auto-growing
    let idx = store.append(b"auto_grow_test");
    println!("   Stored overflow test at index {idx} (auto-grew buffer)");
    println!();
}

fn demo_kv_store() {
    println!("5. Simple Key-Value Store demo:");

    // Build a simple KV store using ByteStore + HashMap
    struct SimpleKV {
        store: Buffers<VecStore>,
        index: HashMap<String, usize>,
    }

    impl SimpleKV {
        fn new(capacity: usize) -> Self {
            Self {
                store: Buffers::new(VecStore::with_capacity(capacity)),
                index: HashMap::new(),
            }
        }

        fn put(&mut self, key: &str, value: &[u8]) -> Result<(), &'static str> {
            let idx = self.store.append(value);
            self.index.insert(key.to_string(), idx);
            Ok(())
        }

        fn get(&self, key: &str) -> Option<&[u8]> {
            self.index.get(key).and_then(|&idx| self.store.get(idx))
        }

        fn stats(&self) -> (usize, usize) {
            (self.store.len(), self.store.free_space())
        }
    }

    let mut kv = SimpleKV::new(1024);

    // Store some key-value pairs
    let data: Vec<(&str, &[u8])> = vec![
        ("name", b"ByteStore"),
        ("version", b"1.0.0"),
        ("language", b"Rust"),
        ("empty_value", b""),
        ("binary", &[0x01, 0x02, 0x03, 0x04]),
    ];

    for (key, value) in &data {
        kv.put(key, value).unwrap();
        println!(
            "   Stored {}: {:?}",
            key,
            std::str::from_utf8(value).unwrap_or("<binary>")
        );
    }

    let (entries, free) = kv.stats();
    println!("   KV Store: {entries} entries, {free} bytes free");

    // Retrieve and verify
    for (key, expected_value) in &data {
        match kv.get(key) {
            Some(value) => {
                assert_eq!(value, *expected_value);
                println!(
                    "   Retrieved {}: {:?}",
                    key,
                    std::str::from_utf8(value).unwrap_or("<binary>")
                );
            }
            None => println!("   Key '{key}' not found"),
        }
    }

    // Try to get non-existent key
    if kv.get("nonexistent").is_none() {
        println!("   Confirmed: nonexistent key returns None");
    }

    println!("\n=== Demo Complete ===");
}
