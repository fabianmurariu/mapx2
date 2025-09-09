use diskhashmap::byte_store::VecStore;
use diskhashmap::{
    types::{Bytes, BytesDecode, BytesEncode},
    DiskHashMap,
};
use rustc_hash::FxBuildHasher;

fn main() {
    let mut map: DiskHashMap<Bytes, Bytes, VecStore, FxBuildHasher> = DiskHashMap::new();

    // Test encoding
    let key = b"hello";
    let value = b"world";

    println!("=== Testing Encoding ===");
    let (key_size, key_bytes) = Bytes::bytes_encode(key).unwrap();
    let (value_size, value_bytes) = Bytes::bytes_encode(value).unwrap();

    println!("Key size: {:?}", key_size);
    println!("Key bytes: {:?}", key_bytes.as_ref());
    println!("Value size: {:?}", value_size);
    println!("Value bytes: {:?}", value_bytes.as_ref());

    // Test decoding
    println!("\n=== Testing Decoding ===");
    let decoded_key = Bytes::bytes_decode_with_size(key_bytes.as_ref(), key_size.unwrap()).unwrap();
    let decoded_value =
        Bytes::bytes_decode_with_size(value_bytes.as_ref(), value_size.unwrap()).unwrap();

    println!("Decoded key: {:?}", decoded_key);
    println!("Decoded value: {:?}", decoded_value);

    // Test insertion and retrieval
    println!("\n=== Testing Map Operations ===");
    map.insert(key, value).unwrap();
    println!("Inserted key-value pair");

    let result = map.get(key).unwrap();
    println!("Retrieved result: {:?}", result);

    if let Some(retrieved_value) = result {
        println!("Retrieved value: {:?}", retrieved_value);
        println!("Expected value: {:?}", value);
        println!("Match: {}", retrieved_value == value);
    } else {
        println!("No value retrieved!");
    }
}
