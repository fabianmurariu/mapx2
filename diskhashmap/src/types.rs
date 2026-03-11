use crate::error::{DiskMapError, Result};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Deref;

pub enum CowBytes<'a> {
    Borrowed(&'a [u8]),
    Owned(Box<dyn AsRef<[u8]> + 'a>),
}

impl<'a> CowBytes<'a> {
    pub fn borrowed(item: &'a [u8]) -> Self {
        CowBytes::Borrowed(item)
    }

    pub fn owned<T: AsRef<[u8]> + 'static>(item: T) -> Self {
        CowBytes::Owned(Box::new(item))
    }
}

impl AsRef<[u8]> for CowBytes<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            CowBytes::Borrowed(item) => item,
            CowBytes::Owned(item) => item.as_ref().as_ref(),
        }
    }
}

impl Deref for CowBytes<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            CowBytes::Borrowed(item) => item,
            CowBytes::Owned(item) => item.as_ref().as_ref(),
        }
    }
}

/// Trait for extracting actual bytes from stored format
pub trait BytesActual<'a> {
    fn bytes_actual(bytes: &'a [u8]) -> &'a [u8];
}

/// Trait for encoding types into byte representation
pub trait BytesEncode<'a>: BytesActual<'a> {
    type EItem: 'a + ?Sized;

    /// Encode an item into bytes
    fn bytes_encode(item: &'a Self::EItem) -> Result<(Option<usize>, CowBytes<'a>)>;

    fn eq_alt(l: &[u8], r: &[u8]) -> bool;

    fn hash_alt<S: Hasher>(item: &[u8], s: &mut S) -> u64 {
        item.hash(s);
        s.finish()
    }
}

/// Trait for decoding types from byte representation
pub trait BytesDecode<'a>: BytesActual<'a> {
    type DItem: 'a;

    /// Decode bytes into an item and return the number of bytes consumed.
    /// The consumed size includes any length prefix for variable-length types.
    fn bytes_decode(bytes: &'a [u8]) -> Result<(Self::DItem, usize)>;
}

/// Wrapper for native types that can be represented as bytes (numbers, etc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Native<T>(PhantomData<T>);

impl<T> Default for Native<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

/// Wrapper for string types - works directly with &str
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Str;

impl Default for Str {
    fn default() -> Self {
        Self
    }
}

/// Wrapper for byte slice types - works directly with &[u8]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Bytes;

impl Default for Bytes {
    fn default() -> Self {
        Self
    }
}

// Implementations for Native<T>
impl<'a, T> BytesActual<'a> for Native<T>
where
    T: bytemuck::Pod,
{
    fn bytes_actual(bytes: &'a [u8]) -> &'a [u8] {
        bytes
    }
}

impl<'a, T> BytesEncode<'a> for Native<T>
where
    T: bytemuck::Pod + Eq + std::hash::Hash,
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<(Option<usize>, CowBytes<'a>)> {
        Ok((None, CowBytes::Borrowed(bytemuck::bytes_of(item))))
    }

    fn eq_alt(l: &[u8], r: &[u8]) -> bool {
        // Use pod_read_unaligned since data may not be properly aligned
        bytemuck::pod_read_unaligned::<T>(l) == bytemuck::pod_read_unaligned::<T>(r)
    }

    fn hash_alt<S: Hasher>(item: &[u8], s: &mut S) -> u64 {
        // Use pod_read_unaligned since data may not be properly aligned
        let value: T = bytemuck::pod_read_unaligned(item);
        value.hash(s);
        s.finish()
    }
}

impl<'a, T> BytesDecode<'a> for Native<T>
where
    T: bytemuck::Pod + Copy,
{
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<(Self::DItem, usize)> {
        let size = std::mem::size_of::<T>();
        if bytes.len() < size {
            return Err(DiskMapError::Decoding(format!(
                "Invalid byte length for {}: expected at least {}, got {}",
                std::any::type_name::<T>(),
                size,
                bytes.len()
            )));
        }
        // Use pod_read_unaligned since heap data may not be properly aligned
        Ok((bytemuck::pod_read_unaligned(&bytes[..size]), size))
    }
}

// Implementations for Str
impl<'a> BytesActual<'a> for Str {
    fn bytes_actual(bytes: &'a [u8]) -> &'a [u8] {
        let len_offset = std::mem::size_of::<usize>();
        let len = usize::from_le_bytes(bytes[0..len_offset].try_into().unwrap());
        &bytes[len_offset..len_offset + len]
    }
}

impl<'a> BytesEncode<'a> for Str {
    type EItem = str;

    fn bytes_encode(item: &'a Self::EItem) -> Result<(Option<usize>, CowBytes<'a>)> {
        let bytes = item.as_bytes();
        Ok((Some(bytes.len()), CowBytes::Borrowed(bytes)))
    }

    fn eq_alt(l: &[u8], r: &[u8]) -> bool {
        l == Self::bytes_actual(r)
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Result<(Self::DItem, usize)> {
        let len_offset = std::mem::size_of::<usize>();
        let len = usize::from_le_bytes(bytes[0..len_offset].try_into().unwrap());
        let str_bytes = &bytes[len_offset..len_offset + len];
        let consumed = len_offset + len;
        let s =
            std::str::from_utf8(str_bytes).map_err(|e| DiskMapError::Decoding(e.to_string()))?;
        Ok((s, consumed))
    }
}

// Implementations for Bytes
impl<'a> BytesActual<'a> for Bytes {
    fn bytes_actual(bytes: &'a [u8]) -> &'a [u8] {
        let len_offset = std::mem::size_of::<usize>();
        let len = usize::from_le_bytes(bytes[0..len_offset].try_into().unwrap());
        &bytes[len_offset..len_offset + len]
    }
}

impl<'a> BytesEncode<'a> for Bytes {
    type EItem = [u8];

    fn bytes_encode(item: &'a Self::EItem) -> Result<(Option<usize>, CowBytes<'a>)> {
        Ok((Some(item.len()), CowBytes::Borrowed(item)))
    }

    fn eq_alt(l: &[u8], r: &[u8]) -> bool {
        l == Self::bytes_actual(r)
    }
}

impl<'a> BytesDecode<'a> for Bytes {
    type DItem = &'a [u8];

    fn bytes_decode(bytes: &'a [u8]) -> Result<(Self::DItem, usize)> {
        let len_offset = std::mem::size_of::<usize>();
        let len = usize::from_le_bytes(bytes[0..len_offset].try_into().unwrap());
        let actual_bytes = &bytes[len_offset..len_offset + len];
        let consumed = len_offset + len;
        Ok((actual_bytes, consumed))
    }
}

/// Helper struct for encoding/decoding key-value pairs with hash prefix.
///
/// Layout: `[hash: u64][key_data][value_data]`
///
/// Where key_data and value_data use BytesEncode format:
/// - For Native<T>: raw bytes (no length prefix)
/// - For Str/Bytes: `[len: usize][actual_bytes]`
pub struct KVPair<K, V>(PhantomData<(K, V)>);

impl<K, V> KVPair<K, V> {
    pub const HASH_SIZE: usize = std::mem::size_of::<u64>();
}

impl<'a, K, V> KVPair<K, V>
where
    K: BytesEncode<'a>,
    V: BytesEncode<'a>,
{
    /// Encode hash, key, and value into a single byte buffer.
    ///
    /// Returns the encoded bytes.
    pub fn encode<B: AsMut<[u8]>, F: FnOnce(usize) -> B>(
        hash: u64,
        key: &'a K::EItem,
        value: &'a V::EItem,
        buf_factory: F,
    ) -> Result<B> {
        let (key_len, key_bytes) = K::bytes_encode(key)?;
        let (value_len, value_bytes) = V::bytes_encode(value)?;

        // Calculate total size
        let key_stored_size = if key_len.is_some() {
            std::mem::size_of::<usize>() + key_bytes.len()
        } else {
            key_bytes.len()
        };

        let value_stored_size = if value_len.is_some() {
            std::mem::size_of::<usize>() + value_bytes.len()
        } else {
            value_bytes.len()
        };

        let total_size = Self::HASH_SIZE + key_stored_size + value_stored_size;
        let mut page = buf_factory(total_size); //vec![0u8; total_size];
        let mut buf = page.as_mut();

        // Write hash
        buf[0..Self::HASH_SIZE].copy_from_slice(&hash.to_le_bytes());
        let mut offset = Self::HASH_SIZE;

        // Write key
        if let Some(len) = key_len {
            buf[offset..offset + std::mem::size_of::<usize>()].copy_from_slice(&len.to_le_bytes());
            offset += std::mem::size_of::<usize>();
        }
        buf[offset..offset + key_bytes.len()].copy_from_slice(&key_bytes);
        offset += key_bytes.len();

        // Write value
        if let Some(len) = value_len {
            buf[offset..offset + std::mem::size_of::<usize>()].copy_from_slice(&len.to_le_bytes());
            offset += std::mem::size_of::<usize>();
        }
        buf[offset..offset + value_bytes.len()].copy_from_slice(&value_bytes);

        Ok(page)
    }
}

impl<K, V> KVPair<K, V> {
    /// Decode the hash from the beginning of the buffer.
    #[inline]
    pub fn decode_hash(bytes: &[u8]) -> u64 {
        u64::from_le_bytes(bytes[0..Self::HASH_SIZE].try_into().unwrap())
    }
}

impl<'a, K, V> KVPair<K, V>
where
    K: BytesDecode<'a>,
{
    /// Decode the key from the buffer (after the hash).
    /// Returns the key and the number of bytes consumed (including hash).
    #[inline]
    pub fn decode_key(bytes: &'a [u8]) -> Result<(K::DItem, usize)> {
        let key_start = Self::HASH_SIZE;
        let (key, key_consumed) = K::bytes_decode(&bytes[key_start..])?;
        Ok((key, key_start + key_consumed))
    }
}

impl<'a, K, V> KVPair<K, V>
where
    K: BytesDecode<'a>,
{
    /// Get the actual key bytes from the buffer (for comparison).
    /// Returns the key bytes and the offset where the value starts.
    #[inline]
    pub fn get_key_bytes(bytes: &'a [u8]) -> Result<(&'a [u8], usize)> {
        let key_start = Self::HASH_SIZE;
        let (_, key_consumed) = K::bytes_decode(&bytes[key_start..])?;
        let key_bytes = &bytes[key_start..key_start + key_consumed];
        Ok((key_bytes, key_start + key_consumed))
    }
}

impl<'a, K, V> KVPair<K, V>
where
    K: BytesDecode<'a>,
    V: BytesDecode<'a>,
{
    /// Decode the value from the buffer, given the key size.
    /// `key_total_offset` is the offset where the value starts (hash + key size).
    #[inline]
    pub fn decode_value(bytes: &'a [u8], key_total_offset: usize) -> Result<(V::DItem, usize)> {
        let (value, value_consumed) = V::bytes_decode(&bytes[key_total_offset..])?;
        Ok((value, key_total_offset + value_consumed))
    }

    /// Decode both key and value from the buffer.
    /// Returns (key, value, total_bytes_consumed).
    #[inline]
    pub fn decode_key_value(bytes: &'a [u8]) -> Result<(K::DItem, V::DItem, usize)> {
        let (key, key_end) = Self::decode_key(bytes)?;
        let (value, total) = Self::decode_value(bytes, key_end)?;
        Ok((key, value, total))
    }
}

#[cfg(feature = "rkyv")]
pub mod rkyv {
    use super::*;
    use ::rkyv::Archive;
    use ::rkyv::api::high::{HighSerializer, HighValidator};
    use ::rkyv::bytecheck::CheckBytes;
    use ::rkyv::ser::allocator::ArenaHandle;
    use ::rkyv::util::AlignedVec;
    use std::marker::PhantomData;

    pub struct Arch<T>(PhantomData<T>);

    impl<'a, T> BytesActual<'a> for Arch<T>
    where
        T: ::rkyv::Archive,
    {
        fn bytes_actual(bytes: &'a [u8]) -> &'a [u8] {
            let len_offset = std::mem::size_of::<usize>();
            let len = usize::from_le_bytes(bytes[0..len_offset].try_into().unwrap());
            &bytes[len_offset..len_offset + len]
        }
    }

    impl<
        'a,
        T: for<'b> ::rkyv::Serialize<
                HighSerializer<AlignedVec, ArenaHandle<'b>, ::rkyv::rancor::Error>,
            > + 'a,
    > BytesEncode<'a> for Arch<T>
    {
        type EItem = T;

        fn bytes_encode(item: &'a Self::EItem) -> Result<(Option<usize>, CowBytes<'a>)> {
            let bytes = ::rkyv::to_bytes::<::rkyv::rancor::Error>(item)
                .map_err(|e| DiskMapError::Serialization(e.to_string()))?;
            let len = bytes.len();
            Ok((Some(len), CowBytes::owned(bytes)))
        }

        fn eq_alt(l: &[u8], r: &[u8]) -> bool {
            l == Self::bytes_actual(r)
        }
    }

    impl<'a, T> BytesDecode<'a> for Arch<T>
    where
        T: 'a + ::rkyv::Archive,
        T::Archived: for<'b> CheckBytes<HighValidator<'b, ::rkyv::rancor::Error>>,
    {
        type DItem = &'a <T as Archive>::Archived;

        fn bytes_decode(bytes: &'a [u8]) -> Result<(Self::DItem, usize)> {
            let len_offset = std::mem::size_of::<usize>();
            let len = usize::from_le_bytes(bytes[0..len_offset].try_into().unwrap());
            let actual_bytes = &bytes[len_offset..len_offset + len];
            let consumed = len_offset + len;
            let item = ::rkyv::access::<::rkyv::Archived<T>, ::rkyv::rancor::Error>(actual_bytes)
                .map_err(|e| DiskMapError::Decoding(e.to_string()))?;
            Ok((item, consumed))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kvpair_encode_decode_native() {
        let hash: u64 = 0xDEADBEEF;
        let key: u64 = 42;
        let value: u64 = 100;

        let encoded =
            KVPair::<Native<u64>, Native<u64>>::encode(hash, &key, &value, |size| vec![0; size])
                .unwrap();

        // Check hash
        assert_eq!(
            KVPair::<Native<u64>, Native<u64>>::decode_hash(&encoded),
            hash
        );

        // Check key
        let (decoded_key, key_end) =
            KVPair::<Native<u64>, Native<u64>>::decode_key(&encoded).unwrap();
        assert_eq!(decoded_key, key);

        // Check value
        let (decoded_value, _) =
            KVPair::<Native<u64>, Native<u64>>::decode_value(&encoded, key_end).unwrap();
        assert_eq!(decoded_value, value);

        // Check combined decode
        let (k, v, _) = KVPair::<Native<u64>, Native<u64>>::decode_key_value(&encoded).unwrap();
        assert_eq!(k, key);
        assert_eq!(v, value);
    }

    #[test]
    fn test_kvpair_encode_decode_str() {
        let hash: u64 = 0xCAFEBABE;
        let key = "hello";
        let value = "world";

        let encoded = KVPair::<Str, Str>::encode(hash, key, value, |size| vec![0; size]).unwrap();

        // Check hash
        assert_eq!(KVPair::<Str, Str>::decode_hash(&encoded), hash);

        // Check key and value
        let (k, v, _) = KVPair::<Str, Str>::decode_key_value(&encoded).unwrap();
        assert_eq!(k, "hello");
        assert_eq!(v, "world");
    }

    #[test]
    fn test_kvpair_encode_decode_bytes() {
        let hash: u64 = 0x12345678;
        let key = b"key_data";
        let value = b"value_data";

        let encoded = KVPair::<Bytes, Bytes>::encode(hash, key.as_ref(), value.as_ref(), |size| {
            vec![0; size]
        })
        .unwrap();

        // Check hash
        assert_eq!(KVPair::<Bytes, Bytes>::decode_hash(&encoded), hash);

        // Check key and value
        let (k, v, _) = KVPair::<Bytes, Bytes>::decode_key_value(&encoded).unwrap();
        assert_eq!(k, b"key_data");
        assert_eq!(v, b"value_data");
    }

    #[test]
    fn test_kvpair_mixed_types() {
        let hash: u64 = 0xABCDEF;
        let key: u64 = 999;
        let value = "string_value";

        let encoded =
            KVPair::<Native<u64>, Str>::encode(hash, &key, value, |size| vec![0; size]).unwrap();

        let (k, v, _) = KVPair::<Native<u64>, Str>::decode_key_value(&encoded).unwrap();
        assert_eq!(k, 999u64);
        assert_eq!(v, "string_value");
    }
}
