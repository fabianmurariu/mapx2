use crate::error::{DiskMapError, Result};
use std::hash::BuildHasher;
use std::marker::PhantomData;
use std::ops::Deref;

use rkyv::Archive;
use rkyv::api::high::{HighSerializer, HighValidator};
use rkyv::bytecheck::CheckBytes;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;

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

impl<'a> AsRef<[u8]> for CowBytes<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            CowBytes::Borrowed(item) => item,
            CowBytes::Owned(item) => item.as_ref().as_ref(),
        }
    }
}

impl<'a> Deref for CowBytes<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            CowBytes::Borrowed(item) => item,
            CowBytes::Owned(item) => item.as_ref().as_ref(),
        }
    }
}

/// Trait for encoding types into byte representation
pub trait BytesEncode<'a> {
    type EItem: 'a + ?Sized;

    /// Encode an item into bytes
    fn bytes_encode(item: &'a Self::EItem) -> Result<CowBytes<'a>>;

    fn eq_alt(l: &[u8], r: &[u8]) -> bool {
        l == r
    }

    fn hash_alt<S: BuildHasher>(item: &[u8], s: &S) -> u64 {
        s.hash_one(item)
    }
}

/// Trait for decoding types from byte representation
pub trait BytesDecode<'a> {
    type DItem: 'a;

    /// Decode bytes into an item
    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem>;
}

// pub trait BytesEqHash<'a>: BytesEncode<'a> + BytesDecode<'a> + Default {
//     /// Check if two items are equal
//     fn bytes_eq(item1: &'a Self::EItem, item2: &'a Self::EItem) -> bool {}

//     /// Get a hash of the item
//     fn bytes_hash(item: &'a Self::EItem) -> u64;
// }

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
impl<'a, T> BytesEncode<'a> for Native<T>
where
    T: bytemuck::Pod + Eq + std::hash::Hash,
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<CowBytes<'a>> {
        Ok(CowBytes::Borrowed(bytemuck::bytes_of(item)))
    }

    fn eq_alt(l: &[u8], r: &[u8]) -> bool {
        bytemuck::from_bytes::<T>(l) == bytemuck::from_bytes::<T>(r)
    }

    fn hash_alt<S: BuildHasher>(item: &[u8], s: &S) -> u64 {
        let value = bytemuck::from_bytes::<T>(item);
        s.hash_one(value)
    }
}

impl<'a, T> BytesDecode<'a> for Native<T>
where
    T: bytemuck::Pod + Copy,
{
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        if bytes.len() != std::mem::size_of::<T>() {
            return Err(DiskMapError::Decoding(format!(
                "Invalid byte length for {}: expected {}, got {}",
                std::any::type_name::<T>(),
                std::mem::size_of::<T>(),
                bytes.len()
            )));
        }
        Ok(*bytemuck::from_bytes(bytes))
    }
}

// Implementations for Str
impl<'a> BytesEncode<'a> for Str {
    type EItem = str;

    fn bytes_encode(item: &'a Self::EItem) -> Result<CowBytes<'a>> {
        Ok(CowBytes::Borrowed(item.as_bytes()))
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        std::str::from_utf8(bytes).map_err(|e| DiskMapError::Decoding(e.to_string()))
    }
}

// Implementations for Bytes
impl<'a> BytesEncode<'a> for Bytes {
    type EItem = [u8];

    fn bytes_encode(item: &'a Self::EItem) -> Result<CowBytes<'a>> {
        Ok(CowBytes::Borrowed(item))
    }
}

impl<'a> BytesDecode<'a> for Bytes {
    type DItem = &'a [u8];

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        Ok(bytes)
    }
}

pub struct Arch<T>(PhantomData<T>);

impl<
    'a,
    T: for<'b> rkyv::Serialize<HighSerializer<AlignedVec, ArenaHandle<'b>, rkyv::rancor::Error>> + 'a,
> BytesEncode<'a> for Arch<T>
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<CowBytes<'a>> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(item)
            .map_err(|e| DiskMapError::Serialization(e.to_string()))?;
        Ok(CowBytes::owned(bytes))
    }
}

impl<'a, T> BytesDecode<'a> for Arch<T>
where
    T: 'a + rkyv::Archive,
    T::Archived: for<'b> CheckBytes<HighValidator<'b, rkyv::rancor::Error>>,
{
    type DItem = &'a <T as Archive>::Archived;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        rkyv::access::<rkyv::Archived<T>, rkyv::rancor::Error>(bytes)
            .map_err(|e| DiskMapError::Decoding(e.to_string()))
    }
}
