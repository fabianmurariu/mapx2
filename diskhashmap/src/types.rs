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

    /// Decode bytes into an item
    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem>;
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
        bytemuck::from_bytes::<T>(l) == bytemuck::from_bytes::<T>(r)
    }

    fn hash_alt<S: Hasher>(item: &[u8], s: &mut S) -> u64 {
        let value = bytemuck::from_bytes::<T>(item);
        value.hash(s);
        s.finish()
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

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        let str_bytes = Self::bytes_actual(bytes);
        std::str::from_utf8(str_bytes).map_err(|e| DiskMapError::Decoding(e.to_string()))
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

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
        Ok(Self::bytes_actual(bytes))
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

        fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem> {
            let actual_bytes = Self::bytes_actual(bytes);
            ::rkyv::access::<::rkyv::Archived<T>, ::rkyv::rancor::Error>(actual_bytes)
                .map_err(|e| DiskMapError::Decoding(e.to_string()))
        }
    }
}
