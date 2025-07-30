use std::borrow::Cow;
use std::error::Error;
use std::marker::PhantomData;

/// Trait for encoding types into byte representation
pub trait BytesEncode<'a> {
    type EItem: 'a + ?Sized;

    /// Encode an item into bytes
    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn Error + Sync + Send>>;
}

/// Trait for decoding types from byte representation
pub trait BytesDecode<'a> {
    type DItem: 'a;

    /// Decode bytes into an item
    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error + Sync + Send>>;
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
impl<'a, T> BytesEncode<'a> for Native<T>
where
    T: bytemuck::Pod,
{
    type EItem = T;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn Error + Sync + Send>> {
        Ok(Cow::Borrowed(bytemuck::bytes_of(item)))
    }
}

impl<'a, T> BytesDecode<'a> for Native<T>
where
    T: bytemuck::Pod + Copy,
{
    type DItem = T;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error + Sync + Send>> {
        if bytes.len() != std::mem::size_of::<T>() {
            return Err(format!(
                "Invalid byte length for {}: expected {}, got {}",
                std::any::type_name::<T>(),
                std::mem::size_of::<T>(),
                bytes.len()
            )
            .into());
        }
        Ok(*bytemuck::from_bytes(bytes))
    }
}

// Implementations for Str
impl<'a> BytesEncode<'a> for Str {
    type EItem = str;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn Error + Sync + Send>> {
        Ok(Cow::Borrowed(item.as_bytes()))
    }
}

impl<'a> BytesDecode<'a> for Str {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error + Sync + Send>> {
        std::str::from_utf8(bytes).map_err(|e| e.into())
    }
}

// Implementations for Bytes
impl<'a> BytesEncode<'a> for Bytes {
    type EItem = [u8];

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn Error + Sync + Send>> {
        Ok(Cow::Borrowed(item))
    }
}

impl<'a> BytesDecode<'a> for Bytes {
    type DItem = &'a [u8];

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error + Sync + Send>> {
        Ok(bytes)
    }
}
