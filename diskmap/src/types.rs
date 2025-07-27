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

/// Wrapper for string types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Str<S>(PhantomData<S>);

impl<S> Default for Str<S> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

/// Wrapper for byte slice types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Bytes<B>(PhantomData<B>);

impl<B> Default for Bytes<B> {
    fn default() -> Self {
        Self(PhantomData)
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

// Implementations for Str<S>
impl<'a, S> BytesEncode<'a> for Str<S>
where
    S: AsRef<str> + 'a,
{
    type EItem = str;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn Error + Sync + Send>> {
        Ok(Cow::Borrowed(item.as_bytes()))
    }
}

impl<'a> BytesDecode<'a> for Str<&'a str> {
    type DItem = &'a str;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error + Sync + Send>> {
        std::str::from_utf8(bytes).map_err(|e| e.into())
    }
}

impl<'a> BytesDecode<'a> for Str<String> {
    type DItem = String;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error + Sync + Send>> {
        std::str::from_utf8(bytes)
            .map(|s| s.to_owned())
            .map_err(|e| e.into())
    }
}

// Implementations for Bytes<B>
impl<'a, B> BytesEncode<'a> for Bytes<B>
where
    B: AsRef<[u8]> + 'a,
{
    type EItem = B;

    fn bytes_encode(item: &'a Self::EItem) -> Result<Cow<'a, [u8]>, Box<dyn Error + Sync + Send>> {
        Ok(Cow::Borrowed(item.as_ref()))
    }
}

impl<'a> BytesDecode<'a> for Bytes<&'a [u8]> {
    type DItem = &'a [u8];

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error + Sync + Send>> {
        Ok(bytes)
    }
}

impl<'a> BytesDecode<'a> for Bytes<Vec<u8>> {
    type DItem = Vec<u8>;

    fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, Box<dyn Error + Sync + Send>> {
        Ok(bytes.to_vec())
    }
}
