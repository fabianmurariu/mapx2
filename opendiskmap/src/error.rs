use std::io;
use thiserror::Error;

/// Errors that can occur when working with disk hash maps
#[derive(Error, Debug)]
pub enum DiskMapError {
    /// IO errors when reading/writing to disk
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Encoding errors when converting data to bytes
    #[error("Encoding error: {0}")]
    Encoding(String),

    /// Decoding errors when converting bytes back to data
    #[error("Decoding error: {0}")]
    Decoding(String),

    /// Serialization errors from rkyv
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Key not found in the map
    #[error("Key not found")]
    KeyNotFound,

    /// Map is full or capacity exceeded
    #[error("Map capacity exceeded")]
    CapacityExceeded,

    /// Invalid input parameters
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Rkyv error: {0}")]
    RkyvError(#[from] rkyv::rancor::Error),
}

pub type Result<T> = std::result::Result<T, DiskMapError>;

impl From<Box<dyn std::error::Error + Send + Sync>> for DiskMapError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        if let Some(io_err) = err.downcast_ref::<io::Error>() {
            DiskMapError::Io(io::Error::new(io_err.kind(), err.to_string()))
        } else {
            DiskMapError::Encoding(err.to_string())
        }
    }
}

impl From<String> for DiskMapError {
    fn from(msg: String) -> Self {
        DiskMapError::Encoding(msg)
    }
}

impl From<&str> for DiskMapError {
    fn from(msg: &str) -> Self {
        DiskMapError::Encoding(msg.to_string())
    }
}
