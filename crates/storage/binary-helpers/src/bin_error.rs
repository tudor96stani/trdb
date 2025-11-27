use std::array::TryFromSliceError;
use thiserror::Error;

/// Errors that can occur when working with binary data.
#[derive(Error, Debug)]
pub enum BinaryError {
    /// Error indicating that a byte slice does not have the expected size.
    #[error(
        "Attempt to read {expected} bytes from {from_offset}, but provided slice does not contain the expected range."
    )]
    BytesSliceSizeMismatch { expected: usize, from_offset: usize },
    /// Error indicating a failure to convert a slice to an array.
    #[error("Error converting a slice")]
    SliceConversionError(#[from] TryFromSliceError),
    /// Error indicating a size mismatch when writing data.
    #[error(
        "Error when attempting to write data due to size mismatch: source {src} vs target {target}"
    )]
    WriteErrorSliceSizeMismatch { src: usize, target: usize },
}
