use std::array::TryFromSliceError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BinaryError {
    #[error("Attempt to read {expected} bytes from {from_offset}, but provided slice does not contain the expected range.")]
    ReadErrorInvalidSliceSize {expected: usize, from_offset: usize},
    #[error("Error converting a slice")]
    SliceConversionError(#[from] TryFromSliceError),
    #[error("Error when attempting to write data due to size mismatch: source {src} vs target {target}")]
    WriteErrorSliceSizeMismatch { src: usize, target: usize }
}