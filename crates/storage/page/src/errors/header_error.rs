use binary_helpers::conversions::ConversionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum HeaderError {
    #[error("Header error")]
    BinaryError(#[from] binary_helpers::bin_error::BinaryError),
    #[error("Arithmetic error while computing offsets within header")]
    OffsetArithmetic,
    #[error("Provided slice length ({actual}) does not match the expected length")]
    HeaderSliceSizeMismatch { actual: usize, expected: usize },
}
