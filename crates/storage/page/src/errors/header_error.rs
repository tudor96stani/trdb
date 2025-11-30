use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum HeaderError {
    #[error("Header error")]
    BinaryError(#[from] binary_helpers::bin_error::BinaryError),
    #[error("Arithmetic error while computing offsets within header")]
    OffsetArithmetic,
}
