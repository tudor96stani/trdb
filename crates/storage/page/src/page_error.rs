use thiserror::Error;

/// Errors that can occur when reading/writing data to/from pages. Internal to the page storage crates only.
#[derive(Debug, Error)]
pub enum PageError {
    #[error("Header error")]
    HeaderReadError(#[from] binary_helpers::bin_error::BinaryError),
}
