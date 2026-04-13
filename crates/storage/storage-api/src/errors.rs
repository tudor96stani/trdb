use buffer::errors::BufferError;
use thiserror::Error;

/// Public storage API errors
#[derive(Debug, Error)]
pub enum StorageError {
    /// Error while reading page
    #[error("Error while reading page")]
    ReadPage(BufferError),
    /// Error while creating new page
    #[error("Error while creating new page")]
    NewPage(BufferError),
    /// Error while writing page
    #[error("Error while writing page")]
    WritePage(BufferError),
}
