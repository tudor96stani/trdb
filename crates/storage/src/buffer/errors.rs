use crate::file::errors::FileManagerError;
use thiserror::Error;

/// Buffer error.
#[derive(Debug, Error)]
pub enum BufferError {
    /// Buffer was full
    #[error("Buffer full")]
    BufferFull,
    /// Error stemming from the File manager
    #[error("File manager error")]
    FileManager(#[from] FileManagerError),
}
