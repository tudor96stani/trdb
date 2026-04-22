use crate::page::metadata::page_id::{FileId, PageId};
use thiserror::Error;

/// All errors exposed by the `file` crate
#[derive(Debug, Error)]
pub enum FileManagerError {
    /// Could not find path to file based on requested `file_id`
    #[error("Could not find path for file with ID {file_id}")]
    FilePathNotFound {
        /// The ID of the file for which the path was requested
        file_id: FileId,
    },
    /// IO error from std libraries
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Wrote 0 bytes
    #[error("Wrote 0 bytes for page with ID {page_id}")]
    WroteZeroBytes {
        /// The ID of the page for which the error occurred
        page_id: PageId,
    },
    /// Unable to write an entire page to disk
    #[error(
        "Did not write PAGE_SIZE bytes for page with ID {page_id}. Only managed to write {bytes_written}"
    )]
    DidNotWriteAllBytes {
        /// The ID of the page for which the error occurred
        page_id: PageId,
        /// The actual number of bytes written
        bytes_written: usize,
    },
    /// Unable to read an entire page off the disk
    #[error(
        "Did not read PAGE_SIZE bytes for page with ID {page_id}. Only managed to read {bytes_read}"
    )]
    DidNotReadAllBytes {
        /// The ID of the page for which the error occurred
        page_id: PageId,
        /// The actual number of bytes read
        bytes_read: usize,
    },
}
