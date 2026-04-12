//! Public API for the `file` crate

use crate::errors::FileManagerError;
use crate::file_catalog::FileCatalog;
use page::page_id::PageId;
use std::path::PathBuf;
use std::sync::Arc;

/// File manager public API
///
/// A `FileManager` manages a collection of fixed-size pages addressed by
/// `PageId`. Implementations are free to choose the backing storage layout and
/// error reporting strategy. The trait itself documents method-level
/// expectations.
pub trait FileManager {
    /// Definition
    /// Create a new file manager instance bound to `path`.
    ///
    /// Params
    /// - `path`: A value convertible into `PathBuf` that identifies the backing
    ///   storage resource for this manager (interpretation left to the
    ///   implementation).
    ///
    /// Return
    /// - `Self`: an instance of the file manager bound to `path`.
    fn new(file_catalog: Arc<FileCatalog>) -> Self;

    /// Definition
    /// Read the page identified by `page_id` into `destination`.
    ///
    /// Params
    /// - `page_id`: Identifier of the page to read.
    /// - `destination`: Caller-provided buffer to receive the page bytes. The
    ///   buffer length must equal the storage page size.
    ///
    /// Returns a `Result<bool, FileModuleErrors>`.
    /// - `Ok(())`:if the page existed and was copied into `destination`;
    /// - `Err` if anything goes wrong
    fn read_page(&self, page_id: PageId, destination: &mut [u8]) -> Result<(), FileManagerError>;

    /// Definition
    /// Write the contents of `page_data` as the page for `page_id`.
    ///
    /// Params
    /// - `page_id`: Identifier of the page to write.
    /// - `page_data`: Byte slice containing exactly one page worth of data. The
    ///   length must equal the storage page size.
    ///
    /// Returns `Result<(), FileModuleErrors>`
    /// - `()`: No value is returned. Implementations control how they report
    ///   internal failures; this trait does not expose an error type yet.
    /// - `Err` if anything goes wrong
    fn write_page(&self, page_id: PageId, page_data: &[u8]) -> Result<(), FileManagerError>;
}
