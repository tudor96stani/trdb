use crate::buffer::buffer_manager::BufferManager;
use crate::buffer::guards::{PageReadGuard, PageWriteGuard};
use crate::errors::StorageError;
use crate::file::api::FileManager;
use crate::page::metadata::page_id::PageId;
use std::sync::Arc;

/// The storage manager
#[derive(Debug)]
pub struct StorageManager<F: FileManager> {
    #[expect(unused)]
    // unused for now, but will be needed in the future, this should give us a warning once the field is actually used
    file_manager: Arc<F>,
    buffer_manager: Arc<BufferManager<F>>,
}

impl<F: FileManager> StorageManager<F> {
    /// Creates a new instance of the `StorageManager`
    pub fn new(file_manager: Arc<F>, buffer_manager: Arc<BufferManager<F>>) -> Self {
        Self {
            file_manager,
            buffer_manager,
        }
    }

    /// Obtain a `&Page` via `PageReadGuard` for the provided `PageId`.
    ///
    /// # Params
    /// - `page_id`: the requested `PageId`
    ///
    /// # Returns
    /// - `PageReadGuard` if page is successfully retrieved in memory
    /// - `StorageError` otherwise
    pub fn read_page(&self, page_id: PageId) -> Result<PageReadGuard<'_>, StorageError> {
        self.buffer_manager
            .read_page(page_id)
            .map_err(StorageError::ReadPage)
    }

    /// Obtain a `&mut Page` via `PageWriteGuard` for the provided `PageId`
    ///
    /// # Params
    /// - `page_id`: the requested `PageId`
    ///
    /// # Returns
    /// - `PageWriteGuard` if page is successfully retrieved in memory
    /// - `StorageError` otherwise
    pub fn read_page_mut(&self, page_id: PageId) -> Result<PageWriteGuard<'_>, StorageError> {
        self.buffer_manager
            .read_page_mut(page_id)
            .map_err(StorageError::ReadPage)
    }

    /// Initialize a new `Page` on the buffer for the provided `PageId` and obtain a `&mut Page`
    /// via a `PageWriteGuard`
    ///
    /// # Params
    /// - `page_id`: the requested `PageId`
    ///
    /// # Returns
    /// - `PageWriteGuard` if page is successfully retrieved in memory
    /// - `StorageError` otherwise
    pub fn new_page(&self, page_id: PageId) -> Result<PageWriteGuard<'_>, StorageError> {
        self.buffer_manager
            .allocate_new_page(page_id)
            .map_err(StorageError::NewPage)
    }

    /// Writes a page to disk
    ///
    /// # Params
    /// - `page_id`: the `PageId` of the page
    /// - `guard`: the `PageWriteGuard` holding access to the updated page that needs to be written
    ///
    /// # Returns
    /// - `()` if write is successful
    /// - `StorageError` otherwise
    pub fn write_page(
        &self,
        page_id: PageId,
        guard: PageWriteGuard<'_>,
    ) -> Result<(), StorageError> {
        self.buffer_manager
            .write_page(page_id, guard)
            .map_err(StorageError::WritePage)
    }
}
