//! The storage manager
use buffer::buffer::BufferManager;
use buffer::errors::BufferError;
use buffer::guards::{PageReadGuard, PageWriteGuard};
use file::api::FileManager;
use file::errors::FileManagerError;
use file::file_catalog::FileCatalog;
use page::page::api::Page;
use page::page_id::PageId;
use std::sync::Arc;
use thiserror::Error;

/// The storage manager
#[derive(Debug)]
pub struct StorageManager<F: FileManager> {
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

    /// Obtain a `&Page` via `PageReadGuard` for the provided `PageId`
    pub fn read_page(&self, page_id: PageId) -> Result<PageReadGuard<'_>, StorageErrors> {
        self.buffer_manager
            .read_page(page_id)
            .map_err(StorageErrors::ReadPage)
    }

    /// Obtain a `&mut Page` via `PageWriteGuard` for the provided `PageId`
    pub fn read_page_mut(&self, page_id: PageId) -> Result<PageWriteGuard<'_>, StorageErrors> {
        self.buffer_manager
            .read_page_mut(page_id)
            .map_err(StorageErrors::ReadPage)
    }

    /// Initialize a new `Page` on the buffer for the provided `PageId` and obtain a `&mut Page`
    /// via a `PageWriteGuard`
    pub fn new_page(&self, page_id: PageId) -> Result<PageWriteGuard<'_>, StorageErrors> {
        self.buffer_manager
            .allocate_new_page(page_id)
            .map_err(StorageErrors::NewPage)
    }

    /// Writes a page
    pub fn write_page(
        &self,
        page_id: PageId,
        guard: PageWriteGuard<'_>,
    ) -> Result<(), StorageErrors> {
        self.buffer_manager
            .write_page(page_id, guard)
            .map_err(StorageErrors::WritePage)
    }
}

/// Public storage API errors
#[derive(Debug, Error)]
pub enum StorageErrors {
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
