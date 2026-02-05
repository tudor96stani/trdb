//! The storage manager
use buffer::buffer::BufferManager;
use buffer::guards::{PageReadGuard, PageWriteGuard};
use file::api::FileManager;
use file::file_catalog::FileCatalog;
use file::in_memory_file_manager::InMemoryFileManager;
use page::impls::Page;
use page::page_id::PageId;
use std::sync::Arc;

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
    pub fn read_page(&self, page_id: PageId) -> PageReadGuard<'_> {
        match self.buffer_manager.read_page(page_id) {
            Ok(page_read_guard) => page_read_guard,
            Err(_) => panic!("buffer manager poisoned"),
        }
    }

    /// Obtain a `&mut Page` via `PageWriteGuard` for the provided `PageId`
    pub fn write_page(&self, page_id: PageId) -> PageWriteGuard<'_> {
        match self.buffer_manager.read_page_mut(page_id) {
            Ok(page_read_guard) => page_read_guard,
            Err(_) => panic!("buffer manager poisoned"),
        }
    }

    /// Initialize a new `Page` on the buffer for the provided `PageId` and obtain a `&mut Page`
    /// via a `PageWriteGuard`
    pub fn new_page(&self, page_id: PageId) -> PageWriteGuard<'_> {
        match self.buffer_manager.allocate_new_page(page_id) {
            Ok(page_write_guard) => page_write_guard,
            Err(_) => panic!("buffer manager poisoned"),
        }
    }
}
