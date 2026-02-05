//! The storage manager
use buffer::buffer::BufferManager;
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
}
