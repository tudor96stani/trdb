use buffer::buffer::BufferManager;
use file::api::FileManager;
use file::file_catalog::FileCatalog;
use file::in_memory_file_manager::InMemoryFileManager;
use std::sync::Arc;
use storage_api::storage_manager::StorageManager;

/// Owner of the singleton-like instances that are needed for the entire lifetime of the server
#[derive(Debug)]
pub struct EngineEnvironment {
    file_manager: Arc<InMemoryFileManager>,
    buffer: Arc<BufferManager<InMemoryFileManager>>,
    storage: Arc<StorageManager<InMemoryFileManager>>,
    file_catalog: Arc<FileCatalog>,
}

impl EngineEnvironment {
    pub fn new() -> Self {
        let file_catalog = Arc::new(FileCatalog::new());
        let file_manager = Arc::new(InMemoryFileManager::new("p", file_catalog.clone()));
        let buffer = Arc::new(BufferManager::new(file_manager.clone(), 100));
        let storage = Arc::new(StorageManager::new(file_manager.clone(), buffer.clone()));
        Self {
            file_manager,
            buffer,
            storage,
            file_catalog,
        }
    }
}
