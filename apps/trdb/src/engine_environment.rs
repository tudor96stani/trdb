use crate::config::EngineConfig;
use buffer::buffer::BufferManager;
use file::api::FileManager;
use file::disk_file_manager::DiskFileManager;
use file::file_catalog::FileCatalog;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use storage_api::storage_manager::StorageManager;

/// Owner of the singleton-like instances that are needed for the entire lifetime of the server
#[derive(Debug)]
pub struct EngineEnvironment {
    pub file_manager: Arc<DiskFileManager>,
    pub buffer: Arc<BufferManager<DiskFileManager>>,
    pub storage: Arc<StorageManager<DiskFileManager>>,
    pub file_catalog: Arc<FileCatalog>,
    pub engine_config: EngineConfig,
}

impl EngineEnvironment {
    pub fn new(config: EngineConfig) -> Self {
        let file_catalog = Arc::new(FileCatalog::new());
        let file_manager = Arc::new(DiskFileManager::new(file_catalog.clone()));
        let buffer = Arc::new(BufferManager::new(
            file_manager.clone(),
            config.storage.buffer_pages.get(),
        ));
        let storage = Arc::new(StorageManager::new(file_manager.clone(), buffer.clone()));
        Self {
            file_manager,
            buffer,
            storage,
            file_catalog,
            engine_config: config,
        }
    }

    pub fn setup_test_data(&self) {
        let path = self.engine_config.storage.data_dir.join("test.tbl");
        let full_path = std::path::absolute(&path).unwrap();
        tracing::info!("Full path: {}", full_path.display());
        self.file_catalog.add_file(1, path)
    }
}
