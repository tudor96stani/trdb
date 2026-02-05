//! Defines a temporary implementation for the `InMemoryFileManager`
//!
use crate::api::FileManager;
use crate::file_catalog::FileCatalog;
use page::page_id::PageId;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// A temporary in memory file manager
#[derive(Debug)]
pub struct InMemoryFileManager {
    data_dir: PathBuf,
    // For now unused in this implementation, as filenames are not yet needed.
    file_catalog: Arc<FileCatalog>,
    pages: RwLock<HashMap<PageId, Box<[u8]>>>,
}

impl FileManager for InMemoryFileManager {
    fn new<P>(path: P, file_catalog: Arc<FileCatalog>) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            data_dir: path.into(),
            file_catalog,
            pages: RwLock::new(HashMap::new()),
        }
    }

    fn read_page(&self, page_id: PageId, destination: &mut [u8]) -> bool {
        if let Some(page) = self.pages.read().unwrap().get(&page_id) {
            destination.copy_from_slice(page);
            true
        } else {
            false
        }
    }

    fn write_page(&self, page_id: PageId, page_data: &[u8]) {
        let boxed = page_data.to_vec().into_boxed_slice();
        self.pages.write().unwrap().insert(page_id, boxed);
    }
}
