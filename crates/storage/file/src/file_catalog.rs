//! A file catalog mapping file IDs to their file names

use page::page_id::FileId;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

/// Catalog holding the mappings between a `FileId` (a `u32`) and its corresponding filename (represented as a `PathBuf`)
#[derive(Debug)]
pub struct FileCatalog {
    mappings: RwLock<HashMap<FileId, PathBuf>>,
}

impl Default for FileCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl FileCatalog {
    /// Creates a new empty `FileCatalog`
    pub fn new() -> Self {
        Self {
            mappings: RwLock::new(HashMap::new()),
        }
    }

    /// Resolve a `file_id` to a file name
    ///
    /// # Params
    /// - `file_id` (`u32`): the ID of the file to resolve
    ///
    /// # Returns
    /// `Option<PathBuf>` containing a `PathBuf` for the file name, if the provided `file_id` was registered in the catalog
    pub(crate) fn get_file_name(&self, file_id: FileId) -> Option<PathBuf> {
        let guard = self
            .mappings
            .read()
            .expect("FileCatalog poisoned: another thread panicked while holding the lock");
        guard.get(&file_id).cloned()
    }

    /// Registers a new mapping in the catalog for the provided data
    ///
    /// # Params
    /// - `file_id` (`u32`): the ID of the file to register
    /// - `path` (`PathBuf`): the name of the file to register
    pub fn add_file(&self, file_id: FileId, path: PathBuf) {
        let mut guard = self
            .mappings
            .write()
            .expect("FileCatalog poisoned: another thread panicked while holding the lock");
        guard.insert(file_id, path);
    }
}
