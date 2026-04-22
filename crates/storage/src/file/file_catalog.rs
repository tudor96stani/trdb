use crate::page::metadata::page_id::FileId;
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
    pub fn get_file_name(&self, file_id: FileId) -> Option<PathBuf> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_returns_empty_catalog() {
        let catalog = FileCatalog::default();

        assert_eq!(catalog.mappings.read().unwrap().len(), 0);
    }

    #[test]
    fn get_file_name_no_match_returns_none() {
        let catalog = FileCatalog::new();
        let file = catalog.get_file_name(1);
        assert!(file.is_none());
    }

    #[test]
    fn get_file_name_match_returns_some() {
        let catalog = FileCatalog::new();
        let expected_path = PathBuf::from("some/path");
        catalog
            .mappings
            .write()
            .unwrap()
            .insert(1, expected_path.clone());

        let result = catalog.get_file_name(1);

        assert_eq!(result, Some(expected_path));
    }

    #[test]
    fn add_file_new_entry_inserted() {
        let catalog = FileCatalog::new();
        let expected_path = PathBuf::from("some/path");

        catalog.add_file(1, expected_path.clone());

        assert_eq!(catalog.mappings.read().unwrap().len(), 1);
        assert_eq!(
            catalog.mappings.read().unwrap().get(&1).unwrap(),
            &expected_path
        );
    }

    #[test]
    fn add_file_existing_entry_updated() {
        let catalog = FileCatalog::new();
        let expected_path = PathBuf::from("updated/path");
        catalog.add_file(1, PathBuf::from("initial/path"));

        catalog.add_file(1, expected_path.clone());

        assert_eq!(catalog.mappings.read().unwrap().len(), 1);
        assert_eq!(
            catalog.mappings.read().unwrap().get(&1).unwrap(),
            &expected_path
        );
    }
}
