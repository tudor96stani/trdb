use crate::page::metadata::page_id::FileId;
use shared::schemas::identifiers::DatabaseId;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

/// Catalog holding the mappings between a `(DatabaseId, FileId)` pair and its corresponding filename (represented as a `PathBuf`)
#[derive(Debug)]
pub struct FileCatalog {
    mappings: RwLock<HashMap<(DatabaseId, FileId), PathBuf>>,
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

    /// Resolve a `file_id` within a database to a file name
    ///
    /// # Params
    /// - `db_id` (`DatabaseId`): the ID of the database owning the file
    /// - `file_id` (`FileId`): the ID of the file to resolve, unique within the database
    ///
    /// # Returns
    /// `Option<PathBuf>` containing a `PathBuf` for the file name, if the provided `(db_id, file_id)` was registered in the catalog
    pub fn get_file_name(&self, db_id: DatabaseId, file_id: FileId) -> Option<PathBuf> {
        let guard = self
            .mappings
            .read()
            .expect("FileCatalog poisoned: another thread panicked while holding the lock");

        guard.get(&(db_id, file_id)).cloned()
    }

    /// Registers a new mapping in the catalog for the provided data
    ///
    /// # Params
    /// - `db_id` (`DatabaseId`): the ID of the database owning the file
    /// - `file_id` (`FileId`): the ID of the file to register, unique within the database
    /// - `path` (`PathBuf`): the name of the file to register
    pub fn add_file(&self, db_id: DatabaseId, file_id: FileId, path: PathBuf) {
        let mut guard = self
            .mappings
            .write()
            .expect("FileCatalog poisoned: another thread panicked while holding the lock");

        guard.insert((db_id, file_id), path);
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
        let file = catalog.get_file_name(DatabaseId::new(1), 1);
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
            .insert((DatabaseId::new(1), 1), expected_path.clone());

        let result = catalog.get_file_name(DatabaseId::new(1), 1);

        assert_eq!(result, Some(expected_path));
    }

    #[test]
    fn add_file_new_entry_inserted() {
        let catalog = FileCatalog::new();
        let expected_path = PathBuf::from("some/path");

        catalog.add_file(DatabaseId::new(1), 1, expected_path.clone());

        assert_eq!(catalog.mappings.read().unwrap().len(), 1);
        assert_eq!(
            catalog
                .mappings
                .read()
                .unwrap()
                .get(&(DatabaseId::new(1), 1))
                .unwrap(),
            &expected_path
        );
    }

    #[test]
    fn add_file_existing_entry_updated() {
        let catalog = FileCatalog::new();
        let expected_path = PathBuf::from("updated/path");
        catalog.add_file(DatabaseId::new(1), 1, PathBuf::from("initial/path"));

        catalog.add_file(DatabaseId::new(1), 1, expected_path.clone());

        assert_eq!(catalog.mappings.read().unwrap().len(), 1);
        assert_eq!(
            catalog
                .mappings
                .read()
                .unwrap()
                .get(&(DatabaseId::new(1), 1))
                .unwrap(),
            &expected_path
        );
    }
}
