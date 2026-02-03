use crate::api::FileManager;
use crate::file_catalog::FileCatalog;
use page::page_id::PageId;
use std::collections::HashMap;
use std::path::PathBuf;

// A temporary in memory file manager
struct InMemoryFileManager {
    data_dir: PathBuf,
    // For now unused in this implementation, as filenames are not yet needed.
    file_catalog: FileCatalog,
    pages: HashMap<PageId, Box<[u8]>>,
}

impl FileManager for InMemoryFileManager {
    fn new<P>(path: P, file_catalog: FileCatalog) -> Self
    where
        P: Into<PathBuf>,
    {
        Self {
            data_dir: path.into(),
            file_catalog,
            pages: HashMap::new(),
        }
    }

    fn read_page(&self, page_id: PageId, destination: &mut [u8]) -> bool {
        if let Some(page) = self.pages.get(&page_id) {
            destination.copy_from_slice(page);
            true
        } else {
            false
        }
    }

    fn write_page(&mut self, page_id: PageId, page_data: &[u8]) {
        let boxed = page_data.to_vec().into_boxed_slice();
        self.pages.insert(page_id, boxed);
    }
}
