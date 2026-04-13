use crate::api::FileManager;
use crate::file_catalog::FileCatalog;
use page::PAGE_SIZE;
use page::page_id::{FileId, PageId};
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::errors::FileManagerError;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

/// A disk based file manager
#[derive(Debug)]
pub struct DiskFileManager {
    files: RwLock<HashMap<FileId, Arc<File>>>,
    file_catalog: Arc<FileCatalog>,
}

impl FileManager for DiskFileManager {
    fn new(file_catalog: Arc<FileCatalog>) -> Self {
        Self {
            files: RwLock::new(HashMap::new()),
            file_catalog,
        }
    }

    fn read_page(&self, page_id: PageId, destination: &mut [u8]) -> Result<(), FileManagerError> {
        let file = self.get_or_open_file(page_id.file_id)?;

        let offset = ((page_id.page_number as usize) * (PAGE_SIZE)) as u64;

        let bytes_read = Self::read_at(file.as_ref(), destination, offset)?;
        if bytes_read == PAGE_SIZE {
            Ok(())
        } else {
            Err(FileManagerError::DidNotReadAllBytes {
                page_id,
                bytes_read,
            })
        }
    }

    fn write_page(&self, page_id: PageId, page_data: &[u8]) -> Result<(), FileManagerError> {
        let file = self.get_or_open_file(page_id.file_id)?;

        let offset = ((page_id.page_number as usize) * (PAGE_SIZE)) as u64;

        let mut written = 0;
        while written < PAGE_SIZE {
            let n = Self::write_at(
                file.as_ref(),
                &page_data[written..],
                offset + written as u64,
            )?;

            if n == 0 {
                return Err(FileManagerError::WroteZeroBytes { page_id });
            }

            written += n;
        }

        if written != PAGE_SIZE {
            return Err(FileManagerError::DidNotWriteAllBytes {
                page_id,
                bytes_written: written,
            });
        }

        Ok(())
    }
}

impl DiskFileManager {
    /// Attempt to either retrieve the file handle or open one if not already present in memory
    /// Returns `Arc<File>` if successful, `FileManagerError` if something goes wrong
    fn get_or_open_file(&self, file_id: FileId) -> Result<Arc<File>, FileManagerError> {
        // First check to see if the file has already been opened - if yes we can return early.
        {
            let files = self.files.read().unwrap();
            if let Some(file) = files.get(&file_id) {
                return Ok(Arc::clone(file));
            }
        }

        // Since we don't have it yet, we need to first lock the map to insert a new entry for this file
        let mut files = self.files.write().unwrap();

        // Double check in case someone got there first
        if let Some(file) = files.get(&file_id) {
            return Ok(Arc::clone(file));
        }

        // Find the path to the file
        let path = self
            .file_catalog
            .get_file_name(file_id)
            .ok_or(FileManagerError::FilePathNotFound { file_id })?;

        // Create parent directory if needed
        Self::ensure_parent_dir(&path);

        // Open the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let file = Arc::new(file);

        files.insert(file_id, Arc::clone(&file));

        Ok(file)
    }

    /// OS Specific method to read an array of bytes from a provided offset in the file
    /// Returns the number of bytes read
    #[inline]
    fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        #[cfg(unix)]
        {
            file.read_at(buf, offset)
        }

        #[cfg(windows)]
        {
            file.seek_read(buf, offset)
        }
    }

    /// OS Specific method to write a byte array at a provided offset in the file
    /// Returns the number of bytes written
    #[inline]
    fn write_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        #[cfg(unix)]
        {
            file.write_at(buf, offset)
        }

        #[cfg(windows)]
        {
            file.seek_write(buf, offset)
        }
    }

    /// Ensures the data directory storing all the database files exists
    /// If it does not exist, it creates it.
    ///
    /// ## Returns
    /// - `()` - nothing, as the method will cause a panic if the directory cannot be created.
    fn ensure_parent_dir(path: &Path) {
        if let Some(parent) = path.parent() {
            // In this case, we want the whole thing to break, since we cannot work
            // if the parent directory cannot be created. `expect` is valid here
            fs::create_dir_all(parent).unwrap_or_else(|_| {
                panic!(
                    "Failed to create data directory: {:?}. Please check permissions!",
                    path
                )
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn write_and_read_full_page_roundtrip() {
        // Create temp dir and file
        let dir = tempdir().expect("tempdir");
        let file_path = dir.path().join("data.db");

        let catalog = FileCatalog::new();
        catalog.add_file(1, file_path.clone());

        let manager = DiskFileManager::new(Arc::new(catalog));

        // Prepare page-sized buffer with a pattern
        let write_buf = vec![0xABu8; PAGE_SIZE];

        // Write page 0
        manager
            .write_page(PageId::new(1, 0), &write_buf)
            .expect("write_page failed");

        // Read back
        let mut read_buf = vec![0u8; PAGE_SIZE];
        manager
            .read_page(PageId::new(1, 0), &mut read_buf)
            .expect("read_page failed");

        assert_eq!(read_buf, write_buf);
    }

    #[test]
    fn reading_unwritten_page_returns_did_not_read_all_bytes() {
        let dir = tempdir().expect("tempdir");
        let file_path = dir.path().join("data2.db");

        let catalog = FileCatalog::new();
        catalog.add_file(2, file_path.clone());

        let manager = DiskFileManager::new(Arc::new(catalog));

        // Attempt to read page 1 (not yet written) - should return DidNotReadAllBytes
        let mut read_buf = vec![0u8; PAGE_SIZE];
        let res = manager.read_page(PageId::new(2, 1), &mut read_buf);

        match res {
            Err(FileManagerError::DidNotReadAllBytes {
                page_id,
                bytes_read: _,
            }) => {
                assert_eq!(page_id, PageId::new(2, 1));
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[test]
    fn write_short_buffer_returns_wrote_zero_bytes() {
        let dir = tempdir().expect("tempdir");
        let file_path = dir.path().join("short.db");

        let catalog = FileCatalog::new();
        catalog.add_file(10, file_path.clone());

        let manager = DiskFileManager::new(Arc::new(catalog));

        // Provide a buffer smaller than PAGE_SIZE - this should eventually trigger WroteZeroBytes
        let short_buf = vec![0xCDu8; PAGE_SIZE - 16];

        let res = manager.write_page(PageId::new(10, 0), &short_buf);

        match res {
            Err(FileManagerError::WroteZeroBytes { page_id }) => {
                assert_eq!(page_id, PageId::new(10, 0));
            }
            other => panic!("expected WroteZeroBytes, got: {:?}", other),
        }
    }

    #[test]
    fn missing_catalog_entry_returns_file_path_not_found() {
        let catalog = FileCatalog::new();
        let manager = DiskFileManager::new(Arc::new(catalog));

        let buf = vec![0u8; PAGE_SIZE];
        let res = manager.write_page(PageId::new(99, 0), &buf);

        match res {
            Err(FileManagerError::FilePathNotFound { file_id }) => assert_eq!(file_id, 99),
            other => panic!("expected FilePathNotFound, got: {:?}", other),
        }
    }

    #[test]
    fn nested_parent_dir_created_and_write_reads_ok() {
        let dir = tempdir().expect("tempdir");
        let file_path = dir.path().join("a/b/c/nested.db");

        let catalog = FileCatalog::new();
        catalog.add_file(42, file_path.clone());

        let manager = DiskFileManager::new(Arc::new(catalog));

        let write_buf = vec![0xEEu8; PAGE_SIZE];
        manager
            .write_page(PageId::new(42, 3), &write_buf)
            .expect("write_page failed");

        let mut read_buf = vec![0u8; PAGE_SIZE];
        manager
            .read_page(PageId::new(42, 3), &mut read_buf)
            .expect("read_page failed");

        assert_eq!(read_buf, write_buf);
        // Ensure the file actually exists on disk
        assert!(file_path.exists());
    }
}
