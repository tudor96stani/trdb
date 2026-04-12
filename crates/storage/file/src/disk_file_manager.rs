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
