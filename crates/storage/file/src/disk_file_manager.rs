use crate::api::FileManager;
use crate::file_catalog::FileCatalog;
use page::PAGE_SIZE;
use page::page_id::{FileId, PageId};
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

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

    fn read_page(&self, page_id: PageId, destination: &mut [u8]) -> bool {
        let file = self.get_or_open_file(page_id.file_id);

        let offset = ((page_id.page_number as usize) * (PAGE_SIZE)) as u64;

        matches!(Self::read_at(file.as_ref(), destination, offset), Ok(n) if n == PAGE_SIZE)
    }

    fn write_page(&self, page_id: PageId, page_data: &[u8]) {
        let file = self.get_or_open_file(page_id.file_id);

        let offset = ((page_id.page_number as usize) * (PAGE_SIZE)) as u64;

        let mut written = 0;
        while written < PAGE_SIZE {
            let n = Self::write_at(
                file.as_ref(),
                &page_data[written..],
                offset + written as u64,
            )
            .expect("disk write failed");

            if n == 0 {
                panic!("disk write failed - wrote 0 bytes");
            }

            written += n;
        }
    }
}

impl DiskFileManager {
    fn get_or_open_file(&self, file_id: FileId) -> Arc<File> {
        // 1. Fast path — read lock
        {
            let files = self.files.read().unwrap();
            if let Some(file) = files.get(&file_id) {
                return Arc::clone(file);
            }
        }

        // 2. Slow path — write lock
        let mut files = self.files.write().unwrap();

        // 3. Double-check
        if let Some(file) = files.get(&file_id) {
            return Arc::clone(file);
        }

        // 4. Actually open file
        let path = self
            .file_catalog
            .get_file_name(file_id)
            .expect("File does not exist");

        Self::ensure_parent_dir(&path);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .expect("Failed to open file");

        let file = Arc::new(file);

        files.insert(file_id, Arc::clone(&file));

        file
    }

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

    fn ensure_parent_dir(path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("failed to create data directory");
        }

        Ok(())
    }
}
