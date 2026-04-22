//! This crate contains the logic for the storage engine

// ----------------- BUFFER -------------------------
/// Implementation for the in memory buffer of data pages
mod buffer;

/// Public `BufferManager` struct
pub use buffer::buffer_manager::BufferManager;
// ------------------------------------------------


// ----------------- FILE -------------------------
/// Implementation for the file manager and direct disk access
mod file;
/// `FileManager`` trait
pub use file::api::FileManager;
/// `DiskFileManager` struct, an implementation of `FileManager`
pub use file::disk_file_manager::DiskFileManager;
/// `FileCatalog` struct, responsible for mapping file ids to paths
pub use file::file_catalog::FileCatalog;
// ------------------------------------------------



// ----------------- PAGE -------------------------
/// Implementation of data pages
mod page;
/// `Page` struct, the in memory representation of a data page
pub use crate::page::layout::page::Page;
/// `PageId` struct, the identifier for a page, consisting of a file id and page number
pub use crate::page::metadata::page_id::PageId;
/// `PageType` enum, the type of a page, used to determine how to interpret the data on the page
pub use crate::page::metadata::page_type::PageType;
// ------------------------------------------------


// ----------------- PAGE -------------------------
/// Implementation of the storage manager, entry point into this library
mod storage_manager;
/// `StorageManager` struct, the main entry point into this library, responsible for managing the buffer and file manager
pub use storage_manager::StorageManager;
// ------------------------------------------------

/// Implementation of exposed errors
pub mod errors;