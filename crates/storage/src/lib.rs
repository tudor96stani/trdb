//! This crate contains the logic for the storage engine

/// Implementation for the in memory buffer of data pages
pub mod buffer;

/// Implementation for the file manager and direct disk access
pub mod file;

/// Implementation of data pages
pub mod page;

/// Implementation of the storage manager, entry point into this library
pub mod storage_manager;

/// Implementation of exposed errors
pub mod errors;
