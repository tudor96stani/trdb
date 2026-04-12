//! The `file` crate is responsible for the implementation of interaction between the engine and the file system.
//! Its main logic centers around retrieving from/writing to disk data pages.

/// Public API for the `file` crate
pub mod api;

/// A file catalog mapping file IDs to their file names
pub mod file_catalog;

/// The actual disk based file manager
pub mod disk_file_manager;

/// Errors for the File crate
pub mod errors;
