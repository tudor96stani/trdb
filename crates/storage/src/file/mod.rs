/// Public API for the `file` crate
pub mod api;
/// The actual disk based file manager
pub mod disk_file_manager;
/// Errors for the File crate
pub mod errors;
/// A file catalog mapping file IDs to their file names
pub mod file_catalog;
