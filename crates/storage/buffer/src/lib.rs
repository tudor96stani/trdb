//! Buffer management module for storage system.

/// Provides the implementation for the main buffer leveraged by the engine
pub mod buffer;

/// Errors for the buffer module
pub mod errors;

/// Exposes `guard`-like structs that will provide the access to the `Page` instances
/// from the buffer via `&Page`
pub mod guards;

/// Defines the in memory frames used by the buffer manager to cache data pages
mod frame;
