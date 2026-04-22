/// Provides the implementation for the main buffer leveraged by the engine
pub mod buffer_manager;

/// Errors for the buffer module
pub(crate) mod errors;

/// Exposes `guard`-like structs that will provide the access to the `Page` instances
/// from the buffer via `&Page`
pub(crate) mod guards;

/// Defines the in memory frames used by the buffer manager to cache data pages
mod frame;
