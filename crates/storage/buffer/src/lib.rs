//! Buffer management module for storage system.

pub mod buffer;
mod errors;
mod frame;

/// Exposes `guard`-like structs that will provide the access to the `Page` instances
/// from the buffer via `&Page`
pub mod guards;
