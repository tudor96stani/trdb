//! This crate provides abstractions and implementations for data and index pages.

#![allow(unused)] // Silence compiler warnings about unused code until they are referenced in main binary. TODO: remove this

#[allow(missing_docs)]
pub mod header;

/// Abstractions and interaction with data and index pages.
pub mod page;

/// Errors related to page operations.
pub mod page_error;
