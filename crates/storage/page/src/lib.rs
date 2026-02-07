//! This crate provides abstractions and implementations for data and index pages.

#![allow(unused)] // Silence compiler warnings about unused code until they are referenced in main binary. TODO: remove this

// The header module contains a doc comment with some links to constants defined in the same file. Due to a cargo doc bug,
// adding module documentation here breaks those links. See https://github.com/rust-lang/rust/issues/119965
pub mod header;

/// Slotted structure and related functionality.
pub mod page;

/// Unique identifier for pages.
pub mod page_id;

mod errors;
pub mod insertion_plan;
/// Different types of pages supported.
pub mod page_type;
mod slot;
mod slot_array;
mod tests;

/// Fixed-size of a page in bytes
pub const PAGE_SIZE: usize = 4096;

/// Size of the header in bytes.
const HEADER_SIZE: usize = 96;
