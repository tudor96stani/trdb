// The header module contains a doc comment with some links to constants defined in the same file. Due to a cargo doc bug,
// adding module documentation here breaks those links. See https://github.com/rust-lang/rust/issues/119965

/// Errors exposed by the `page` module
pub mod errors;

mod behavior;
pub mod layout;
/// Metadata definitions for a page (`PageId`, `PageType`, etc)
pub mod metadata;

#[cfg(test)]
mod tests;

/// Fixed-size of a page in bytes
pub const PAGE_SIZE: usize = 4096;

/// Size of the header in bytes.
const HEADER_SIZE: usize = 96;
