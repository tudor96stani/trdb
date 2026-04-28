//! Module defining a fixed-size slotted page structure with its associated methods.
//!
//! # Memory Layout Overview
//!
//! A typical slotted page has this physical structure (generalized):
//!
//! ```text
//!   ↓ page_start
//!   ┌───────────────────────────────────────────────────────────────┐
//!   │ Page Header (contains slot_count, free space ptrs, etc.)      │
//!   ├───────────────────────────────────────────────────────────────┤
//!   │ Tuple Data Region (grows upward)                              │
//!   │   records / row fragments                                     │
//!   │   variable sized                                              │
//!   │   aligned upwards                                             │
//!   ├───────────────────────────────────────────────────────────────┤
//!   │ Free Space                                                    │
//!   ├───────────────────────────────────────────────────────────────┤
//!   │ Slot Array Region (grows downward)                            │
//!   │   fixed-size SLOT_SIZE entries                                │
//!   │   indexed logically left-to-right,                            │
//!   │   stored physically right-to-left                             │
//!   └───────────────────────────────────────────────────────────────┘
//!
//!                                                          page_end ↑
//! ```
//!
//! # Why This Design?
//!
//! - Adding a new slot does **not** require moving existing slots.
//! - Tuple movement and compaction only affect the data region.
//! - Both read and write operations are zero-copy and O(1).
//!
//! This module encapsulates that logic cleanly, exposing a safe and API for manipulating the slotted page.
//!
//!
//! Header access is provided via `header::HeaderRef` and `header::HeaderMut` types.
//! Slot array access is provided via `slot::SlotArrayRef` and `slot::SlotArrayMut` types.
pub(crate) mod header;
pub(crate) mod slot;
pub(crate) mod slot_array;

/// Contains the definition of the `Page` struct, the in-memory representation of a data page
pub mod page;
