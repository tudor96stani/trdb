//! Module defining a fixed-size slotted page structure with its associated methods.
//!
//! # Memory Layout Overview
//!
//! A typical slotted page has this physical structure (generalized):
//!
//! ```text
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
//!                     ↑ page_start                        page_end ↑
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
use crate::PAGE_SIZE;
use crate::errors::page_error::{PageResult, WithPageId};
use crate::errors::page_op_error::PageOpError;
use crate::insertion_plan::InsertionPlan;
use crate::page_id::PageId;

pub(crate) mod accessors;
pub(crate) mod ctors;
pub(crate) mod delete;
pub(crate) mod insert;
pub(crate) mod internal;
pub(crate) mod plan_insert;
pub(crate) mod read_row;
pub(crate) mod update;

/// Public API for the `Page` struct
pub mod api;
