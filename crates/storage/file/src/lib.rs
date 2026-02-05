//! The `file` crate is responsible for the implementation of interaction between the engine and the file system.
//! Its main logic centers around retrieving from/writing to disk data pages.

#![allow(unused)] // Silence compiler warnings about unused code until they are referenced in main binary. TODO: remove this

pub mod in_memory_file_manager;

pub mod api;

pub mod file_catalog;
