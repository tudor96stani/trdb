//! This module contains structs and implementations for schema definitions, data types, etc.

/// Defines data types
pub mod data_type;

/// Defines `Schema` and related structs
pub mod schema;

/// Unique identifier for an object within the database engine (table/index/etc)
pub type ObjectId = u16;
