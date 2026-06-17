//! This module contains structs and implementations for schema definitions, data types, etc.

/// Defines data types
pub mod data_type;

/// Defines `Schema` and related structs
pub mod schema;

/// Defines the data structures required for representing a data row in memory
pub mod row;

/// Unique identifier for an object within the database engine (table/index/etc)
pub type ObjectId = u16;
