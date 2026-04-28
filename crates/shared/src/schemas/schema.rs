use crate::schemas::ObjectId;
use crate::schemas::data_type::DataType;
use std::iter::Map;

/// An in memory representation of the schema of a table or index
#[derive(Debug)]
pub struct Schema {
    /// The ID of the owner (table/index) of this schema
    owner_id: ObjectId,
    /// The list of columns in the schema, mapped via their `ColumnId`
    columns: Map<ColumnId, SchemaEntry>,
}

/// A single entry in the `Schema` container
#[derive(Debug)]
pub struct SchemaEntry {
    /// Unique identifier of the column
    id: ColumnId,
    /// Name of the column
    name: String,
    /// Data type of the column
    data_type: DataType,
    /// The position of the
    position: usize,
    /// Flag indicating whether column is part of the primary key
    is_pk: bool,
}

/// Represents the unique identifier of a column within a `SchemaEntry`.
#[derive(Debug)]
pub struct ColumnId {
    /// The ID of the owner of this column (table or index id)
    owner_id: ObjectId,
    /// The unique ID of the column within the owner's namespace
    column_id: ObjectId,
}
