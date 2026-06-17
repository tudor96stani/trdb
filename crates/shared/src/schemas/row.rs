use crate::schemas::ObjectId;
use crate::schemas::schema::{Schema, SchemaEntry, SchemaEntryId};
use std::collections::HashMap;

/// Size of a fixed sized column (equivalent to an integer)
pub const FIXED_LEN_SIZE: usize = 4;

/// Container storing the actual value stored in a row cell
#[derive(Debug)]
pub enum RowCellValue {
    /// Integer value
    Int(i32),
    /// String value
    String(String),
    /// NULL value
    NULL,
}

impl From<i32> for RowCellValue {
    fn from(value: i32) -> Self {
        Self::Int(value)
    }
}

impl From<String> for RowCellValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for RowCellValue {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

/// A single cell from a `Row`, containing both value and schema metadata
#[derive(Debug)]
pub struct RowCell {
    schema_entry_id: SchemaEntryId,
    value: RowCellValue,
}

impl RowCell {
    /// Constructor
    fn new(schema_entry_id: SchemaEntryId, value: RowCellValue) -> Self {
        Self {
            schema_entry_id,
            value,
        }
    }

    /// Getter for schema entry id of cell
    pub fn schema_entry_id(&self) -> SchemaEntryId {
        self.schema_entry_id
    }

    /// Getter for value of cell
    pub fn value(&self) -> &RowCellValue {
        &self.value
    }
}

/// An in memory representation of a row
#[derive(Debug)]
pub struct Row {
    values: HashMap<SchemaEntryId, RowCell>,
}

impl Row {
    /// Constructor
    fn new(values: HashMap<SchemaEntryId, RowCell>) -> Self {
        Self { values }
    }

    /// Creates a `RowBuilder` to be used to generate a new `Row` object
    pub fn builder(schema: &Schema) -> RowBuilder<'_> {
        RowBuilder::new(schema)
    }

    /// Iterator over the cells
    pub fn values(&self) -> impl Iterator<Item = &RowCell> {
        self.values.values()
    }

    /// Returns the cell associated with the given schema entry ID, if present.
    pub fn get(&self, schema_entry_id: &SchemaEntryId) -> Option<&RowCell> {
        self.values.get(schema_entry_id)
    }
}

/// A builder of a `Row`
#[derive(Debug)]
pub struct RowBuilder<'a> {
    schema: &'a Schema,
    values: HashMap<SchemaEntryId, RowCell>,
}

impl<'a> RowBuilder<'a> {
    /// Creates a new `RowBuilder` - not to be used externally. Invoke the `builder()` method on `Row`
    fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            values: HashMap::new(),
        }
    }

    /// Append a new cell to the row builder using an existing schema entry.
    pub fn cell(mut self, schema_entry: &'a SchemaEntry, value: impl Into<RowCellValue>) -> Self {
        let schema_entry_id = schema_entry.id();
        let value = value.into();

        assert!(
            self.schema.columns().contains_key(&schema_entry_id),
            "schema entry does not belong to this row schema",
        );
        assert!(
            Self::value_matches_schema_entry(schema_entry, &value),
            "row cell value does not match schema entry data type",
        );

        self.values
            .insert(schema_entry_id, RowCell::new(schema_entry_id, value));
        self
    }

    /// Append a new value to the row builder by column id.
    pub fn value(self, column_id: ObjectId, value: impl Into<RowCellValue>) -> Self {
        let schema_entry = self.schema_entry_by_column_id(column_id);
        self.cell(schema_entry, value)
    }

    /// Append a new value to the row builder by column position.
    pub fn value_at_position(self, position: usize, value: impl Into<RowCellValue>) -> Self {
        let schema_entry = self.schema_entry_by_position(position);
        self.cell(schema_entry, value)
    }

    /// Append a new value to the row builder by column name.
    pub fn value_named(self, name: &str, value: impl Into<RowCellValue>) -> Self {
        let schema_entry = self.schema_entry_by_name(name);
        self.cell(schema_entry, value)
    }

    /// Builds the final immutable `Row`.
    pub fn build(self) -> Row {
        Row::new(self.values)
    }

    fn schema_entry_by_column_id(&self, column_id: ObjectId) -> &'a SchemaEntry {
        self.schema
            .columns()
            .values()
            .find(|entry| entry.id().column_id() == column_id)
            .unwrap_or_else(|| panic!("schema does not contain column id {column_id}"))
    }

    fn schema_entry_by_position(&self, position: usize) -> &'a SchemaEntry {
        self.schema
            .columns()
            .values()
            .find(|entry| entry.position() == position)
            .unwrap_or_else(|| panic!("schema does not contain column at position {position}"))
    }

    fn schema_entry_by_name(&self, name: &str) -> &'a SchemaEntry {
        self.schema
            .columns()
            .values()
            .find(|entry| entry.name() == name)
            .unwrap_or_else(|| panic!("schema does not contain column named {name}"))
    }

    fn value_matches_schema_entry(schema_entry: &SchemaEntry, value: &RowCellValue) -> bool {
        matches!(
            (schema_entry.data_type(), value),
            (_, RowCellValue::NULL)
                | (
                    crate::schemas::data_type::DataType::Int,
                    RowCellValue::Int(_)
                )
                | (
                    crate::schemas::data_type::DataType::String,
                    RowCellValue::String(_)
                )
        )
    }
}

#[cfg(test)]
mod row_builder_tests {
    use super::*;
    use crate::schemas::data_type::DataType;

    #[test]
    fn test_row_builder_creates_cells_from_column_ids() {
        let schema = Schema::builder(1)
            .column("id", DataType::Int, 0, true)
            .column("name", DataType::String, 1, false)
            .build();

        let row = Row::builder(&schema).value(0, 10).value(1, "Ada").build();

        let mut values = row.values().collect::<Vec<_>>();
        values.sort_by_key(|cell| cell.schema_entry_id().column_id());

        assert_eq!(values.len(), 2);
        assert_eq!(values[0].schema_entry_id().owner_id(), 1);
        assert_eq!(values[0].schema_entry_id().column_id(), 0);
        assert!(matches!(values[0].value(), RowCellValue::Int(10)));
        assert_eq!(values[1].schema_entry_id().owner_id(), 1);
        assert_eq!(values[1].schema_entry_id().column_id(), 1);
        assert!(matches!(values[1].value(), RowCellValue::String(value) if value == "Ada"));
    }

    #[test]
    fn test_row_builder_creates_cells_from_schema_entries_positions_and_names() {
        let schema = Schema::builder(1)
            .column("id", DataType::Int, 0, true)
            .column("name", DataType::String, 1, false)
            .column("email", DataType::String, 2, false)
            .build();

        let id_entry = schema
            .columns()
            .values()
            .find(|entry| entry.name() == "id")
            .unwrap();

        let row = Row::builder(&schema)
            .cell(id_entry, 10)
            .value_named("name", "Ada")
            .value_at_position(2, RowCellValue::NULL)
            .build();

        let mut values = row.values().collect::<Vec<_>>();
        values.sort_by_key(|cell| cell.schema_entry_id().column_id());

        assert_eq!(values.len(), 3);
        assert!(matches!(values[0].value(), RowCellValue::Int(10)));
        assert!(matches!(values[1].value(), RowCellValue::String(value) if value == "Ada"));
        assert!(matches!(values[2].value(), RowCellValue::NULL));
    }
}
