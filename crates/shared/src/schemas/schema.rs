use crate::schemas::ObjectId;
use crate::schemas::data_type::DataType;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

/// An in memory representation of the schema of a table or index
#[derive(Debug)]
pub struct Schema {
    /// The ID of the owner (table/index) of this schema
    owner_id: ObjectId,
    /// The list of columns in the schema, mapped via their `SchemaEntryId`
    columns: HashMap<SchemaEntryId, SchemaEntry>,
    /// IDs of the columns that make up the primary key
    primary_keys: Vec<SchemaEntryId>,
}

impl Schema {
    /// Low-level constructor. Prefer `Schema::builder()` for ergonomic creation.
    fn new(
        owner_id: ObjectId,
        columns: HashMap<SchemaEntryId, SchemaEntry>,
        primary_keys: Vec<SchemaEntryId>,
    ) -> Self {
        Self {
            owner_id,
            columns,
            primary_keys,
        }
    }

    /// Creates a `SchemaBuilder` to be used to generate a new `Schema` object
    pub fn builder(owner_id: ObjectId) -> SchemaBuilder {
        SchemaBuilder::new(owner_id)
    }

    /// Returns the ID of the owner of this schema.
    pub fn owner_id(&self) -> ObjectId {
        self.owner_id
    }

    /// Returns the columns of this schema.
    pub fn columns(&self) -> &HashMap<SchemaEntryId, SchemaEntry> {
        &self.columns
    }

    /// Returns the primary-key columns in order.
    pub fn primary_keys(&self) -> &[SchemaEntryId] {
        &self.primary_keys
    }

    /// Returns the sorted columns
    pub fn columns_sorted(&self) -> impl Iterator<Item = &SchemaEntry> {
        let mut values: Vec<_> = self.columns.values().collect();
        values.sort_by_key(|b| b.position());
        values.into_iter()
    }
}

/// A single entry in the `Schema` container
#[derive(Debug)]
pub struct SchemaEntry {
    /// Unique identifier of the column
    id: SchemaEntryId,
    /// Name of the column
    name: String,
    /// Data type of the column
    data_type: DataType,
    /// The zero-based position of the column within the schema
    position: usize,
    /// Flag indicating whether column is part of the primary key
    is_pk: bool,
}

impl SchemaEntry {
    /// Constructor
    fn new(
        id: SchemaEntryId,
        name: String,
        data_type: DataType,
        position: usize,
        is_pk: bool,
    ) -> Self {
        Self {
            id,
            name,
            data_type,
            position,
            is_pk,
        }
    }

    /// Returns the unique identifier of this schema entry.
    pub fn id(&self) -> SchemaEntryId {
        self.id
    }

    /// Returns the name of this schema entry.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the data type of this schema entry.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the position of this schema entry.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Returns whether this schema entry is part of the primary key.
    pub fn is_pk(&self) -> bool {
        self.is_pk
    }

    /// Returns a boolean indicating whether the schema entry (column) is fixed sized or not.
    pub fn is_fixed_size(&self) -> bool {
        match self.data_type {
            DataType::Int => true,
            DataType::String => false,
        }
    }
}

/// Represents the unique identifier of a column within a `SchemaEntry`.
#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq)]
pub struct SchemaEntryId {
    /// The ID of the owner of this column (table or index id)
    owner_id: ObjectId,
    /// The unique ID of the column within the owner's namespace
    column_id: ObjectId,
}

impl SchemaEntryId {
    /// Constructor
    fn new(owner_id: ObjectId, column_id: ObjectId) -> Self {
        Self {
            owner_id,
            column_id,
        }
    }

    /// Returns the owner ID of this schema entry ID.
    pub fn owner_id(&self) -> ObjectId {
        self.owner_id
    }

    /// Returns the column ID within the owner namespace.
    pub fn column_id(&self) -> ObjectId {
        self.column_id
    }
}

/// Validation failure when building a schema.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SchemaError {
    /// A column is marked as a primary key but no ordering list was supplied.
    #[error("primary-key columns require an explicit ordering list through .pk(...)")]
    MissingPrimaryKeyOrder,
    /// A schema must contain between 1 and 64 columns.
    #[error("schema must contain between 1 and 64 columns, got {count}")]
    InvalidColumnCount {
        /// Number of supplied columns.
        count: usize,
    },
    /// Column names must be unique under ASCII case-insensitive comparison.
    #[error("duplicate column name: {name}")]
    DuplicateColumnName {
        /// Conflicting name as supplied by the caller.
        name: String,
    },
    /// Column IDs must be unique within the schema.
    #[error("duplicate column ID: {column_id}")]
    DuplicateColumnId {
        /// Conflicting ID within the owner namespace.
        column_id: ObjectId,
    },
    /// Column positions must be unique.
    #[error("duplicate column position: {position}")]
    DuplicateColumnPosition {
        /// Conflicting position.
        position: usize,
    },
    /// Each position must be in the range zero through the column count minus one.
    #[error("column position {position} is out of range for {count} columns")]
    InvalidColumnPosition {
        /// Position outside the valid range.
        position: usize,
        /// Number of supplied columns.
        count: usize,
    },
}

/// A builder of a `Schema`
#[derive(Debug)]
pub struct SchemaBuilder {
    owner_id: ObjectId,
    columns: Vec<SchemaEntry>,
    primary_keys: Option<Vec<SchemaEntryId>>,
}

impl SchemaBuilder {
    /// Creates a new `SchemaBuilder` - not to be used externally. Invoke the `builder()` method on `Schema`
    fn new(owner_id: ObjectId) -> Self {
        Self {
            owner_id,
            columns: Vec::new(),
            primary_keys: None,
        }
    }

    /// Appends a column with an explicit ID independent of its position.
    ///
    /// Validation is performed by [`Self::build`].
    pub fn column(
        mut self,
        column_id: ObjectId,
        name: impl Into<String>,
        data_type: DataType,
        position: usize,
        is_pk: bool,
    ) -> Self {
        let id = SchemaEntryId::new(self.owner_id, column_id);
        self.columns.push(SchemaEntry::new(
            id,
            name.into(),
            data_type,
            position,
            is_pk,
        ));
        self
    }

    /// Sets the ordered primary-key column IDs, preserving the supplied order.
    ///
    /// A subsequent call replaces the previous list. An explicit list is required
    /// when any column is marked as a primary key.
    pub fn pk(mut self, column_ids: impl Into<Vec<ObjectId>>) -> Self {
        self.primary_keys = Some(
            column_ids
                .into()
                .into_iter()
                .map(|column_id| SchemaEntryId::new(self.owner_id, column_id))
                .collect(),
        );
        self
    }

    /// Builds the immutable schema, validating columns and requiring an explicit
    /// primary-key ordering list when any column is marked as a primary key.
    pub fn build(self) -> Result<Schema, SchemaError> {
        let count = self.columns.len();
        if !(1..=64).contains(&count) {
            return Err(SchemaError::InvalidColumnCount { count });
        }

        let mut names = HashSet::new();
        let mut ids = HashSet::new();
        let mut positions = HashSet::new();
        for entry in &self.columns {
            if !names.insert(entry.name().to_ascii_lowercase()) {
                return Err(SchemaError::DuplicateColumnName {
                    name: entry.name().to_owned(),
                });
            }
            if !ids.insert(entry.id()) {
                return Err(SchemaError::DuplicateColumnId {
                    column_id: entry.id().column_id(),
                });
            }
            if entry.position() >= count {
                return Err(SchemaError::InvalidColumnPosition {
                    position: entry.position(),
                    count,
                });
            }
            if !positions.insert(entry.position()) {
                return Err(SchemaError::DuplicateColumnPosition {
                    position: entry.position(),
                });
            }
        }

        if self.primary_keys.is_none() && self.columns.iter().any(|entry| entry.is_pk()) {
            return Err(SchemaError::MissingPrimaryKeyOrder);
        }
        let primary_keys = self.primary_keys.unwrap_or_default();
        let columns = self
            .columns
            .into_iter()
            .map(|entry| (entry.id(), entry))
            .collect();
        Ok(Schema::new(self.owner_id, columns, primary_keys))
    }
}

#[cfg(test)]
mod schema_builder_tests {
    use super::*;

    #[test]
    fn test_schema_builder_creates_schema_entries_with_explicit_ids() {
        let owner_id = 42;
        let schema = Schema::builder(owner_id)
            .column(0, "id", DataType::Int, 0, true)
            .column(1, "name", DataType::String, 1, false)
            .pk([0])
            .build()
            .unwrap();

        assert_eq!(schema.owner_id(), owner_id);
        assert_eq!(schema.columns().len(), 2);

        let id_entry_id = SchemaEntryId::new(owner_id, 0);
        let id_entry = schema.columns().get(&id_entry_id).unwrap();
        assert_eq!(id_entry.id(), id_entry_id);
        assert_eq!(id_entry.name(), "id");
        assert!(matches!(id_entry.data_type(), DataType::Int));
        assert_eq!(id_entry.position(), 0);
        assert!(id_entry.is_pk());
        assert_eq!(id_entry.id().owner_id(), owner_id);

        let name_entry_id = SchemaEntryId::new(owner_id, 1);
        let name_entry = schema.columns().get(&name_entry_id).unwrap();
        assert_eq!(name_entry.id(), name_entry_id);
        assert_eq!(name_entry.name(), "name");
        assert!(matches!(name_entry.data_type(), DataType::String));
        assert_eq!(name_entry.position(), 1);
        assert!(!name_entry.is_pk());
        assert_eq!(name_entry.id().owner_id(), owner_id);
    }

    #[test]
    fn test_schema_builder_preserves_supplied_primary_key_order() {
        let owner_id = 42;
        let schema = Schema::builder(owner_id)
            .column(0, "name", DataType::String, 2, true)
            .column(1, "tenant_id", DataType::Int, 0, true)
            .column(2, "id", DataType::Int, 1, true)
            .pk([2, 0, 1])
            .build()
            .unwrap();

        let primary_keys = schema.primary_keys();
        assert_eq!(primary_keys.len(), 3);
        assert_eq!(primary_keys[0], SchemaEntryId::new(owner_id, 2));
        assert_eq!(primary_keys[1], SchemaEntryId::new(owner_id, 0));
        assert_eq!(primary_keys[2], SchemaEntryId::new(owner_id, 1));
        let ordered_columns: Vec<_> = schema.columns_sorted().collect();
        assert_eq!(
            ordered_columns
                .iter()
                .map(|entry| entry.position())
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            ordered_columns
                .iter()
                .map(|entry| entry.id().column_id())
                .collect::<Vec<_>>(),
            vec![1, 2, 0]
        );
    }

    #[test]
    fn column_count_boundaries() {
        for count in [0, 1, 64, 65] {
            let mut builder = Schema::builder(1);
            for position in 0..count {
                builder = builder.column(
                    position as ObjectId,
                    format!("col_{position}"),
                    DataType::Int,
                    position,
                    false,
                );
            }
            let result = builder.build();
            if (1..=64).contains(&count) {
                assert_eq!(result.unwrap().columns().len(), count);
            } else {
                assert_eq!(
                    result.unwrap_err(),
                    SchemaError::InvalidColumnCount { count }
                );
            }
        }
    }

    #[test]
    fn column_names_use_ascii_case_insensitive_comparison() {
        for (first, second, duplicate) in [
            ("Name", "nAME", true),
            ("same", "same", true),
            ("ÄA", "Äa", true),
            ("Ä", "ä", false),
            ("name", "names", false),
            ("a-b", "a_b", false),
        ] {
            let result = Schema::builder(1)
                .column(0, first, DataType::Int, 0, false)
                .column(1, second, DataType::String, 1, false)
                .build();
            if duplicate {
                assert_eq!(
                    result.unwrap_err(),
                    SchemaError::DuplicateColumnName {
                        name: second.into()
                    }
                );
            } else {
                let schema = result.unwrap();
                assert_eq!(
                    schema
                        .columns_sorted()
                        .map(|entry| entry.name())
                        .collect::<Vec<_>>(),
                    vec![first, second]
                );
            }
        }
    }

    #[test]
    fn duplicate_ids_are_rejected_without_overwriting_columns() {
        let error = Schema::builder(1)
            .column(0, "first", DataType::Int, 0, true)
            .column(0, "second", DataType::String, 1, false)
            .build()
            .unwrap_err();
        assert_eq!(error, SchemaError::DuplicateColumnId { column_id: 0 });
    }

    #[test]
    fn duplicate_positions_are_rejected_even_with_distinct_ids() {
        let error = Schema::builder(1)
            .column(0, "first", DataType::Int, 0, false)
            .column(1, "second", DataType::String, 0, false)
            .build()
            .unwrap_err();
        assert_eq!(error, SchemaError::DuplicateColumnPosition { position: 0 });
    }

    #[test]
    fn gaps_and_out_of_range_positions_are_rejected_without_panicking() {
        for position in [2, 64, usize::MAX] {
            let error = Schema::builder(1)
                .column(0, "first", DataType::Int, 0, false)
                .column(1, "second", DataType::String, position, false)
                .build()
                .unwrap_err();
            assert_eq!(
                error,
                SchemaError::InvalidColumnPosition { position, count: 2 }
            );
        }
        let error = Schema::builder(1)
            .column(0, "first", DataType::Int, 1, false)
            .build()
            .unwrap_err();
        assert_eq!(
            error,
            SchemaError::InvalidColumnPosition {
                position: 1,
                count: 1
            }
        );
    }

    #[test]
    fn primary_key_flag_requires_an_explicit_ordering_list() {
        let error = Schema::builder(42)
            .column(0, "id", DataType::Int, 0, true)
            .build()
            .unwrap_err();
        assert_eq!(error, SchemaError::MissingPrimaryKeyOrder);
    }

    #[test]
    fn schema_without_primary_key_flags_needs_no_ordering_list() {
        let schema = Schema::builder(42)
            .column(0, "name", DataType::String, 0, false)
            .build()
            .unwrap();
        assert!(schema.primary_keys().is_empty());
        assert!(!schema.columns_sorted().next().unwrap().is_pk());
    }

    #[test]
    fn explicitly_empty_primary_key_list_is_preserved() {
        for is_pk in [false, true] {
            let schema = Schema::builder(42)
                .column(0, "id", DataType::Int, 0, is_pk)
                .pk([])
                .build()
                .unwrap();
            assert!(schema.primary_keys().is_empty());
            assert_eq!(schema.columns_sorted().next().unwrap().is_pk(), is_pk);
        }
    }

    #[test]
    fn primary_key_order_can_be_supplied_before_columns_and_replaced() {
        let schema = Schema::builder(42)
            .pk(vec![0, 1])
            .pk([1, 0])
            .column(0, "first", DataType::Int, 0, true)
            .column(1, "second", DataType::String, 1, true)
            .build()
            .unwrap();
        assert_eq!(
            schema.primary_keys(),
            &[SchemaEntryId::new(42, 1), SchemaEntryId::new(42, 0)]
        );
        assert!(schema.columns_sorted().all(|entry| entry.is_pk()));
    }
}
