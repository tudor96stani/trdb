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
    /// A primary-key ID does not identify a supplied column.
    #[error("unknown primary-key column ID: {column_id}")]
    UnknownPrimaryKeyColumn {
        /// ID absent from the supplied columns.
        column_id: ObjectId,
    },
    /// A primary-key ID occurs more than once in the ordering list.
    #[error("duplicate primary-key column ID: {column_id}")]
    DuplicatePrimaryKeyColumn {
        /// ID repeated in the ordering list.
        column_id: ObjectId,
    },
    /// A column marked as a primary key is absent from the ordering list.
    #[error("primary-key ordering list omits column ID: {column_id}")]
    MissingPrimaryKeyColumn {
        /// ID of the omitted primary-key column.
        column_id: ObjectId,
    },
    /// A listed column is not marked as a primary key.
    #[error("column ID {column_id} is not marked as a primary key")]
    NonPrimaryKeyColumn {
        /// ID of the listed column whose primary-key flag is false.
        column_id: ObjectId,
    },
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
    /// when any column is marked as a primary key. The list must contain exactly
    /// those columns, each once.
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

    /// Builds the immutable schema, validating columns and the primary-key list.
    /// The list must contain each flagged primary-key column exactly once and
    /// must be supplied explicitly when any column is marked as a primary key.
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
        let flagged_keys: HashSet<_> = self
            .columns
            .iter()
            .filter(|entry| entry.is_pk())
            .map(|entry| entry.id())
            .collect();
        let mut listed_keys = HashSet::new();
        for id in &primary_keys {
            let column_id = id.column_id();
            if !ids.contains(id) {
                return Err(SchemaError::UnknownPrimaryKeyColumn { column_id });
            }
            if !listed_keys.insert(*id) {
                return Err(SchemaError::DuplicatePrimaryKeyColumn { column_id });
            }
            if !flagged_keys.contains(id) {
                return Err(SchemaError::NonPrimaryKeyColumn { column_id });
            }
        }
        for entry in &self.columns {
            if entry.is_pk() && !listed_keys.contains(&entry.id()) {
                return Err(SchemaError::MissingPrimaryKeyColumn {
                    column_id: entry.id().column_id(),
                });
            }
        }
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
        let schema = Schema::builder(42)
            .column(0, "id", DataType::Int, 0, false)
            .pk([])
            .build()
            .unwrap();
        assert!(schema.primary_keys().is_empty());
        assert!(!schema.columns_sorted().next().unwrap().is_pk());
    }

    #[test]
    fn unknown_primary_key_ids_are_rejected() {
        let error = Schema::builder(42)
            .column(7, "id", DataType::Int, 0, true)
            .pk([7, 99])
            .build()
            .unwrap_err();
        assert_eq!(
            error,
            SchemaError::UnknownPrimaryKeyColumn { column_id: 99 }
        );
    }

    #[test]
    fn duplicate_primary_key_ids_are_rejected() {
        let error = Schema::builder(42)
            .column(7, "id", DataType::Int, 0, true)
            .pk([7, 7])
            .build()
            .unwrap_err();
        assert_eq!(
            error,
            SchemaError::DuplicatePrimaryKeyColumn { column_id: 7 }
        );
    }

    #[test]
    fn omitted_primary_key_ids_are_rejected_including_empty_lists() {
        for keys in [vec![], vec![7]] {
            let error = Schema::builder(42)
                .column(7, "tenant_id", DataType::Int, 0, true)
                .column(99, "id", DataType::Int, 1, true)
                .pk(keys.clone())
                .build()
                .unwrap_err();
            let column_id = if keys.is_empty() { 7 } else { 99 };
            assert_eq!(error, SchemaError::MissingPrimaryKeyColumn { column_id });
        }
    }

    #[test]
    fn unflagged_primary_key_ids_are_rejected() {
        for flagged in [false, true] {
            let error = Schema::builder(42)
                .column(7, "id", DataType::Int, 0, flagged)
                .column(99, "name", DataType::String, 1, false)
                .pk(if flagged { vec![7, 99] } else { vec![99] })
                .build()
                .unwrap_err();
            assert_eq!(error, SchemaError::NonPrimaryKeyColumn { column_id: 99 });
        }
    }

    #[test]
    fn sparse_column_ids_are_independent_of_positions_and_primary_key_order() {
        let schema = Schema::builder(42)
            .column(99, "Name", DataType::String, 1, true)
            .column(ObjectId::MAX, "value", DataType::Int, 2, false)
            .column(7, "id", DataType::Int, 0, true)
            .pk([99, 7])
            .build()
            .unwrap();
        assert_eq!(schema.owner_id(), 42);
        assert_eq!(
            schema.primary_keys(),
            &[SchemaEntryId::new(42, 99), SchemaEntryId::new(42, 7)]
        );
        let columns: Vec<_> = schema.columns_sorted().collect();
        for (entry, (id, name, position, is_pk)) in columns.iter().zip([
            (7, "id", 0, true),
            (99, "Name", 1, true),
            (ObjectId::MAX, "value", 2, false),
        ]) {
            assert_eq!(entry.id(), SchemaEntryId::new(42, id));
            assert_eq!(entry.name(), name);
            assert_eq!(entry.position(), position);
            assert_eq!(entry.is_pk(), is_pk);
        }
        assert!(matches!(columns[0].data_type(), DataType::Int));
        assert!(matches!(columns[1].data_type(), DataType::String));
        assert!(matches!(columns[2].data_type(), DataType::Int));
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

#[cfg(test)]
mod test_cases {
    use super::*;

    // TC-SCHEMA-BLD-001 — Reject an empty schema
    // Requirements: SCHEMA-001, SCHEMA-BLD-001
    #[test]
    fn tc_schema_bld_001_reject_an_empty_schema() {
        let error = Schema::builder(17).build().unwrap_err();
        assert_eq!(error, SchemaError::InvalidColumnCount { count: 0 });
    }

    // TC-SCHEMA-BLD-002 — Accept a one-column schema
    // Requirements: SCHEMA-001, SCHEMA-004, SCHEMA-005, SCHEMA-BLD-002
    #[test]
    fn tc_schema_bld_002_accept_a_one_column_schema() {
        let schema = Schema::builder(17)
            .column(42, "Value", DataType::Int, 0, false)
            .build()
            .unwrap();

        assert_eq!(schema.owner_id(), 17);
        assert_eq!(schema.columns().len(), 1);
        let entry = schema.columns().get(&SchemaEntryId::new(17, 42)).unwrap();
        assert_eq!(entry.id().owner_id(), 17);
        assert_eq!(entry.id().column_id(), 42);
        assert_eq!(entry.name(), "Value");
        assert_eq!(entry.data_type(), &DataType::Int);
        assert_eq!(entry.position(), 0);
        assert!(!entry.is_pk());
    }

    // TC-SCHEMA-BLD-003 — Accept the maximum column count
    // Requirements: SCHEMA-001, SCHEMA-002, SCHEMA-003, SCHEMA-004, SCHEMA-005, SCHEMA-BLD-002
    #[test]
    fn tc_schema_bld_003_accept_the_maximum_column_count() {
        let mut builder = Schema::builder(17);
        for p in (0..64).rev() {
            builder = builder.column(
                1000 + 2 * p as ObjectId,
                format!("column_{p}"),
                if p % 2 == 0 {
                    DataType::Int
                } else {
                    DataType::String
                },
                p,
                false,
            );
        }
        let schema = builder.build().unwrap();
        assert_eq!(schema.columns().len(), 64);

        assert_eq!(schema.owner_id(), 17);
        for p in 0..64 {
            let column_id = 1000 + 2 * p as ObjectId;
            let entry = schema
                .columns()
                .get(&SchemaEntryId::new(17, column_id))
                .unwrap();
            assert_eq!(entry.id().owner_id(), 17);
            assert_eq!(entry.id().column_id(), column_id);
            assert_eq!(entry.name(), format!("column_{p}"));
            assert_eq!(
                entry.data_type(),
                &if p % 2 == 0 {
                    DataType::Int
                } else {
                    DataType::String
                }
            );
            assert_eq!(entry.position(), p);
            assert!(!entry.is_pk());
        }
    }

    // TC-SCHEMA-BLD-004 — Reject a column count above the maximum
    // Requirements: SCHEMA-001, SCHEMA-BLD-001
    #[test]
    fn tc_schema_bld_004_reject_a_column_count_above_the_maximum() {
        let mut builder = Schema::builder(17);
        for p in 0..=64 {
            builder = builder.column(
                1000 + p as ObjectId,
                format!("column_{p}"),
                DataType::Int,
                p,
                false,
            );
        }
        assert_eq!(
            builder.build().unwrap_err(),
            SchemaError::InvalidColumnCount { count: 65 }
        );
    }

    // TC-SCHEMA-BLD-005 — Reject exactly duplicated column names
    // Requirements: SCHEMA-002, SCHEMA-BLD-001
    #[test]
    fn tc_schema_bld_005_reject_exactly_duplicated_column_names() {
        let result = Schema::builder(17)
            .column(42, "value", DataType::Int, 0, false)
            .column(7, "value", DataType::String, 1, false)
            .build();
        assert!(matches!(
            result,
            Err(SchemaError::DuplicateColumnName { .. })
        ));
    }

    // TC-SCHEMA-BLD-006 — Reject ASCII case variants of a column name
    // Requirements: SCHEMA-002, SCHEMA-BLD-001
    #[test]
    fn tc_schema_bld_006_reject_ascii_case_variants_of_a_column_name() {
        let result = Schema::builder(17)
            .column(42, "AccountID", DataType::Int, 0, false)
            .column(7, "aCCOUNTid", DataType::String, 1, false)
            .build();
        assert!(matches!(
            result,
            Err(SchemaError::DuplicateColumnName { .. })
        ));
    }

    // TC-SCHEMA-BLD-010 — Reject duplicate column IDs with distinct metadata
    // Requirements: SCHEMA-003, SCHEMA-BLD-001
    #[test]
    fn tc_schema_bld_010_reject_duplicate_column_ids_with_distinct_metadata() {
        let error = Schema::builder(17)
            .column(42, "first", DataType::Int, 0, false)
            .column(42, "second", DataType::String, 1, false)
            .build()
            .unwrap_err();
        assert_eq!(error, SchemaError::DuplicateColumnId { column_id: 42 });
    }

    // TC-SCHEMA-BLD-011 — Reject duplicate positions within the valid range
    // Requirements: SCHEMA-004, SCHEMA-BLD-001
    #[test]
    fn tc_schema_bld_011_reject_duplicate_positions_within_the_valid_range() {
        let error = Schema::builder(17)
            .column(42, "first", DataType::Int, 0, false)
            .column(7, "second", DataType::String, 1, false)
            .column(100, "third", DataType::Int, 1, false)
            .build()
            .unwrap_err();
        assert_eq!(error, SchemaError::DuplicateColumnPosition { position: 1 });
    }

    // TC-SCHEMA-BLD-012 — Reject a position equal to the column count
    // Requirements: SCHEMA-004, SCHEMA-BLD-001
    #[test]
    fn tc_schema_bld_012_reject_a_position_equal_to_the_column_count() {
        let error = Schema::builder(17)
            .column(42, "first", DataType::Int, 0, false)
            .column(7, "second", DataType::String, 1, false)
            .column(100, "third", DataType::Int, 3, false)
            .build()
            .unwrap_err();
        assert_eq!(
            error,
            SchemaError::InvalidColumnPosition {
                position: 3,
                count: 3
            }
        );
    }

    // TC-SCHEMA-BLD-014 — Preserve independent IDs and explicit composite primary-key order
    // Requirements: SCHEMA-001, SCHEMA-002, SCHEMA-003, SCHEMA-004, SCHEMA-005, SCHEMA-BLD-002, SCHEMA-BLD-003
    #[test]
    fn tc_schema_bld_014_preserve_independent_ids_and_explicit_composite_primary_key_order() {
        let schema = Schema::builder(17)
            .column(42, "Tenant", DataType::Int, 2, true)
            .column(7, "Label", DataType::String, 0, false)
            .column(100, "Record", DataType::String, 1, true)
            .pk(vec![100, 42])
            .build()
            .unwrap();

        assert_eq!(schema.owner_id(), 17);
        assert_eq!(schema.columns().len(), 3);
        for (column_id, name, data_type, position, is_pk) in [
            (42, "Tenant", DataType::Int, 2, true),
            (7, "Label", DataType::String, 0, false),
            (100, "Record", DataType::String, 1, true),
        ] {
            let entry = schema
                .columns()
                .get(&SchemaEntryId::new(17, column_id))
                .unwrap();
            assert_eq!(entry.id().owner_id(), 17);
            assert_eq!(entry.id().column_id(), column_id);
            assert_eq!(entry.name(), name);
            assert_eq!(entry.data_type(), &data_type);
            assert_eq!(entry.position(), position);
            assert_eq!(entry.is_pk(), is_pk);
        }

        assert_eq!(
            schema
                .primary_keys()
                .iter()
                .map(|id| (id.owner_id(), id.column_id()))
                .collect::<Vec<_>>(),
            vec![(17, 100), (17, 42)]
        );
    }

    // TC-SCHEMA-BLD-016 — Require an ordering list when any column is a primary key
    // Requirements: SCHEMA-BLD-004
    #[test]
    fn tc_schema_bld_016_require_an_ordering_list_when_any_column_is_a_primary_key() {
        let error = Schema::builder(17)
            .column(42, "label", DataType::String, 0, false)
            .column(7, "key", DataType::Int, 1, true)
            .build()
            .unwrap_err();
        assert_eq!(error, SchemaError::MissingPrimaryKeyOrder);
    }

    // TC-SCHEMA-BLD-019 — Reject an unknown primary-key column ID
    // Requirements: SCHEMA-BLD-005
    #[test]
    fn tc_schema_bld_019_reject_an_unknown_primary_key_column_id() {
        let result = Schema::builder(17)
            .column(42, "key", DataType::Int, 0, true)
            .column(7, "label", DataType::String, 1, false)
            .pk(vec![42, 999])
            .build();
        assert!(result.is_err());
    }

    // TC-SCHEMA-BLD-020 — Reject a repeated primary-key column ID
    // Requirements: SCHEMA-BLD-006
    #[test]
    fn tc_schema_bld_020_reject_a_repeated_primary_key_column_id() {
        let result = Schema::builder(17)
            .column(42, "key", DataType::Int, 0, true)
            .column(7, "label", DataType::String, 1, false)
            .pk(vec![42, 42])
            .build();
        assert!(result.is_err());
    }

    // TC-SCHEMA-BLD-021 — Reject an ordering list that omits a marked primary-key column
    // Requirements: SCHEMA-BLD-007
    #[test]
    fn tc_schema_bld_021_reject_an_ordering_list_that_omits_a_marked_primary_key_column() {
        let result = Schema::builder(17)
            .column(42, "tenant", DataType::Int, 0, true)
            .column(7, "record", DataType::String, 1, true)
            .pk(vec![42])
            .build();
        assert!(result.is_err());
    }

    // TC-SCHEMA-BLD-022 — Reject an ordering list that includes an unmarked column
    // Requirements: SCHEMA-BLD-008
    #[test]
    fn tc_schema_bld_022_reject_an_ordering_list_that_includes_an_unmarked_column() {
        let result = Schema::builder(17)
            .column(42, "key", DataType::Int, 0, true)
            .column(7, "label", DataType::String, 1, false)
            .pk(vec![42, 7])
            .build();
        assert!(result.is_err());
    }

    // TC-SCHEMA-ORD-001 — Sort columns by position independently of insertion and ID order
    #[test]
    fn tc_schema_ord_001_sort_columns_by_position_independently_of_insertion_and_id_order() {
        let schema = Schema::builder(17)
            .column(42, "third", DataType::Int, 2, false)
            .column(7, "first", DataType::String, 0, false)
            .column(100, "fourth", DataType::String, 3, false)
            .column(23, "second", DataType::Int, 1, false)
            .build()
            .unwrap();
        let columns: Vec<_> = schema.columns_sorted().collect();
        assert_eq!(
            columns
                .iter()
                .map(|entry| (entry.id().column_id(), entry.position()))
                .collect::<Vec<_>>(),
            vec![(7, 0), (23, 1), (42, 2), (100, 3)]
        );
    }
}
