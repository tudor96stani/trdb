use crate::schemas::ObjectId;
use crate::schemas::data_type::DataType;
use std::collections::HashMap;
use std::convert::TryInto;

/// An in memory representation of the schema of a table or index
#[derive(Debug)]
pub struct Schema {
    /// The ID of the owner (table/index) of this schema
    owner_id: ObjectId,
    /// The list of columns in the schema, mapped via their `ColumnId`
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
    /// The position of the
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

/// A builder of a `Schema`
#[derive(Debug)]
pub struct SchemaBuilder {
    owner_id: ObjectId,
    columns: HashMap<SchemaEntryId, SchemaEntry>,
    primary_keys: Vec<SchemaEntryId>,
}

impl SchemaBuilder {
    /// Creates a new `SchemaBuilder` - not to be used externally. Invoke the `builder()` method on `Schema`
    fn new(owner_id: ObjectId) -> Self {
        Self {
            owner_id,
            columns: HashMap::new(),
            primary_keys: Vec::new(),
        }
    }

    /// Append a new column to the schema builder
    pub fn column(
        mut self,
        name: impl Into<String>,
        data_type: DataType,
        position: usize,
        is_pk: bool,
    ) -> Self {
        let column_id = SchemaEntryId::new(
            self.owner_id,
            position
                .try_into()
                .expect("column position does not fit in ObjectId"),
        );

        let entry = SchemaEntry::new(column_id, name.into(), data_type, position, is_pk);

        if is_pk {
            self.primary_keys.push(column_id);
        }

        self.columns.insert(column_id, entry);
        self
    }

    /// Builds the final immutable `Schema`.
    pub fn build(mut self) -> Schema {
        self.primary_keys.sort_by_key(|id| id.column_id());
        Schema::new(self.owner_id, self.columns, self.primary_keys)
    }
}

#[cfg(test)]
mod schema_builder_tests {
    use super::*;

    #[test]
    fn test_schema_builder_creates_schema_entries_with_generated_ids() {
        let owner_id = 42;
        let schema = Schema::builder(owner_id)
            .column("id", DataType::Int, 0, true)
            .column("name", DataType::String, 1, false)
            .build();

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
    fn test_schema_builder_sorts_primary_keys_by_position() {
        let owner_id = 42;
        let schema = Schema::builder(owner_id)
            .column("name", DataType::String, 2, true)
            .column("tenant_id", DataType::Int, 0, true)
            .column("id", DataType::Int, 1, true)
            .build();

        let primary_keys = schema.primary_keys();
        assert_eq!(primary_keys.len(), 3);
        assert_eq!(primary_keys[0], SchemaEntryId::new(owner_id, 0));
        assert_eq!(primary_keys[1], SchemaEntryId::new(owner_id, 1));
        assert_eq!(primary_keys[2], SchemaEntryId::new(owner_id, 2));
    }
}
