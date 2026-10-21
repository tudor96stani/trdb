//! Identifiers for the objects managed by the database engine.

/// Unique identifier for an object within the database engine (table/index/etc)
pub type ObjectId = u16;

/// Identifier for a database. Unique within the entire server instance.
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub struct DatabaseId(ObjectId);

impl DatabaseId {
    /// Wraps a raw [`ObjectId`].
    pub fn new(id: ObjectId) -> Self {
        Self(id)
    }

    /// Returns the wrapped raw [`ObjectId`].
    pub fn value(&self) -> ObjectId {
        self.0
    }
}

impl std::fmt::Display for DatabaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Identifier for a table. Unique within the scope of its parent database
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub struct TableId(ObjectId);

impl TableId {
    /// Wraps a raw [`ObjectId`].
    pub fn new(id: ObjectId) -> Self {
        Self(id)
    }

    /// Returns the wrapped raw [`ObjectId`].
    pub fn value(&self) -> ObjectId {
        self.0
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Identifier for an index. Unique within the scope of its parent database
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub struct IndexId(ObjectId);

impl IndexId {
    /// Wraps a raw [`ObjectId`].
    pub fn new(id: ObjectId) -> Self {
        Self(id)
    }

    /// Returns the wrapped raw [`ObjectId`].
    pub fn value(&self) -> ObjectId {
        self.0
    }
}

impl std::fmt::Display for IndexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Identifier for a column. Unique within the scope of its parent table
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub struct ColumnId(ObjectId);

impl ColumnId {
    /// Wraps a raw [`ObjectId`].
    pub fn new(id: ObjectId) -> Self {
        Self(id)
    }

    /// Returns the wrapped raw [`ObjectId`].
    pub fn value(&self) -> ObjectId {
        self.0
    }
}

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Identifier for a constraint. Unique within the scope of its parent table
#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub struct ConstraintId(ObjectId);

impl ConstraintId {
    /// Wraps a raw [`ObjectId`].
    pub fn new(id: ObjectId) -> Self {
        Self(id)
    }

    /// Returns the wrapped raw [`ObjectId`].
    pub fn value(&self) -> ObjectId {
        self.0
    }
}

impl std::fmt::Display for ConstraintId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
