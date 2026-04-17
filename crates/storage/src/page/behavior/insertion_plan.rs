//! Defines the insertion plan struct for inserting records into unsorted heap pages, along with related enums.

/// Defines the offset at which a new record should be inserted in an unsorted heap page.
#[derive(Debug)]
pub enum InsertionOffset {
    /// Record should be inserted at the start of free space after compacting the page.
    AfterCompactionFreeStart,
    /// Record should be inserted at an exact offset.
    Exact(usize),
}

/// Defines whether a new slot should be created for the record or an existing slot can be reused when inserting into an unsorted heap page.
#[derive(Debug)]
pub enum InsertionSlot {
    /// A new slot should be created for the record.
    New,
    /// An existing slot can be reused for the record.
    Reuse(usize),
}

/// Represents a plan for inserting a new record into an unsorted heap page.
#[derive(Debug)]
pub struct InsertionPlan {
    /// The slot information for the insertion.
    pub slot: InsertionSlot,
    /// The offset information for the insertion.
    pub offset: InsertionOffset,
}
