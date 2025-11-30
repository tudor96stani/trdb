/// Defines flags and metadata needed for the insertion of a row in a data page.
#[derive(Debug, Copy, Clone)]
pub struct InsertionPlan {
    pub slot_number: u16,
    pub start_offset: usize,
    pub inserting_at_free_start: bool,
    pub inserting_new_slot: bool,
    pub needs_compaction: bool,
}
