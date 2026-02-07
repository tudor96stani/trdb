use crate::PAGE_SIZE;
use crate::errors::page_error::{PageResult, WithPageId};
use crate::errors::page_op_error::PageOpError;
use crate::insertion_plan::InsertionPlan;
use crate::page_id::PageId;
use crate::page_type::PageType;

/// Wrapper around a fixed-size byte array representing a page.
#[derive(Debug)]
pub struct Page {
    /// Unique identifier of the page. Comprised of file_name_hash::page_number_within_file
    pub(crate) page_id: PageId,
    /// Main binary array holding the `PAGE_SIZE` bytes of data for the page. Boxed and owned by this struct.
    pub(crate) data: Box<[u8; PAGE_SIZE]>,
}

/// Public APIs for the Page struct.
/// All public APIs use the `PageResult` type
impl Page {
    /// Creates a new page with all bytes initialized to zero. Private constructor.
    pub fn new_zeroed(page_id: PageId) -> Self {
        Self {
            page_id,
            data: Box::new([0; PAGE_SIZE]),
        }
    }

    /// Initializes a page for the given `PageId` and `PageType`
    /// Beware, this method will wipe out the contents of the internal byte array, zero-ing them out.
    pub fn initialize(&mut self, page_id: PageId, page_type: PageType) -> PageResult<()> {
        // Completely wipe the page by zero-ing it out.
        (&mut *self.data)[..].fill(0);

        let mut header = self
            .header_mut()
            .map_err(PageOpError::from)
            .with_page_id(page_id)?;

        // And reset the header for a fresh page.
        header
            .default(page_id.page_number, page_type)
            .map_err(PageOpError::from)
            .with_page_id(page_id)?;

        Ok(())
    }

    /// Retrieves a row from the page by its slot index.
    ///
    /// # Arguments
    ///
    /// * `slot_index` - The index of the slot to retrieve the row from. Indexing starts from 0.
    ///
    /// # Returns
    ///
    /// * `PageResult<&[u8]>` - A result containing a reference to the row data as a byte slice
    ///   if successful, or an error wrapped in `PageResult` if the operation fails.
    ///
    /// # Errors
    ///
    /// This method can return the following errors:
    /// * `PageError` - If there is an issue with the operation, such as an invalid slot index.
    ///
    /// The error is augmented with the `page_id` of the current page for better traceability.
    pub fn row(&self, slot_index: u32) -> PageResult<&[u8]> {
        self.read_row_internal(slot_index)
            .map_err(PageOpError::from)
            .with_page_id(self.page_id)
    }

    /// Plans the insertion of a row into the page. Used only for heap pages.
    ///
    /// # Arguments
    ///
    /// * `row_len` - The length of the row to be inserted, in bytes.
    ///
    /// # Returns
    ///
    /// * `PageResult<InsertionPlan>` - A result containing the insertion plan if successful,
    ///   or an error wrapped in `PageResult` if the operation fails.
    ///
    /// # Errors
    ///
    /// This method can return the following errors:
    /// * `PageError` - If there is an issue with the operation, such as insufficient space
    ///   or other constraints preventing the insertion.
    ///
    /// The error is augmented with the `page_id` of the current page for better traceability.
    pub fn plan_insert(&self, row_len: usize) -> PageResult<InsertionPlan> {
        self.plan_insert_internal(row_len)
            .map_err(PageOpError::from)
            .with_page_id(self.page_id)
    }

    /// Inserts a row into a heap page using the provided insertion plan.
    ///
    /// # Arguments
    ///
    /// * `plan` - The `InsertionPlan` that specifies where and how the row should be inserted.
    /// * `row` - A `Vec<u8>` containing the row data to be inserted.
    ///
    /// # Returns
    ///
    /// * `PageResult<()>` - A result indicating success (`Ok(())`) or failure (`Err(PageError)`).
    ///
    /// # Errors
    ///
    /// This method can return the following errors:
    /// * `PageOpError` - If there is an issue during the insertion process. `PageOpError` will contain the source error.
    /// * The error is augmented with the `page_id` of the current page for better traceability.
    pub fn insert_heap(&mut self, plan: InsertionPlan, row: Vec<u8>) -> PageResult<()> {
        self.insert_row_unsorted_internal(plan, row)
            .map_err(PageOpError::from)
            .with_page_id(self.page_id)
    }

    /// Deletes a row from the page at the specified slot index.
    ///
    /// # Arguments
    ///
    /// * `slot_index` - The index of the slot from which the row should be deleted.
    ///   Indexing starts from 0.
    /// * `compact_requested` - A boolean flag indicating whether compaction should
    ///   be performed after the deletion. If `true`, the page may attempt to compact
    ///   the remaining rows to reclaim space.
    ///
    /// # Returns
    ///
    /// * `PageResult<()>` - A result indicating success (`Ok(())`) or failure
    ///   (`Err(PageOpError)`). The error is augmented with the `page_id` of the
    ///   current page for better traceability.
    ///
    /// # Errors
    ///
    /// This method can return the following errors:
    /// * `PageOpError` - If there is an issue during the deletion process, such as
    ///   an invalid slot index or other constraints preventing the deletion.
    pub fn delete_row(&mut self, slot_index: usize, compact_requested: bool) -> PageResult<()> {
        self.delete_row_internal(slot_index, compact_requested)
            .map_err(PageOpError::from)
            .with_page_id(self.page_id)
    }

    /// Updates the contents of a row.
    /// New content can be of same size, smaller or larger.
    /// For larger content, the new row size must still fit within the current page. It there is not enough room, an error will be returned to indicate this.
    /// For such scenarios, a deletion & reinsertion is necessary.
    ///
    /// # Arguments
    ///
    /// * `slot_index`: the slot number of the row being updated
    /// * `row`: the new content of the row.
    ///
    /// # Returns
    ///
    /// * `PageResult<()>` - A result indicating success (`Ok(())`) or failure
    ///   (`Err(PageOpError)`). The error is augmented with the `page_id` of the
    ///   current page for better traceability.
    ///
    /// # Errors
    ///
    /// This method can return the following errors:
    /// * `PageOpError` - If there is an issue during the update process, such as
    ///   an invalid slot index or other constraints preventing the update.
    pub fn update_row(&mut self, slot_index: usize, row: Vec<u8>) -> PageResult<()> {
        self.update_internal(slot_index, row)
            .map_err(PageOpError::from)
            .with_page_id(self.page_id)
    }

    /// Returns an immutable reference to the underlying data of the page.
    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    /// Returns a mutable reference to the underlying byte array of the page
    pub fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.data
    }
}
