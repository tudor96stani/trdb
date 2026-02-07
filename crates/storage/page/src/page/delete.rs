use crate::HEADER_SIZE;
use crate::errors::delete_error::DeleteError;
use crate::errors::slot_error::SlotError;
use crate::page::api::Page;

impl Page {
    pub(super) fn delete_row_internal(
        &mut self,
        slot_index: usize,
        compact_requested: bool,
    ) -> Result<(), DeleteError> {
        // First check if the slot is valid before doing anything. We should not allow attempts to delete an invalid slot number (or at least not proceed with the process in case something gets fucked up)
        {
            let slot_array = self.slot_array_ref()?;
            let slot_to_be_deleted = slot_array.slot_ref(slot_index as u32)?;
            if !self.is_slot_valid(&slot_to_be_deleted)? {
                return Err(SlotError::InvalidSlot { slot_index }.into());
            }
        }

        // First let's see if deleting this row will allow us to shift the free start
        let can_reset_free_start = self.try_to_find_new_free_start(slot_index)?;

        let mut slot_array = self.slot_array_mut()?;
        let mut slot = slot_array.slot_mut(slot_index as u32)?;
        let row_size = slot.length()?;

        slot.set_length(0)?;
        slot.set_offset(0)?;

        let mut header = self.header_mut()?;

        let current_free_space = header.get_free_space()?;
        let new_free_space = current_free_space + row_size;

        header.set_free_space(new_free_space)?;

        // If we received a value here, it is the new free start that we will use.
        if let Some(new_free_start) = can_reset_free_start {
            header.set_free_start(new_free_start as u16)?;
        } else {
            header.set_can_compact(1)?;
        }

        if (compact_requested) {
            self.compact()?;
        }

        Ok(())
    }

    /// Determines if we are in an edge case of deleting the last row (physically) on the page:
    /// | row 1  | row 2  | row 3  |
    ///                            ^
    ///                         free_start
    /// if we delete row 3, we can safely move the free_start pointer to where row2 ends:
    /// | row 1  | row 2  |
    ///                   ^
    ///               free_start
    /// this means the page will not be fragmented, thus compaction will not be needed.
    /// # Arguments
    ///
    /// * `deleted_index` - The `usize` index of the row that we are going to delete
    ///
    /// # Returns
    ///
    /// * `Result<Option<usize>, DeleteError>` - Returns `Some(new_free_start)` if a new free start is found,
    ///   `None` if we are not in this particular edge case and we cannot avoid fragmenting the page.
    ///
    /// # Errors
    ///
    /// * Returns `DeleteError` if the slot index is invalid or other errors occur during the operation.
    fn try_to_find_new_free_start(
        &mut self,
        deleted_idx: usize,
    ) -> Result<Option<usize>, DeleteError> {
        let slot_array = self.slot_array_ref()?;

        // Store the slot info for the row that is placed at the highest offset in the page
        let (mut last_offset, mut last_len, mut last_idx) = (0usize, 0usize, 0usize);
        // Also store the slot info for the row at the 2nd to highest offset in the page. The end of this row will become the new free_start
        let (mut next_to_last_offset, mut next_to_last_len) = (0usize, 0usize);

        let slot_count = self.header_ref()?.get_slot_count()? as usize;

        for idx in 0..slot_count {
            let current_slot = slot_array.slot_ref(idx as u32)?;

            if !self.is_slot_valid(&current_slot)? {
                continue;
            }

            let (offset, len) = (
                current_slot.offset()? as usize,
                current_slot.length()? as usize,
            );

            // If the current slot points to a bigger offset than what we have so far
            if last_idx == usize::MAX || offset >= last_offset {
                // First capture the previous max as the 2nd max
                next_to_last_offset = last_offset;
                next_to_last_len = last_len;

                // And replace the current max
                last_offset = offset;
                last_len = len;
                last_idx = idx;
            } else if offset > next_to_last_offset && offset < last_offset {
                // We did not find a new global max, but we did find a new 2nd max
                next_to_last_offset = offset;
                next_to_last_len = len;
            }
        }

        // we have not found any valid slot in the array - mostly covers the edge case where we only have invalid slots in the array.
        if last_idx == usize::MAX {
            return Ok(None);
        }

        // We are not deleting the rightmost row, so nothing to do here anymore. Oh well we tried.
        if last_idx != deleted_idx {
            return Ok(None);
        }

        // The new free start will be at the 2nd highest offset + length of that row.
        let new_free_start = if next_to_last_offset + next_to_last_len == 0 {
            HEADER_SIZE
        } else {
            next_to_last_offset + next_to_last_len
        };

        Ok(Some(new_free_start))
    }
}
