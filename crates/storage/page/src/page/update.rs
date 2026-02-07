use crate::errors::header_error::HeaderError;
use crate::errors::slot_error::SlotError;
use crate::errors::update_error::UpdateError;
use crate::insertion_plan::InsertionOffset;
use crate::page::api::Page;
use binary_helpers::conversions::UsizeConversion;

impl Page {
    pub(crate) fn update_internal(
        &mut self,
        old_row_slot_index: usize,
        new_row: Vec<u8>,
    ) -> Result<(), UpdateError> {
        // Check for available space -> we only need to do free_space + old_row.len >= new_row.len
        let free_space = self.header_ref()?.get_free_space()? as usize;

        let (old_row_length, old_row_offset) = {
            let slot_array = self.slot_array_ref()?;
            let slot = slot_array.slot_ref(old_row_slot_index.to_u32()?)?;

            // quick check to ensure slot index is valid
            if !self.is_slot_valid(&slot)? {
                return Err(SlotError::InvalidSlot {
                    slot_index: old_row_slot_index,
                }
                .into());
            }

            (slot.length()? as usize, slot.offset()? as usize)
        };

        let current_free_space = free_space + old_row_length;

        if new_row.len() > current_free_space {
            return Err(UpdateError::NotEnoughSpace {
                row_len: new_row.len(),
                page_free_space: current_free_space,
            });
        }

        // Then find in which scenario we are
        // 1) new_row <= old_row => just reuse that space, we will fragment
        // 2) new_row > old_row => we need to treat this in two possible ways
        //      a) if free_end - free_start >= new_row => just place there and accept fragmentation (same as for insert)
        //      b) call the method used for insert. pass the old_row_slot_index. it should give us the offset at which we need to place our row.
        enum UpdateScenario {
            Smaller,
            Equal,
            Larger,
        }

        let scenario = if (new_row.len() < old_row_length) {
            UpdateScenario::Smaller
        } else if (new_row.len() == old_row_length) {
            UpdateScenario::Equal
        } else {
            UpdateScenario::Larger
        };

        let (insertion_offset, insert_at_free_start) = match scenario {
            UpdateScenario::Smaller | UpdateScenario::Equal => {
                // Place the row at the start offset of the old row
                (old_row_offset, false)
            }
            UpdateScenario::Larger => {
                let insertion_offset =
                    self.find_insertion_offset(new_row.len(), Some(old_row_slot_index))?;
                match insertion_offset {
                    InsertionOffset::Exact(start_offset) => {
                        // We need to determine if this exact offset is somewhere in between two rows, or at the current free_start
                        let current_free_start = self.header_ref()?.get_free_start()? as usize;
                        (start_offset, start_offset == current_free_start)
                        // Note that the insertion offset algorithm will always prioritize a faster response by stopping if the free space region can fit the row, even if this creates fragmentation.
                    }
                    InsertionOffset::AfterCompactionFreeStart => {
                        // We need to delete the old row, otherwise the compaction will have no effect.
                        self.delete_row_internal(old_row_slot_index, true)?;

                        // In this case, it is pretty clear we will be inserting at the (new) free start
                        (self.header_ref()?.get_free_start()? as usize, true)
                    }
                }
            }
        };

        if insertion_offset
            .checked_add(new_row.len())
            .is_none_or(|end| end > self.data.len())
        {
            return Err(UpdateError::NotEnoughSpace {
                row_len: new_row.len(),
                page_free_space: current_free_space,
            });
        }

        // Do the actual insertion
        self.data[insertion_offset..(insertion_offset + new_row.len())]
            .copy_from_slice(new_row.as_slice());

        // Update the slot entry and header.
        match scenario {
            UpdateScenario::Equal => {
                Ok(()) // Do nothing
            }
            UpdateScenario::Smaller => {
                // For smaller rows, since they are placed on top of the old one, we only need to update the length
                let mut slot_array_mut = self.slot_array_mut()?;
                let mut slot = slot_array_mut.slot_mut(old_row_slot_index.to_u32()?)?;
                slot.set_length(new_row.len().to_u16()?)?;

                let new_free_space = free_space + old_row_length - new_row.len();
                self.header_mut()?
                    .set_free_space(new_free_space.to_u16()?)?;

                Ok(())
            }
            UpdateScenario::Larger => {
                let mut slot_array_mut = self.slot_array_mut()?;
                let mut slot = slot_array_mut.slot_mut(old_row_slot_index.to_u32()?)?;
                slot.set_length(new_row.len().to_u16()?)?;
                slot.set_offset(insertion_offset.to_u16()?)?;

                let new_free_space = free_space + old_row_length - new_row.len();
                self.header_mut()?
                    .set_free_space(new_free_space.to_u16()?)?;

                // If insert_at_free_start is set to true, we need to update the free_start -> since we inserted there, we can just move it to after this new row
                if insert_at_free_start {
                    let new_free_start = insertion_offset + new_row.len();
                    self.header_mut()?
                        .set_free_start(new_free_start.to_u16()?)?;
                }

                Ok(())
            }
        }
    }
}
