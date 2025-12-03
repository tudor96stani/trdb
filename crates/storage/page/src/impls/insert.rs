use crate::errors::insert_error::InsertError;
use crate::impls::Page;
use crate::insertion_plan::InsertionPlan;
use crate::slot::SLOT_SIZE;

/// Internal insert methods for the `Page` struct.
impl Page {
    // TODO this method will require numerous tests
    pub(super) fn plan_insert_internal(
        &self,
        row_len: usize,
    ) -> Result<InsertionPlan, InsertError> {
        let header = self.header_ref();

        if !self.row_size_fits(row_len)? {
            return Err(InsertError::NotEnoughSpace {
                row_len,
                page_free_space: header.get_free_space()? as usize,
            });
        }

        let mut unused_slot: Option<u32> = None;
        let mut available_space_start =
            if (header.get_free_end()? - header.get_free_start()?) as usize > row_len {
                Some(header.get_free_start()? as usize)
            } else {
                None
            };

        let mut inserting_at_free_start = available_space_start.is_some();

        for slot_index in 0..header.get_slot_count()? {
            if unused_slot.is_some() && available_space_start.is_some() {
                break;
            }

            if unused_slot.is_none() {
                let current_slot = self.slot_array()?.slot_ref(slot_index as u32)?;
                if current_slot.length()? == 0 && current_slot.offset()? == 0 {
                    unused_slot = Some(slot_index as u32)
                }
            }

            if available_space_start.is_none() && slot_index < header.get_slot_count()? - 1 {
                let current_slot = self.slot_array()?.slot_ref(slot_index as u32)?;
                let next_slot = self.slot_array()?.slot_ref((slot_index + 1) as u32)?;
                let gap = next_slot.offset()? - (current_slot.offset()? + current_slot.length()?);
                if gap >= row_len as u16 {
                    available_space_start =
                        Some((current_slot.offset()? + current_slot.length()?) as usize);
                }
            }
        }

        if unused_slot.is_none() {
            unused_slot = Some(header.get_slot_count()? as u32);
        }

        let needs_compaction = available_space_start.is_none()
            && header.get_can_compact()? != 0
            && header.needs_compaction(row_len)?;

        if needs_compaction {
            available_space_start = Some(header.get_free_start()? as usize);
            inserting_at_free_start = true;
        }

        if available_space_start.is_none() {
            return Err(InsertError::CannotFindSpace {
                required_space: row_len,
            });
        }

        let inserting_new_slot = unused_slot.unwrap() == header.get_slot_count()? as u32;

        Ok(InsertionPlan {
            slot_number: unused_slot.unwrap() as u16,
            start_offset: available_space_start.unwrap(),
            inserting_at_free_start,
            inserting_new_slot,
            needs_compaction,
        })
    }

    pub(super) fn insert_row_unsorted_internal(
        &mut self,
        plan: InsertionPlan,
        bytes: Vec<u8>,
    ) -> Result<(), InsertError> {
        let mut header_mut = self.header_mut();

        // Could not find available space during the insertion planning => compact now if needed
        if plan.needs_compaction {
            todo!("Compaction not yet implemented")
        }

        // If we are inserting at free start, use the *current* freeStart after potential compaction.
        // Otherwise, use the offset we planned in the gap.
        let start_offset = if plan.inserting_at_free_start {
            header_mut.get_free_start()? as usize
        } else {
            plan.start_offset
        };

        // Update the total slot count in the header - only increment it if we are generating a new slot
        let updated_total_slots =
            header_mut.get_slot_count()? + if plan.inserting_new_slot { 1 } else { 0 };
        header_mut.set_slot_count(updated_total_slots)?;

        // Only update free start if we are inserting there - if we are placing it in some free fragment, free start can stay the same
        if plan.inserting_at_free_start {
            let new_free_start = header_mut.get_free_start()? + bytes.len() as u16;
            header_mut.set_free_start(new_free_start)?
        }

        // Only update the free end if we are creating a new slot - if we are re-using one of the existing unused ones, the free end can stay the same
        if plan.inserting_new_slot {
            let new_free_end = header_mut.get_free_end()? - SLOT_SIZE as u16;
            header_mut.set_free_end(new_free_end)?
        }

        // freeSpace is always updated -> but depending on whether we are creating a new slot or reusing an existing one,
        // we need to subtract the size of a slot (for creating a new slot) or 0 (for reusing an existing slot)
        let new_free_space = header_mut.get_free_space()?
            - bytes.len() as u16
            - if plan.inserting_new_slot {
                SLOT_SIZE as u16
            } else {
                0
            };
        header_mut.set_free_space(new_free_space)?;

        // Insert the actual row in the page
        self.data[start_offset..(start_offset + bytes.len())].copy_from_slice(bytes.as_slice());

        // Right now, the header should contain the latest information regarding the number of slots (whether we added a new one or not),
        // so the slot array should include any new bytes segment. We place the new slot directly at the requested index
        self.slot_array_mut()?.set_slot(
            plan.slot_number as u32,
            start_offset as u16,
            bytes.len() as u16,
        );

        Ok(())
    }
}
