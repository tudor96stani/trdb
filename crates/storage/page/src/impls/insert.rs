use crate::errors::insert_error::InsertError;
use crate::errors::slot_error::SlotError;
use crate::impls::Page;
use crate::insertion_plan::InsertionPlan;
use crate::slot::{SLOT_SIZE, SlotRef};

/// Internal insert methods for the `Page` struct.
impl Page {
    /// Internal method for planning the insert of a record in a heap unsorted page
    pub(super) fn plan_insert_internal(
        &self,
        row_len: usize,
    ) -> Result<InsertionPlan, InsertError> {
        let header = self.header_ref()?;
        let slot_array_ref = self.slot_array_ref()?;
        let free_start = header.get_free_start()? as usize;
        let free_end = header.get_free_end()? as usize;
        let slot_count = header.get_slot_count()? as usize;

        if !self.row_size_fits(row_len)? {
            return Err(InsertError::NotEnoughSpace {
                row_len,
                page_free_space: header.get_free_space()? as usize,
            });
        }

        // Attempt to look for an empty slot that we can reuse from a row that has been deleted.
        let mut unused_slot: Option<usize> = None;

        // While going through the slot array, we will also look for available space,
        // in case the row cannot fit between free start & end.
        let mut available_space_start = if free_end - free_start > row_len {
            Some(free_start)
        } else {
            None
        };

        // Just a flag that will be used down the line
        // -> namely, we need to know whether we are adding the new row at the end of the data section (basically extending the data section)
        // or if we have found some fragment of space in between existing rows that we can leverage
        // For starters -> we can check if we are from the start using the freeStart as the start offset of the row.
        let mut inserting_at_free_start = available_space_start.is_some();

        for slot_index in 0..slot_count {
            // If we found both things we were looking for, we can terminate early.
            if unused_slot.is_some() && available_space_start.is_some() {
                break;
            }

            if unused_slot.is_none() {
                // Let's check if this current slot position is empty and can be used
                let current_slot = slot_array_ref.slot_ref(slot_index as u32)?;
                if !self.is_slot_valid(&current_slot)? {
                    unused_slot = Some(slot_index)
                }
            }

            // Only go up to the next-to-last slot for this check.
            // example: inserting 10 bytes, with this slot array:
            //        current,      next
            // ....., (1050, 50), (1120, 30),....
            // which means the data in the page looks like ( . = used space, _ = free space)
            //     1050      1099  1100       1119   1120
            // .... a ........ a    _ ___________     b .......
            // if 1120 - 1050 - 50 = 20 > 10 bytes we want to insert => we can use this space
            // the start position will be at 1050 + 50 = 1100
            if available_space_start.is_none() && slot_index < slot_count - 1 {
                let current_slot = slot_array_ref.slot_ref(slot_index as u32)?;
                let next_slot = slot_array_ref.slot_ref((slot_index + 1) as u32)?;
                let gap = next_slot
                    .offset()?
                    .checked_sub(current_slot.offset()? + current_slot.length()?);
                if let Some(g) = gap
                    && g >= row_len as u16
                    && self.is_slot_valid(&next_slot)?
                    && self.is_slot_valid(&current_slot)?
                {
                    available_space_start =
                        Some((current_slot.offset()? + current_slot.length()?) as usize);
                }
            }
        }

        // No unused slot found in the slot array -> we will append a new one at the end of the array
        if unused_slot.is_none() {
            unused_slot = Some(slot_count);
        }

        let can_compact = header.get_can_compact()?;

        // We could not find any available space. We should try to compact
        let needs_compaction =
            available_space_start.is_none() && header.needs_compaction(row_len)?;

        if needs_compaction {
            // Since we have already compacted, we will extend the data section.
            // freeStart now holds the next available position
            // todo: this is fully not correct. in theory, yes, the free_start is the correct value at which we will be inserting, but it's not the **current** free_start, but rather the one that will be computed after the compaction. as long as the insert algorithm handles this and adjusts the offset at which it does the insert, it is fine though.
            available_space_start = Some(free_start);

            // Also, set this flag to true, regardless if the value it had had.
            inserting_at_free_start = true;
        }

        // todo should this ever happen??
        if available_space_start.is_none() {
            return Err(InsertError::CannotFindSpace {
                required_space: row_len,
            });
        }

        // Should be safe to unwrap here. Above, we assigned a value to unused_slot if it was None
        let unused_slot = unused_slot.unwrap();
        let inserting_new_slot = unused_slot == slot_count;

        Ok(InsertionPlan {
            slot_number: unused_slot as u16,
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
        let mut header_mut = self.header_mut()?;

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

    fn is_slot_valid(&self, slot: &SlotRef) -> Result<bool, SlotError> {
        Ok(slot.length()? != 0 && slot.offset()? != 0)
    }
}
