use crate::errors::insert_error::InsertError;
use crate::insertion_plan::{InsertionOffset, InsertionPlan, InsertionSlot};
use crate::page::api::Page;
use crate::slot::SLOT_SIZE;

impl Page {
    pub(super) fn insert_row_unsorted_internal(
        &mut self,
        plan: InsertionPlan,
        bytes: Vec<u8>,
    ) -> Result<(), InsertError> {
        // If compaction is required, do it now.
        // After compaction, we will insert at the (new) free_start.
        if matches!(plan.offset, InsertionOffset::AfterCompactionFreeStart) {
            self.compact()?
        }

        let mut header_mut = self.header_mut()?;

        // Decide the concrete start offset for row bytes.
        // - Exact(pos) => use pos
        // - AfterCompactionFreeStart => use current free_start (after compaction)
        let current_free_start = header_mut.get_free_start()? as usize;
        let start_offset = match plan.offset {
            InsertionOffset::Exact(pos) => pos,
            InsertionOffset::AfterCompactionFreeStart => current_free_start,
        };

        // We only advance free_start when we are appending at the current free_start.
        // (Inserting into a gap should not move free_start.)
        let inserting_at_free_start = start_offset == current_free_start;

        // Decide which slot index we will write.
        // - Reuse(i) => write into slot i
        // - New => append at current slot_count (before increment)
        let old_slot_count = header_mut.get_slot_count()? as usize;
        let (slot_index, inserting_new_slot) = match plan.slot {
            InsertionSlot::Reuse(i) => (i, false),
            InsertionSlot::New => (old_slot_count, true),
        };

        // Update header fields: slot_count, free_start/free_end, free_space.
        if inserting_new_slot {
            header_mut.set_slot_count((old_slot_count + 1) as u16)?;
            let new_free_end = header_mut.get_free_end()? - SLOT_SIZE as u16;
            header_mut.set_free_end(new_free_end)?;
        }

        if inserting_at_free_start {
            let new_free_start = header_mut.get_free_start()? + bytes.len() as u16;
            header_mut.set_free_start(new_free_start)?;
        }

        // freeSpace always shrinks by row bytes, plus slot bytes only if creating a new slot
        let new_free_space = header_mut.get_free_space()?
            - bytes.len() as u16
            - if inserting_new_slot {
                SLOT_SIZE as u16
            } else {
                0
            };
        header_mut.set_free_space(new_free_space)?;

        // Write the row bytes
        self.data[start_offset..(start_offset + bytes.len())].copy_from_slice(bytes.as_slice());

        // Write/update the slot entry
        self.slot_array_mut()?
            .set_slot(slot_index as u32, start_offset as u16, bytes.len() as u16);

        Ok(())
    }
}
