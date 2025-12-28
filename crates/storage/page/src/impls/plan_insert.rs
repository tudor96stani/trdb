use crate::errors::insert_error::InsertError;
use crate::errors::slot_error::SlotError;
use crate::impls::Page;
use crate::insertion_plan::{InsertionOffset, InsertionPlan, InsertionSlot};
use crate::slot::{SLOT_SIZE, SlotRef};

impl Page {
    pub(super) fn plan_insert_internal(
        &self,
        row_len: usize,
    ) -> Result<InsertionPlan, InsertError> {
        // Decide which slot will be used (reused or new)
        let slot = self.get_insertion_slot()?; // Reuse(idx) or New

        // Global available space check.
        // Total free space must cover row bytes + (slot entry bytes if we need a new slot).
        let header = self.header_ref()?;
        let page_free_space = header.get_free_space()? as usize;

        let needs_new_slot = matches!(slot, InsertionSlot::New);
        let required_total = row_len + if needs_new_slot { SLOT_SIZE } else { 0 };

        if page_free_space < required_total {
            return Err(InsertError::NotEnoughSpace {
                row_len,
                page_free_space,
            });
        }

        // Offset planning
        let offset = self.find_insertion_offset(row_len)?;

        Ok(InsertionPlan { slot, offset })
    }

    fn get_insertion_slot(&self) -> Result<InsertionSlot, InsertError> {
        let header = self.header_ref()?;
        let slot_array = self.slot_array_ref()?;
        let slot_count = header.get_slot_count()? as usize;

        for slot_index in 0..slot_count {
            let current_slot = slot_array.slot_ref(slot_index as u32)?;
            if !self.is_slot_valid(&current_slot)? {
                return Ok(InsertionSlot::Reuse(slot_index));
            }
        }

        Ok(InsertionSlot::New)
    }

    fn find_insertion_offset(&self, row_len: usize) -> Result<InsertionOffset, InsertError> {
        let header = self.header_ref()?;
        let slot_array = self.slot_array_ref()?;

        let free_start = header.get_free_start()? as usize;
        let free_end = header.get_free_end()? as usize;
        let slot_count = header.get_slot_count()? as usize;

        // Fast path: row fits in contiguous free area (no compaction needed)
        if free_end.saturating_sub(free_start) >= row_len {
            return Ok(InsertionOffset::Exact(free_start));
        }

        // Collect physical extents of all valid rows.
        // Slot index order != physical order, so we sort by offset.
        let mut extents: Vec<(usize, usize)> = Vec::new(); // (start, end)

        for i in 0..slot_count {
            let s = slot_array.slot_ref(i as u32)?;
            if self.is_slot_valid(&s)? {
                let start = s.offset()? as usize;
                let end = start + s.length()? as usize;
                extents.push((start, end));
            }
        }

        // If there are no valid rows, then after compaction the page becomes
        // one contiguous free region
        if extents.is_empty() {
            return Ok(InsertionOffset::AfterCompactionFreeStart);
        }

        extents.sort_by_key(|(start, _end)| *start);

        // Check gaps between consecutive rows
        for w in extents.windows(2) {
            let (_a_start, a_end) = w[0];
            let (b_start, _b_end) = w[1];

            // If b_start < a_end, rows overlap / corruption; ignore for placement.
            if b_start >= a_end && (b_start - a_end) >= row_len {
                return Ok(InsertionOffset::Exact(a_end));
            }
        }

        // Check tail gap after the physically last row up to free_end
        let (_last_start, last_end) = *extents.last().unwrap();
        if free_end >= last_end && (free_end - last_end) >= row_len {
            return Ok(InsertionOffset::Exact(last_end));
        }

        // No contiguous placement found => compaction required
        Ok(InsertionOffset::AfterCompactionFreeStart)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::page_op_error::PageOpError;
    use crate::page_id::PageId;
    use crate::page_type::PageType;
    use crate::slot::SLOT_SIZE;
    use crate::{HEADER_SIZE, PAGE_SIZE};

    #[test]
    fn get_insertion_slot_no_slots_returns_new() {
        let page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        assert!(matches!(
            page.get_insertion_slot().unwrap(),
            InsertionSlot::New
        ));
    }

    #[test]
    fn get_insertion_slot_all_slots_valid_returns_new() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        let slot_count: u16 = 2;
        page.header_mut()
            .unwrap()
            .set_slot_count(slot_count)
            .unwrap();
        let new_free_end = (PAGE_SIZE - 1 - (slot_count as usize * SLOT_SIZE)) as u16;
        page.header_mut()
            .unwrap()
            .set_free_end(new_free_end)
            .unwrap();

        // Populate both slots with non-zero offset and length
        {
            let mut sa = page.slot_array_mut().unwrap();
            sa.set_slot(0, HEADER_SIZE as u16, 10).unwrap();
            sa.set_slot(1, (HEADER_SIZE + 10) as u16, 20).unwrap();
        }

        assert!(matches!(
            page.get_insertion_slot().unwrap(),
            InsertionSlot::New
        ));
    }

    #[test]
    fn get_insertion_slot_reuses_first_invalid_slot() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        let slot_count: u16 = 3;
        page.header_mut()
            .unwrap()
            .set_slot_count(slot_count)
            .unwrap();
        let new_free_end = (PAGE_SIZE - 1 - (slot_count as usize * SLOT_SIZE)) as u16;
        page.header_mut()
            .unwrap()
            .set_free_end(new_free_end)
            .unwrap();

        // Leave slot 0 as the default (zeros -> invalid). Set slots 1 and 2 to valid values.
        {
            let mut sa = page.slot_array_mut().unwrap();
            sa.set_slot(1, HEADER_SIZE as u16, 10).unwrap();
            sa.set_slot(2, (HEADER_SIZE + 10) as u16, 20).unwrap();
        }

        assert!(matches!(
            page.get_insertion_slot().unwrap(),
            InsertionSlot::Reuse(0)
        ));
    }

    #[test]
    fn get_insertion_slot_reuses_middle_invalid_slot() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        let slot_count: u16 = 3;
        page.header_mut()
            .unwrap()
            .set_slot_count(slot_count)
            .unwrap();
        let new_free_end = (PAGE_SIZE - 1 - (slot_count as usize * SLOT_SIZE)) as u16;
        page.header_mut()
            .unwrap()
            .set_free_end(new_free_end)
            .unwrap();

        // Make slot 0 valid, leave slot 1 invalid (zeros), slot 2 valid
        {
            let mut sa = page.slot_array_mut().unwrap();
            sa.set_slot(0, HEADER_SIZE as u16, 8).unwrap();
            // slot 1 left as zeros
            sa.set_slot(2, (HEADER_SIZE + 8) as u16, 16).unwrap();
        }

        assert!(matches!(
            page.get_insertion_slot().unwrap(),
            InsertionSlot::Reuse(1)
        ));
    }

    // Tests for find_insertion_offset
    #[test]
    fn find_insertion_offset_fast_path_returns_free_start() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        // No slots => slot_count == 0, slot array must be empty: set free_end to PAGE_SIZE-1
        page.header_mut().unwrap().set_free_start(100).unwrap();
        page.header_mut()
            .unwrap()
            .set_free_end((PAGE_SIZE - 1) as u16)
            .unwrap();

        let res = page.find_insertion_offset(50).unwrap();
        assert!(matches!(res, InsertionOffset::Exact(100)));
    }

    #[test]
    fn find_insertion_offset_no_valid_rows_requires_compaction() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        // Create one slot but leave it invalid (zeros) so extents will be empty.
        let slot_count: u16 = 1;
        page.header_mut()
            .unwrap()
            .set_slot_count(slot_count)
            .unwrap();
        let new_free_end = (PAGE_SIZE - 1 - (slot_count as usize * SLOT_SIZE)) as u16;
        // Make the contiguous free region small so fast path fails
        page.header_mut()
            .unwrap()
            .set_free_end(new_free_end)
            .unwrap();
        page.header_mut()
            .unwrap()
            .set_free_start(new_free_end - 10)
            .unwrap();

        let res = page.find_insertion_offset(50).unwrap();
        assert!(matches!(res, InsertionOffset::AfterCompactionFreeStart));
    }

    #[test]
    fn find_insertion_offset_finds_gap_between_rows() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        let slot_count: u16 = 2;
        page.header_mut()
            .unwrap()
            .set_slot_count(slot_count)
            .unwrap();
        // Ensure fast path does not trigger by making free_start very close to free_end
        let new_free_end = (PAGE_SIZE - 1 - (slot_count as usize * SLOT_SIZE)) as u16;
        page.header_mut()
            .unwrap()
            .set_free_end(new_free_end)
            .unwrap();
        page.header_mut()
            .unwrap()
            .set_free_start(new_free_end - 5)
            .unwrap();

        // Place two rows with a gap between them (physical offsets earlier in the page)
        {
            let mut sa = page.slot_array_mut().unwrap();
            sa.set_slot(0, HEADER_SIZE as u16, 10).unwrap(); // ends at HEADER_SIZE+10
            sa.set_slot(1, (HEADER_SIZE + 30) as u16, 10).unwrap(); // starts after a gap
        }

        let expected = HEADER_SIZE + 10;
        let res = page.find_insertion_offset(15).unwrap();
        assert!(matches!(res, InsertionOffset::Exact(pos) if pos == expected));
    }

    #[test]
    fn find_insertion_offset_finds_tail_gap_after_last_row() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        let slot_count: u16 = 1;
        page.header_mut()
            .unwrap()
            .set_slot_count(slot_count)
            .unwrap();
        let new_free_end = (PAGE_SIZE - 1 - (slot_count as usize * SLOT_SIZE)) as u16;
        // Set free_start close to free_end so fast path does not trigger
        page.header_mut()
            .unwrap()
            .set_free_end(new_free_end)
            .unwrap();
        page.header_mut()
            .unwrap()
            .set_free_start(new_free_end - 5)
            .unwrap();

        // Single row that ends well before free_end
        {
            let mut sa = page.slot_array_mut().unwrap();
            sa.set_slot(0, HEADER_SIZE as u16, 8).unwrap();
        }

        let last_end = (HEADER_SIZE + 8);
        let res = page.find_insertion_offset(10).unwrap();
        assert!(matches!(res, InsertionOffset::Exact(pos) if pos == last_end));
    }

    #[test]
    fn find_insertion_offset_no_contiguous_placement_requires_compaction() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        let slot_count: u16 = 2;
        page.header_mut()
            .unwrap()
            .set_slot_count(slot_count)
            .unwrap();
        let new_free_end = (PAGE_SIZE - 1 - (slot_count as usize * SLOT_SIZE)) as u16;
        // Set free_start such that the tail gap is smaller than the requested row_len
        page.header_mut()
            .unwrap()
            .set_free_end(new_free_end)
            .unwrap();
        page.header_mut()
            .unwrap()
            .set_free_start(new_free_end - 5)
            .unwrap();

        // Place two adjacent rows located just before free_end so there's no internal gap
        {
            let mut sa = page.slot_array_mut().unwrap();
            // First row starts 40 bytes before free_end and is 20 bytes long (ends at free_end-20)
            sa.set_slot(0, (new_free_end - 40), 20).unwrap();
            // Second row starts at free_end-20 and is 20 bytes long (ends at free_end)
            sa.set_slot(1, (new_free_end - 20), 20).unwrap();
        }

        let res = page.find_insertion_offset(10).unwrap();
        assert!(matches!(res, InsertionOffset::AfterCompactionFreeStart));
    }
}
