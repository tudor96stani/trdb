use crate::errors::header_error::HeaderError;
use crate::errors::slot_error::SlotError;
use crate::page::api::Page;
use crate::slot::{SLOT_SIZE, SlotRef};
use crate::slot_array::{SlotArrayMut, SlotArrayRef};
use crate::{HEADER_SIZE, PAGE_SIZE};

/// Internal methods for the `Page` struct.
impl Page {
    /// Returns an immutable view of the slot array.
    #[inline]
    pub(crate) fn slot_array_ref(&'_ self) -> Result<SlotArrayRef<'_>, SlotError> {
        let free_end_offset = self.header_ref()?.get_free_end()? as usize;
        let slot_count = self.header_ref()?.get_slot_count()?;
        SlotArrayRef::new(&self.data[free_end_offset + 1..PAGE_SIZE], slot_count)
    }

    /// Returns a mutable view of the slot array.
    pub(crate) fn slot_array_mut(&'_ mut self) -> Result<SlotArrayMut<'_>, SlotError> {
        let free_end_offset = self.header_ref()?.get_free_end()? as usize;
        let slot_count = self.header_ref()?.get_slot_count()?;
        SlotArrayMut::new(&mut self.data[free_end_offset + 1..PAGE_SIZE], slot_count)
    }

    /// Determines whether the requested row size fits on the page.
    /// Does not account for fragmentation (i.e., row might fit only after a compaction of the page).
    /// Returns a boolean or error if something goes wrong while processing the header.
    #[inline]
    pub(super) fn row_size_fits(&self, row_size: usize) -> Result<bool, HeaderError> {
        Ok(self.header_ref()?.get_free_space()? >= (row_size + SLOT_SIZE) as u16)
    }

    /// Determines if a slot is valid (used) or it has been invalidated (row referenced by it was deleted).
    pub(super) fn is_slot_valid(&self, slot: &SlotRef) -> Result<bool, SlotError> {
        Ok(slot.length()? != 0 && slot.offset()? != 0)
    }

    pub(super) fn compact(&mut self) -> Result<(), SlotError> {
        let start = HEADER_SIZE;
        let end = self.header_ref()?.get_free_end()? as usize;

        let mut new_buffer = vec![0u8; end - start];
        let total_slots = self.header_ref()?.get_slot_count()? as usize;
        let mut write_head = 0usize;

        for slot_index in 0..total_slots {
            let (slot_offset, slot_length) = {
                let slot = self.slot_array_ref()?.slot_ref(slot_index as u32)?;
                if !self.is_slot_valid(&slot)? {
                    continue;
                };
                (slot.offset()? as usize, slot.length()? as usize)
            };

            let source = slot_offset..(slot_offset + slot_length);
            let destination = write_head..(write_head + slot_length);

            new_buffer[destination].copy_from_slice(&self.data[source]);

            let new_offset = start + write_head;
            self.slot_array_mut()?.set_slot(
                slot_index as u32,
                new_offset as u16,
                slot_length as u16,
            )?;
            write_head += slot_length;
        }

        self.data[start..start + write_head].copy_from_slice(&new_buffer[..write_head]);
        self.header_mut()?
            .set_free_start((start + write_head) as u16);

        Ok(())
    }
}

#[cfg(test)]
mod private_methods_tests {
    use super::*;
    use crate::page_id::PageId;
    use crate::page_type::PageType;

    // region Row fits
    #[test]
    fn row_fits_enough_space() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        assert!(page.row_size_fits(100).unwrap());
    }

    #[test]
    fn row_fits_at_limit() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        assert!(page.row_size_fits(3996).unwrap());
    }

    #[test]
    fn row_fits_slot_would_not_fit() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        assert!(!page.row_size_fits(3998).unwrap());
    }

    #[test]
    fn row_fits_would_not_fit_at_all() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        assert!(!page.row_size_fits(4000).unwrap());
    }
    // endregion

    // region Slot array
    #[test]
    fn slot_array_corrupted_header_returns_error() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        page.header_mut().unwrap().set_free_end(4090);
        page.header_mut().unwrap().set_slot_count(10);

        let result = page.slot_array_ref();
        assert!(matches!(
            result,
            Err(SlotError::SlotRegionSizeMismatch {
                expected_size: 40,
                actual_size: 5
            })
        ))
    }
    // endregion

    // region Compact
    #[test]
    fn compact_with_no_slots_sets_free_end_to_header_size() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        // Ensure no slots
        page.header_mut().unwrap().set_slot_count(0).unwrap();
        // Call compact
        page.compact().unwrap();
        // free_start should become HEADER_SIZE
        assert_eq!(
            page.header_ref().unwrap().get_free_start().unwrap() as usize,
            HEADER_SIZE
        );
    }

    #[test]
    fn compact_with_all_slots_invalid_sets_free_end_to_header_size_and_leaves_slots_zero() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        // Allocate two slots but leave them zeroed (invalid)
        let slot_count: u16 = 2;
        page.header_mut()
            .unwrap()
            .set_slot_count(slot_count)
            .unwrap();
        let old_free_end = (PAGE_SIZE - 1 - (slot_count as usize * SLOT_SIZE)) as u16;
        page.header_mut()
            .unwrap()
            .set_free_end(old_free_end)
            .unwrap();

        // Capture the physical slot-array bytes region BEFORE compaction
        let slot_region_start = old_free_end as usize + 1;
        // Do not copy the region yet — compact will update it in-place. Remember start index.

        // Compact
        page.compact().unwrap();

        // After compact, free_start should be HEADER_SIZE
        assert_eq!(
            page.header_ref().unwrap().get_free_start().unwrap() as usize,
            HEADER_SIZE
        );

        // The original physical slot array region (now possibly updated) should still contain zeros
        let slot_region = page.data[slot_region_start..PAGE_SIZE].to_vec();
        assert_eq!(slot_region, vec![0u8; slot_count as usize * SLOT_SIZE]);
    }

    #[test]
    fn compact_moves_valid_rows_in_slot_index_order_and_updates_slots() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        // Prepare 3 slots; slot 0 and 2 are valid, slot 1 invalid
        let slot_count: u16 = 3;
        page.header_mut()
            .unwrap()
            .set_slot_count(slot_count)
            .unwrap();
        let old_free_end = (PAGE_SIZE - 1 - (slot_count as usize * SLOT_SIZE)) as u16;
        page.header_mut()
            .unwrap()
            .set_free_end(old_free_end)
            .unwrap();

        // Fill source data at distinct locations
        let a_offset = (HEADER_SIZE + 10) as u16;
        let a_len = 5u16;
        let b_offset = (HEADER_SIZE + 100) as u16; // for slot 2
        let b_len = 3u16;

        // Write distinct bytes so we can verify copy
        for i in 0..a_len as usize {
            page.data[a_offset as usize + i] = 0xAAu8.wrapping_add(i as u8);
        }
        for i in 0..b_len as usize {
            page.data[b_offset as usize + i] = 0xC0u8.wrapping_add(i as u8);
        }

        {
            let mut sa = page.slot_array_mut().unwrap();
            sa.set_slot(0, a_offset, a_len).unwrap();
            // slot 1 left zero
            sa.set_slot(2, b_offset, b_len).unwrap();
        }

        // Capture the original physical slot array bytes region BEFORE compaction
        // Remember the start index of the slot array region; we'll read it after compaction to inspect updated entries
        let slot_region_start = old_free_end as usize + 1;
        eprintln!("DEBUG compact: slot_region_start={}", slot_region_start);

        // Run compact
        page.compact().unwrap();

        // After compact, data should be laid out starting at HEADER_SIZE in slot-index order: slot0 then slot2
        let header = page.header_ref().unwrap();
        let expected_first = HEADER_SIZE;
        let expected_second = HEADER_SIZE + a_len as usize;

        // The compact implementation writes updated slot entries into the ORIGINAL slot array region
        // (slot_array_mut used the header values that were valid during compaction). Inspect that region.
        let slot_region_bytes = page.data[slot_region_start..PAGE_SIZE].to_vec();
        eprintln!(
            "DEBUG before SlotArrayRef::new: slot_region_bytes.len()={}",
            slot_region_bytes.len()
        );
        let sa_ref = SlotArrayRef::new(&slot_region_bytes, slot_count).unwrap();
        let s0 = sa_ref.slot_ref(0).unwrap();
        assert_eq!(s0.offset().unwrap() as usize, expected_first);
        assert_eq!(s0.length().unwrap(), a_len);
        let s1 = sa_ref.slot_ref(1).unwrap();
        assert_eq!(s1.offset().unwrap(), 0); // kept invalid
        assert_eq!(s1.length().unwrap(), 0);
        let s2 = sa_ref.slot_ref(2).unwrap();
        assert_eq!(s2.offset().unwrap() as usize, expected_second);
        assert_eq!(s2.length().unwrap(), b_len);

        // verify the actual bytes were copied
        for i in 0..a_len as usize {
            assert_eq!(page.data[expected_first + i], 0xAAu8.wrapping_add(i as u8));
        }
        for i in 0..b_len as usize {
            assert_eq!(page.data[expected_second + i], 0xC0u8.wrapping_add(i as u8));
        }

        // free_start should be HEADER_SIZE + total_moved (per current compact implementation)
        let total = a_len as usize + b_len as usize;
        assert_eq!(
            header.get_free_start().unwrap() as usize,
            HEADER_SIZE + total
        );
    }
    // endregion
}
