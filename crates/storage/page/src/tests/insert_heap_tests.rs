#[cfg(test)]
mod tests {
    use super::*;
    use crate::impls::Page;
    use crate::insertion_plan::{InsertionOffset, InsertionPlan, InsertionSlot};
    use crate::page_id::PageId;
    use crate::page_type::PageType;
    use crate::slot::SLOT_SIZE;
    use crate::{HEADER_SIZE, PAGE_SIZE};

    #[test]
    fn insert_row_new_slot_at_free_start_updates_header_and_slot() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();

        // Default header: free_start == HEADER_SIZE, free_end == PAGE_SIZE-1, free_space == PAGE_SIZE-HEADER_SIZE
        let old_slot_count = page.header_ref().unwrap().get_slot_count().unwrap() as usize;
        assert_eq!(old_slot_count, 0);

        let old_free_start = page.header_ref().unwrap().get_free_start().unwrap() as usize;
        let old_free_end = page.header_ref().unwrap().get_free_end().unwrap() as usize;
        let old_free_space = page.header_ref().unwrap().get_free_space().unwrap() as usize;

        let bytes = vec![1u8; 10];
        let plan = InsertionPlan {
            slot: InsertionSlot::New,
            offset: InsertionOffset::Exact(old_free_start),
        };

        page.insert_heap(plan, bytes.clone()).unwrap();

        // Header updates
        let header = page.header_ref().unwrap();
        assert_eq!(header.get_slot_count().unwrap(), 1);
        assert_eq!(
            header.get_free_end().unwrap() as usize,
            old_free_end - SLOT_SIZE
        );
        assert_eq!(
            header.get_free_start().unwrap() as usize,
            old_free_start + bytes.len()
        );
        let expected_free_space = old_free_space - bytes.len() - SLOT_SIZE;
        assert_eq!(
            header.get_free_space().unwrap() as usize,
            expected_free_space
        );

        // Slot entry and data
        let slot = page.slot_array_ref().unwrap().slot_ref(0).unwrap();
        assert_eq!(slot.offset().unwrap() as usize, old_free_start);
        assert_eq!(slot.length().unwrap() as usize, bytes.len());
        assert_eq!(
            &page.data_mut()[old_free_start..old_free_start + bytes.len()],
            bytes.as_slice()
        );
    }

    #[test]
    fn insert_row_reuse_slot_into_gap_does_not_move_free_start_and_decreases_only_row_space() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();

        // Prepare 3 slots where slot 1 is invalid (to be reused). Adjust free_end for slot_count=3.
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

        // Place two valid rows leaving a gap between them; leave slot 1 zeros
        let a_start = HEADER_SIZE as u16;
        let a_len = 8u16;
        let b_start = (HEADER_SIZE + 30) as u16;
        let b_len = 12u16;

        let mut sa = page.slot_array_mut().unwrap();
        sa.set_slot(0, a_start, a_len).unwrap();
        // slot 1 left as zeros -> invalid
        sa.set_slot(2, b_start, b_len).unwrap();

        // Remember free_start and free_space before insertion
        let old_free_start = page.header_ref().unwrap().get_free_start().unwrap() as usize;
        let old_free_space = page.header_ref().unwrap().get_free_space().unwrap() as usize;

        // Insert into the gap between a and b: offset should be a_start + a_len
        let insert_offset = (a_start + a_len) as usize;
        let bytes = vec![9u8; 5];
        let plan = InsertionPlan {
            slot: InsertionSlot::Reuse(1),
            offset: InsertionOffset::Exact(insert_offset),
        };

        page.insert_heap(plan, bytes.clone()).unwrap();

        // Header: slot_count unchanged, free_start unchanged, free_space decreased by only row bytes
        let header = page.header_ref().unwrap();
        assert_eq!(
            header.get_slot_count().unwrap() as usize,
            slot_count as usize
        );
        assert_eq!(header.get_free_start().unwrap() as usize, old_free_start);
        assert_eq!(
            header.get_free_space().unwrap() as usize,
            old_free_space - bytes.len()
        );

        // Slot 1 should now point to the inserted data
        let slot1 = page.slot_array_ref().unwrap().slot_ref(1).unwrap();
        assert_eq!(slot1.offset().unwrap() as usize, insert_offset);
        assert_eq!(slot1.length().unwrap() as usize, bytes.len());
        assert_eq!(
            &page.data_mut()[insert_offset..insert_offset + bytes.len()],
            bytes.as_slice()
        );
    }

    #[test]
    fn insert_row_reuse_at_free_start_advances_free_start_and_does_not_change_slot_count() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();

        // Prepare single slot (invalid) so it will be reused. Set free_end accordingly.
        let slot_count: u16 = 1;
        page.header_mut()
            .unwrap()
            .set_slot_count(slot_count)
            .unwrap();
        let new_free_end = (PAGE_SIZE - 1 - (slot_count as usize * SLOT_SIZE)) as u16;
        page.header_mut()
            .unwrap()
            .set_free_end(new_free_end)
            .unwrap();

        let old_free_start = page.header_ref().unwrap().get_free_start().unwrap() as usize;
        let old_free_space = page.header_ref().unwrap().get_free_space().unwrap() as usize;

        let bytes = vec![7u8; 6];
        let plan = InsertionPlan {
            slot: InsertionSlot::Reuse(0),
            offset: InsertionOffset::Exact(old_free_start),
        };

        page.insert_heap(plan, bytes.clone()).unwrap();

        let header = page.header_ref().unwrap();
        // slot count unchanged
        assert_eq!(
            header.get_slot_count().unwrap() as usize,
            slot_count as usize
        );
        // free_start advanced by bytes.len()
        assert_eq!(
            header.get_free_start().unwrap() as usize,
            old_free_start + bytes.len()
        );
        // free_end unchanged
        assert_eq!(
            header.get_free_end().unwrap() as usize,
            new_free_end as usize
        );
        // free_space decreased only by row bytes
        assert_eq!(
            header.get_free_space().unwrap() as usize,
            old_free_space - bytes.len()
        );

        // slot 0 updated
        let slot0 = page.slot_array_ref().unwrap().slot_ref(0).unwrap();
        assert_eq!(slot0.offset().unwrap() as usize, old_free_start);
        assert_eq!(slot0.length().unwrap() as usize, bytes.len());
        assert_eq!(
            &page.data_mut()[old_free_start..old_free_start + bytes.len()],
            bytes.as_slice()
        );
    }

    #[test]
    fn insert_row_new_slot_into_gap_appends_slot_and_updates_free_end_and_space() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();

        // Prepare two existing slots with a gap between them. Set slot_count=2 and free_end accordingly.
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

        let a_start = HEADER_SIZE as u16;
        let a_len = 8u16;
        let b_start = (HEADER_SIZE + 24) as u16; // leave gap of 16 bytes
        let b_len = 6u16;

        {
            let mut sa = page.slot_array_mut().unwrap();
            sa.set_slot(0, a_start, a_len).unwrap();
            sa.set_slot(1, b_start, b_len).unwrap();
        }

        let insert_offset = (a_start + a_len) as usize; // gap start
        let bytes = vec![3u8; 10];

        let old_slot_count = page.header_ref().unwrap().get_slot_count().unwrap() as usize;
        let old_free_start = page.header_ref().unwrap().get_free_start().unwrap() as usize;
        let old_free_end = page.header_ref().unwrap().get_free_end().unwrap() as usize;
        let old_free_space = page.header_ref().unwrap().get_free_space().unwrap() as usize;

        let plan = InsertionPlan {
            slot: InsertionSlot::New,
            offset: InsertionOffset::Exact(insert_offset),
        };
        page.insert_heap(plan, bytes.clone()).unwrap();

        let header = page.header_ref().unwrap();
        // slot_count incremented
        assert_eq!(
            header.get_slot_count().unwrap() as usize,
            old_slot_count + 1
        );
        // free_end decreased by SLOT_SIZE
        assert_eq!(
            header.get_free_end().unwrap() as usize,
            old_free_end - SLOT_SIZE
        );
        // free_start unchanged (we inserted into a gap)
        assert_eq!(header.get_free_start().unwrap() as usize, old_free_start);
        // free_space decreased by row bytes + SLOT_SIZE
        assert_eq!(
            header.get_free_space().unwrap() as usize,
            old_free_space - bytes.len() - SLOT_SIZE
        );

        // New slot should be appended at old_slot_count index
        let new_slot = page
            .slot_array_ref()
            .unwrap()
            .slot_ref(old_slot_count as u32)
            .unwrap();
        assert_eq!(new_slot.offset().unwrap() as usize, insert_offset);
        assert_eq!(new_slot.length().unwrap() as usize, bytes.len());
        assert_eq!(
            &page.data_mut()[insert_offset..insert_offset + bytes.len()],
            bytes.as_slice()
        );
    }

    #[test]
    fn insert_row_new_slot_compaction_requested() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();

        page.header_mut().unwrap().set_slot_count(2).unwrap();
        page.header_mut().unwrap().set_free_start(4050).unwrap();
        page.header_mut().unwrap().set_free_end(4087).unwrap();

        // insert 40 bytes + 4 for the new slot = 44 bytes
        // have 2 rows
        // 96 -> 4000
        // 4010 -> 4050
        page.slot_array_mut()
            .unwrap()
            .set_slot(0, 96, 3904)
            .unwrap();
        page.slot_array_mut()
            .unwrap()
            .set_slot(1, 4010, 40)
            .unwrap();

        page.data_mut()[96..4000].copy_from_slice(vec![1u8; 3904].as_slice());
        page.data_mut()[4010..4050].copy_from_slice(vec![2u8; 40].as_slice());

        let insertion_plan = InsertionPlan {
            slot: InsertionSlot::New,
            offset: InsertionOffset::AfterCompactionFreeStart,
        };

        let row = vec![3u8; 40];

        page.insert_heap(insertion_plan, row).unwrap();

        assert_eq!(
            page.header_ref().unwrap().get_free_start().unwrap() as usize,
            4080
        );
        assert_eq!(
            page.header_ref().unwrap().get_free_end().unwrap() as usize,
            4083
        );
        assert_eq!(
            page.header_ref().unwrap().get_slot_count().unwrap() as usize,
            3
        );

        let new_slot = page.slot_array_ref().unwrap().slot_ref(2).unwrap();
        assert_eq!(new_slot.offset().unwrap() as usize, 4040);
        assert_eq!(new_slot.length().unwrap() as usize, 40);

        assert_eq!(
            page.slot_array_ref()
                .unwrap()
                .slot_ref(0)
                .unwrap()
                .offset()
                .unwrap() as usize,
            96
        );
        assert_eq!(
            page.slot_array_ref()
                .unwrap()
                .slot_ref(0)
                .unwrap()
                .length()
                .unwrap() as usize,
            3904
        );
        assert_eq!(
            page.slot_array_ref()
                .unwrap()
                .slot_ref(1)
                .unwrap()
                .offset()
                .unwrap() as usize,
            4000
        );
        assert_eq!(
            page.slot_array_ref()
                .unwrap()
                .slot_ref(1)
                .unwrap()
                .length()
                .unwrap() as usize,
            40
        );

        assert_eq!(page.data_mut()[96..4000], vec![1u8; 3904]);
        assert_eq!(page.data_mut()[4000..4040], vec![2u8; 40]);
        assert_eq!(page.data_mut()[4040..4080], vec![3u8; 40]);
    }
}
