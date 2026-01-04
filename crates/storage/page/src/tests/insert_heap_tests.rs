#[cfg(test)]
mod tests {
    use super::*;
    use crate::impls::Page;
    use crate::insertion_plan::{InsertionOffset, InsertionPlan, InsertionSlot};
    use crate::page_id::PageId;
    use crate::page_type::PageType;
    use crate::slot::SLOT_SIZE;
    use crate::tests::SlotValues;
    use crate::{HEADER_SIZE, PAGE_SIZE};

    #[test]
    fn insert_row_empty_page() {
        let mut page = Page::test_create_empty_heap();

        let plan = page.plan_insert(100).unwrap();
        page.insert_heap(plan, vec![1u8; 100]).unwrap();

        page.assert_slot(0, 96, 100);
        page.assert_row_values(96, 100, 1);

        page.assert_header(&[
            // Slot count should be 1
            &|h| assert_eq!(h.get_slot_count().unwrap(), 1),
            // Free end should be 4091 since we have a single 4 byte slot
            &|h| assert_eq!(h.get_free_end().unwrap(), 4091),
            // Free start should be 196 since there is one row placed at 96 for 100 bytes
            &|h| assert_eq!(h.get_free_start().unwrap(), 196),
            // Free space should be 4000 - 100 - 4 = 3896
            &|h| assert_eq!(h.get_free_space().unwrap(), 3896),
        ])
    }

    // insert new row in fully compacted page (at free_start), create new slot
    #[test]
    fn insert_row_at_free_start_create_new_slot() {
        let mut page = Page::test_create_empty_heap();
        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 100,
            },
            SlotValues {
                offset: 196,
                len: 50,
            },
        ]);

        let row_len = 150;
        let plan = page.plan_insert(row_len).unwrap();
        page.insert_heap(plan, vec![3u8; row_len]).unwrap();

        let expected_offset_of_new_row = 96 + 100 + 50;

        page.assert_slot(2, expected_offset_of_new_row, row_len);
        page.assert_row_values(expected_offset_of_new_row, row_len, 3);

        page.assert_header(&[
            // Slot count should be 3
            &|h| assert_eq!(h.get_slot_count().unwrap(), 3),
            // Free end should be 4083 since we have a 3 4-byte slots
            &|h| assert_eq!(h.get_free_end().unwrap(), 4083),
            // Free start should be start of our row + its length
            &|h| {
                assert_eq!(
                    h.get_free_start().unwrap(),
                    (expected_offset_of_new_row + row_len) as u16
                )
            },
            // Free space should be 4000 - 100 - 50 - 150 - 12 = 3688
            &|h| assert_eq!(h.get_free_space().unwrap(), 3688),
        ])
    }

    //noinspection DuplicatedCode
    // insert new row in fully compacted page (at free start), reuse a slot
    #[test]
    fn insert_row_at_free_start_reuse_slot() {
        let mut page = Page::test_create_empty_heap();
        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 100,
            },
            SlotValues {
                offset: 196,
                len: 50,
            },
            SlotValues { offset: 0, len: 0 },
        ]);

        let row_len = 150;
        let plan = page.plan_insert(row_len).unwrap();
        page.insert_heap(plan, vec![3u8; row_len]).unwrap();

        let expected_offset_of_new_row = 96 + 100 + 50;

        page.assert_slot(2, expected_offset_of_new_row, row_len);
        page.assert_row_values(expected_offset_of_new_row, row_len, 3);

        page.assert_header(&[
            // Slot count should be 3
            &|h| assert_eq!(h.get_slot_count().unwrap(), 3),
            // Free end should be 4083 since we have a 3 4-byte slots
            &|h| assert_eq!(h.get_free_end().unwrap(), 4083),
            // Free start should be start of our row + its length
            &|h| {
                assert_eq!(
                    h.get_free_start().unwrap(),
                    (expected_offset_of_new_row + row_len) as u16
                )
            },
            // Free space should be 4000 - 100 - 50 - 150 - 12 = 3688
            &|h| assert_eq!(h.get_free_space().unwrap(), 3688),
        ])
    }

    // insert new row at free start even when there is available space between rows
    #[test]
    fn insert_row_prefer_free_start_despite_fragment_that_could_fit_row() {
        let mut page = Page::test_create_empty_heap();
        // Row 1: 96-196
        // Row 2: deleted
        // Row 3: 296-296
        // attempt to insert 100 bytes - should prefer at free_start
        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 100,
            },
            SlotValues { offset: 0, len: 0 },
            SlotValues {
                offset: 296,
                len: 50,
            },
        ]);

        let row_len = 100;
        let plan = page.plan_insert(row_len).unwrap();
        page.insert_heap(plan, vec![4u8; row_len]).unwrap();

        let expected_offset_of_new_row = 296 + 50;

        page.assert_slot(1, expected_offset_of_new_row, row_len);
        page.assert_row_values(expected_offset_of_new_row, row_len, 4);

        page.assert_header(&[
            // Slot count should be 3
            &|h| assert_eq!(h.get_slot_count().unwrap(), 3),
            // Free end should be 4083 since we have a 3 4-byte slots
            &|h| assert_eq!(h.get_free_end().unwrap(), 4083),
            // Free start should be start of our row + its length
            &|h| {
                assert_eq!(
                    h.get_free_start().unwrap(),
                    (expected_offset_of_new_row + row_len) as u16
                )
            },
            // Free space should be 4000 - 100 - 50 - 100 - 12 = 3738
            &|h| assert_eq!(h.get_free_space().unwrap(), 3738),
        ])
    }

    // insert new row in between 2 rows (perfect fit), reuse a slot
    #[test]
    fn insert_row_in_fragment_when_does_not_fit_in_free_range_reuse_slot() {
        let mut page = Page::test_create_empty_heap();
        // Row 1: 96-196
        // Row 2: deleted
        // Row 3: 296-4000
        // attempt to insert 100 bytes - should happen between rows 1 and 3
        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 100,
            },
            SlotValues { offset: 0, len: 0 },
            SlotValues {
                offset: 296,
                len: 3704,
            },
        ]);
        // Just a double check
        page.assert_header(&[&|h| assert_eq!(h.get_free_start().unwrap(), 4000)]);

        let row_len = 100;
        let plan = page.plan_insert(row_len).unwrap();
        page.insert_heap(plan, vec![4u8; row_len]).unwrap();

        let expected_offset_of_new_row = 196;

        page.assert_slot(1, expected_offset_of_new_row, row_len);
        page.assert_row_values(expected_offset_of_new_row, row_len, 4);

        page.assert_header(&[
            // Slot count should be 3
            &|h| assert_eq!(h.get_slot_count().unwrap(), 3),
            // Free end should be 4083 since we have a 3 4-byte slots
            &|h| assert_eq!(h.get_free_end().unwrap(), 4083),
            // Free start should still be 4000, should not have been updated
            &|h| assert_eq!(h.get_free_start().unwrap(), 4000),
            // Free space should be 4000-3704-100-100-12 = 84
            &|h| assert_eq!(h.get_free_space().unwrap(), 84),
        ])
    }

    // insert new row at free start only after compaction, reuse slot
    #[test]
    fn insert_row_after_compaction_reuse_slot() {
        let mut page = Page::test_create_empty_heap();
        // row 1: 96 -> 3000 (2904 bytes)
        // row 2: deleted
        // row 3: 3050 -> 4000 (950 bytes)
        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 2904,
            },
            SlotValues { offset: 0, len: 0 },
            SlotValues {
                offset: 3050,
                len: 950,
            },
        ]);
        // Just a double check
        page.assert_header(&[&|h| assert_eq!(h.get_free_start().unwrap(), 4000), &|h| {
            assert_eq!(h.get_free_space().unwrap(), 134)
        }]);

        // There are two sections of free space, both smaller than 100 bytes
        let row_len = 100;
        let plan = page.plan_insert(row_len).unwrap();
        page.insert_heap(plan, vec![4u8; row_len]).unwrap();

        // The page should first be compacted:
        // Row 1: 96-3000 (2904 bytes)
        // Row 3: 3000-3950
        // new row will be placed at 3950
        let expected_offset_of_new_row = 3950;

        page.assert_slot(1, expected_offset_of_new_row, row_len);
        page.assert_row_values(expected_offset_of_new_row, row_len, 4);

        page.assert_header(&[
            // Slot count should be 3
            &|h| assert_eq!(h.get_slot_count().unwrap(), 3),
            // Free end should be 4083 since we have a 3 4-byte slots
            &|h| assert_eq!(h.get_free_end().unwrap(), 4083),
            // Free start should be 4050, after compacting everything the initial rows went up to 3950, and we added an 100 bytes row
            &|h| assert_eq!(h.get_free_start().unwrap(), 4050),
            // Free space should be 4000-2904-950-100-12 = 34
            &|h| assert_eq!(h.get_free_space().unwrap(), 34),
        ])
    }

    // Invalid scenarios:
    // - insert new row in between 2 rows (perfect fit), create new slot
    // - insert new row at free start only after compaction, create new slot
    // this is because in order to have a fragment, it means that a row was deleted. if the row was deleted, we still have an available/unused slot in the array
    // Technically these scenarios are possible, but will not cover them since it would mean to manually corrupt the page (insert delete the 2nd row from the page, but invalidate the last slot:
    // row1: 96-100,
    // row2: deleted,
    // row3: 196-296,
    // but have slot array defined as
    // (96,100), (196,100), (0,0) -> how did row2 become assigned

    /*
    insert row1 offset 96 len 100 => new slot => free_start = 196
    insert row2 offset 196 len 100 => new slot => free_start = 296
    delete row2 => if delete determines that we are removing the last row in the page (physically) it can shift back the free_start pointer to the previous physical row. so free_start is now 196 again
    insert row3 offset 196 len 100 => reuse existing slot1 => free_start = 296
     */
}
