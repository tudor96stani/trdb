#[cfg(test)]
mod plan_insert_tests {
    use super::*;
    use crate::PAGE_SIZE;
    use crate::errors::insert_error::InsertError;
    use crate::errors::page_error::PageError;
    use crate::errors::page_op_error::PageOpError;
    use crate::impls::Page;
    use crate::page_id::PageId;
    use crate::page_type::PageType;
    use crate::slot::{SLOT_SIZE, SlotMut};

    #[test]
    fn plan_insert_empty_page() {
        let page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        let row_size = 100usize;

        let plan = page.plan_insert(row_size).unwrap();

        assert_eq!(plan.slot_number, 0);
        assert_eq!(plan.start_offset, 96);
        assert!(plan.inserting_new_slot);
        assert!(plan.inserting_at_free_start);
        assert!(!plan.needs_compaction);
    }

    #[test]
    fn plan_insert_no_available_space() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();

        page.header_mut().unwrap().set_free_space(50).unwrap();

        let plan_result = page.plan_insert(100);

        assert!(plan_result.is_err());
        assert!(matches!(
            plan_result,
            Err(PageError {
                page_id: PageId {
                    file_id: 1,
                    page_number: 0
                },
                source: PageOpError::Insert(InsertError::NotEnoughSpace {
                    page_free_space: 50,
                    row_len: 100
                })
            })
        ))
    }

    #[test]
    fn plan_insert_between_two_rows() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();

        // Assume we have inserted two rows:
        // the first one: from 96 to 596 (500 bytes)
        // the 2nd one: from 1000 to 3980 (2980 bytes)
        page.header_mut()
            .unwrap()
            .set_free_space((PAGE_SIZE - 500 - 2980) as u16)
            .unwrap();

        // This free start value makes sense by looking below at how we place our slots.
        page.header_mut().unwrap().set_free_start(3980).unwrap();

        // Hack the slot array. the free end is PAGE_SIZE - 1 - 8 bytes (2 slots)
        page.header_mut()
            .unwrap()
            .set_free_end((PAGE_SIZE - 1 - 2 * SLOT_SIZE) as u16)
            .unwrap();
        page.header_mut().unwrap().set_slot_count(2).unwrap();

        // Insert the slot for the first row
        // This will be placed at offset 96, for a length of 500
        // So offset 96-596
        page.slot_array_mut().unwrap().set_slot(0, 96, 500).unwrap();

        // 2nd slot will be after a gap, at offset 1000 and span 2980 bytes
        // so offset 1000-3980. This leaves around 100 free bytes between start and end.
        page.slot_array_mut()
            .unwrap()
            .set_slot(1, 1000, 2980)
            .unwrap();

        // Now we will attempt to insert a new row of size 400. It should be placed at offset 596
        let plan = page.plan_insert(400).unwrap();

        assert_eq!(plan.slot_number, 2);
        assert_eq!(plan.start_offset, 596);
        assert!(!plan.inserting_at_free_start);
        assert!(plan.inserting_new_slot);
    }

    #[test]
    fn plan_insert_reuse_empty_slot() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();

        // We have 3 slots
        page.header_mut().unwrap().set_slot_count(3).unwrap();
        page.header_mut()
            .unwrap()
            .set_free_end((4096 - 1 - 3 * SLOT_SIZE) as u16)
            .unwrap();
        page.header_mut().unwrap().set_free_start(1000).unwrap();

        // But only the first and the third are used
        page.slot_array_mut().unwrap().set_slot(0, 96, 196).unwrap();
        page.slot_array_mut()
            .unwrap()
            .set_slot(2, 500, 500)
            .unwrap();

        let plan = page.plan_insert(400).unwrap();

        assert_eq!(plan.slot_number, 1);
        assert!(!plan.inserting_new_slot);
    }

    #[test]
    fn plan_insert_reuse_first_slot_and_place_between_two_rows() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();

        // we will have 3 rows but 4 slots:
        // slot array = 4 * 4 = 16 bytes => free_end = 4079
        // first row = 96 -> 196 (100 bytes) on slot index = 1
        // 2nd row = 200 -> 3900 (3700 bytes) on slot index = 2
        // 3rd row = 3999 -> 4050 (51 bytes) on slot index = 3
        // slot index 0 will be empty (reusable)
        // we will insert between rows 2 and 3 (at offset 3900) a 60-byte row
        page.header_mut()
            .unwrap()
            .set_free_space((PAGE_SIZE - 100 - 3700 - 51) as u16)
            .unwrap();
        page.header_mut().unwrap().set_free_start(4050).unwrap();
        page.header_mut().unwrap().set_free_end(4079).unwrap();
        page.header_mut().unwrap().set_slot_count(4).unwrap();
        page.slot_array_mut().unwrap().set_slot(1, 96, 100).unwrap();
        page.slot_array_mut()
            .unwrap()
            .set_slot(2, 200, 3700)
            .unwrap();
        page.slot_array_mut()
            .unwrap()
            .set_slot(3, 3999, 51)
            .unwrap();

        let plan = page.plan_insert(60).unwrap();

        // Reuse slot index 0
        assert_eq!(plan.slot_number, 0);
        assert_eq!(plan.start_offset, 3900);
        assert!(!plan.inserting_at_free_start);
        assert!(!plan.inserting_new_slot);
    }

    #[test]
    fn plan_insert_reuse_second_slot_and_place_between_two_rows() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();

        // we will have 3 rows but 4 slots:
        // slot array = 4 * 4 = 16 bytes => free_end = 4080
        // first row = 96 -> 196 (100 bytes) on slot index = 0
        // 2nd row = 200 -> 3900 (3700 bytes) on slot index = 2
        // 3rd row = 3999 -> 4050 (51 bytes) on slot index = 3
        // slot index 0 will be empty (reusable)
        // we will insert between rows 2 and 3 (at offset 3900) a 60-byte row
        page.header_mut()
            .unwrap()
            .set_free_space((PAGE_SIZE - 100 - 3700 - 51) as u16)
            .unwrap();
        page.header_mut().unwrap().set_free_start(4050).unwrap();
        page.header_mut().unwrap().set_free_end(4079).unwrap();
        page.header_mut().unwrap().set_slot_count(4).unwrap();
        page.slot_array_mut().unwrap().set_slot(0, 96, 100).unwrap();
        page.slot_array_mut()
            .unwrap()
            .set_slot(2, 200, 3700)
            .unwrap();
        page.slot_array_mut()
            .unwrap()
            .set_slot(3, 3999, 51)
            .unwrap();

        let plan = page.plan_insert(60).unwrap();

        // Reuse slot index 0
        assert_eq!(plan.slot_number, 1);
        assert_eq!(plan.start_offset, 3900);
        assert!(!plan.inserting_at_free_start);
        assert!(!plan.inserting_new_slot);
    }

    #[test]
    fn plan_insert_will_require_compaction() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();

        // We will insert a 200-bytes row, but there is not enough space between rows or between free start-end.
        page.header_mut()
            .unwrap()
            .set_free_space(210) // 210 bytes still available
            .unwrap();
        page.header_mut().unwrap().set_free_end(4087).unwrap();
        page.header_mut().unwrap().set_free_start(4050).unwrap();
        page.header_mut().unwrap().set_slot_count(2).unwrap();
        // first slot: 96 -> 1000
        // second slot: 1073 -> 4050
        page.slot_array_mut().unwrap().set_slot(0, 96, 904).unwrap();
        page.slot_array_mut()
            .unwrap()
            .set_slot(1, 1073, 2977)
            .unwrap();

        let plan = page.plan_insert(200).unwrap();

        assert_eq!(plan.slot_number, 2);
        assert!(plan.needs_compaction);
        assert!(plan.inserting_at_free_start);
        assert!(plan.inserting_new_slot);
        assert_eq!(plan.start_offset, 4050)
    }
}

#[cfg(test)]
mod insert_unsorted_tests {
    use super::*;
    use crate::impls::Page;
    use crate::page_id::PageId;
    use crate::page_type::PageType;

    #[test]
    fn insert_empty_page() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        let row = vec![5u8; 100];

        let plan = page.plan_insert(row.len()).unwrap();

        let insert_result = page.insert_heap(plan, row);

        assert!(insert_result.is_ok());
    }

    #[test]
    fn insert_fifth_row() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        page.insert_heap(page.plan_insert(100).unwrap(), vec![1u8; 100]);
        page.insert_heap(page.plan_insert(200).unwrap(), vec![2u8; 200]);
        page.insert_heap(page.plan_insert(300).unwrap(), vec![3u8; 300]);
        page.insert_heap(page.plan_insert(400).unwrap(), vec![4u8; 400]);

        let plan = page.plan_insert(500).unwrap();

        assert_eq!(plan.slot_number, 4);
        assert_eq!(plan.start_offset, 1096);
    }
}
