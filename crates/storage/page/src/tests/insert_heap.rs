#[cfg(test)]
mod plan_insert_tests {
    use super::*;
    use crate::impls::Page;
    use crate::page_id::PageId;
    use crate::page_type::PageType;

    #[test]
    fn plan_insert_empty_page() {
        let page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
        let row_size = 100usize;

        let plan = page.plan_insert(row_size).unwrap();

        assert_eq!(plan.slot_number, 0);
        assert_eq!(plan.start_offset, 96);
        assert!(plan.inserting_new_slot);
        assert!(plan.inserting_at_free_start);
        assert!(!plan.needs_compaction);
    }

    #[test]
    fn insert_empty_page() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
        let row = vec![5u8; 100];

        let plan = page.plan_insert(row.len()).unwrap();

        let insert_result = page.insert_heap(plan, row);

        assert!(insert_result.is_ok());
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
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
        let row = vec![5u8; 100];

        let plan = page.plan_insert(row.len()).unwrap();

        let insert_result = page.insert_heap(plan, row);

        assert!(insert_result.is_ok());
    }

    #[test]
    fn insert_fifth_row() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
        page.insert_heap(page.plan_insert(100).unwrap(), vec![1u8; 100]);
        page.insert_heap(page.plan_insert(200).unwrap(), vec![2u8; 200]);
        page.insert_heap(page.plan_insert(300).unwrap(), vec![3u8; 300]);
        page.insert_heap(page.plan_insert(400).unwrap(), vec![4u8; 400]);

        let plan = page.plan_insert(500).unwrap();

        assert_eq!(plan.slot_number, 4);
        assert_eq!(plan.start_offset, 1096);
    }
}
