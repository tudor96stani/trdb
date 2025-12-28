#[cfg(test)]
mod plan_insert_test {
    use super::*;
    use crate::errors::insert_error::InsertError;
    use crate::errors::page_op_error::PageOpError;
    use crate::impls::Page;
    use crate::insertion_plan::{InsertionOffset, InsertionSlot};
    use crate::page_id::PageId;
    use crate::page_type::PageType;
    use crate::slot::SLOT_SIZE;
    use crate::{HEADER_SIZE, PAGE_SIZE};

    #[test]
    fn plan_insert_new_slot_fast_path() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        // ensure no slots and a large free region
        page.header_mut().unwrap().set_free_start(100).unwrap();
        page.header_mut()
            .unwrap()
            .set_free_end((PAGE_SIZE - 1) as u16)
            .unwrap();
        // free_space should be consistent with header implementation; set a large free_space
        page.header_mut()
            .unwrap()
            .set_free_space((PAGE_SIZE - HEADER_SIZE) as u16)
            .unwrap();

        let plan = page.plan_insert(50).unwrap();
        // Expect a new slot and Exact at free_start
        assert!(matches!(plan.slot, InsertionSlot::New));
        assert!(matches!(plan.offset, InsertionOffset::Exact(100)));
    }

    #[test]
    fn plan_insert_reuse_slot() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        // prepare one slot that's invalid (zeros)
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
        page.header_mut()
            .unwrap()
            .set_free_start(new_free_end - 100)
            .unwrap();
        page.header_mut().unwrap().set_free_space(200).unwrap();

        // leave slot 0 as zeros (invalid) so it should be reused
        let plan = page.plan_insert(10).unwrap();
        assert!(matches!(plan.slot, InsertionSlot::Reuse(0)));
    }

    #[test]
    fn plan_insert_not_enough_space_error() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        // make free_space small so insertion fails; keep free_end consistent with slot_count==0
        page.header_mut().unwrap().set_free_start(100).unwrap();
        page.header_mut()
            .unwrap()
            .set_free_end((PAGE_SIZE - 1) as u16)
            .unwrap();
        page.header_mut().unwrap().set_free_space(10).unwrap();

        let res = page.plan_insert(50);

        assert!(matches!(res,
            Err(e) if e.page_id == PageId::new(1, 0)
                && matches!(
                    e.source,
                    PageOpError::Insert(InsertError::NotEnoughSpace {
                    row_len,
                    page_free_space,
                }) if row_len == 50 && page_free_space == 10)
        ))
    }
}
