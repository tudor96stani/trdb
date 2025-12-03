#[cfg(test)]
mod read_row_tests {
    use super::*;
    use crate::PAGE_SIZE;
    use crate::errors::page_error::PageError;
    use crate::errors::page_op_error::PageOpError;
    use crate::errors::read_row_error::ReadRowError;
    use crate::errors::slot_error::SlotError;
    use crate::impls::Page;
    use crate::page_id::PageId;
    use crate::page_type::PageType;
    use crate::slot::SlotMut;

    #[test]
    fn read_row_out_of_bounds() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        page.header_mut().unwrap().set_slot_count(2);
        page.header_mut().unwrap().set_free_end(4088);

        let result_internal = page.row(3);
        assert!(matches!(
            result_internal,
            Err(PageError {
                page_id: PageId {
                    file_id: 1,
                    page_number: 0
                },
                source: PageOpError::ReadRow(ReadRowError::SlotError(
                    SlotError::SlotRegionSizeMismatch {
                        expected_size: 8,
                        actual_size: 7
                    }
                ))
            })
        ));

        let result = page.row(3);
        // Rust
        assert!(matches!(
            result,
            Err(e) if e.page_id == PageId::new(1, 0)
                && matches!(
                    e.source,
                    PageOpError::ReadRow(ReadRowError::SlotError(
                        SlotError::SlotRegionSizeMismatch {
                            expected_size: 8,
                            actual_size: 7
                        }
                    ))
                )
        ));
    }

    #[test]
    fn read_row_valid_slot_index() {
        let mut page_bytes = Box::new([0u8; PAGE_SIZE]);

        // Place a fake 10-byte row at offset 96 (the first row)
        page_bytes[96..106].copy_from_slice([5u8; 10].as_ref());

        // register a slot for this row in the slot array
        let mut slot = SlotMut::from_raw(0, &mut page_bytes[PAGE_SIZE - 4..PAGE_SIZE]).unwrap();
        slot.set_offset(96);
        slot.set_length(10);

        let mut page = Page::new_from_bytes(page_bytes, PageId::new(1, 0));
        page.header_mut().unwrap().set_free_end(4091);
        page.header_mut().unwrap().set_slot_count(1);

        // Get the row via the slot number
        let row_internal = page.row(0).unwrap();
        let row = page.row(0).unwrap();

        // Should be the same.
        assert_eq!([5u8; 10], *row_internal);
        assert_eq!([5u8; 10], *row);
    }
}
