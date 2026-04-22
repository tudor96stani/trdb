#[cfg(test)]
mod read_row_test {
    use crate::page::PAGE_SIZE;
    use crate::page::errors::{PageError, PageOpError, ReadRowError, SlotError};
    use crate::page::layout::page::Page;
    use crate::page::layout::slot::SlotMut;
    use crate::page::metadata::page_id::PageId;
    use crate::page::metadata::page_type::PageType;

    #[test]
    fn read_row_out_of_bounds() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        page.header_mut().unwrap().set_slot_count(2).unwrap();
        page.header_mut().unwrap().set_free_end(4088).unwrap();

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
        slot.set_offset(96).unwrap();
        slot.set_length(10).unwrap();

        let mut page = Page::new_from_bytes(page_bytes, PageId::new(1, 0));
        page.header_mut().unwrap().set_free_end(4091).unwrap();
        page.header_mut().unwrap().set_slot_count(1).unwrap();

        // Get the row via the slot number
        let row_internal = page.row(0).unwrap();
        let row = page.row(0).unwrap();

        // Should be the same.
        assert_eq!([5u8; 10], *row_internal);
        assert_eq!([5u8; 10], *row);
    }
}
