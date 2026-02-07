#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::delete_error::DeleteError;
    use crate::errors::page_error::PageError;
    use crate::errors::page_op_error::PageOpError;
    use crate::errors::slot_error::SlotError;
    use crate::errors::update_error::UpdateError;
    use crate::page::api::Page;
    use crate::tests::SlotValues;
    use crate::tests::tests_error_helpers;

    #[test]
    fn update_row_invalid_slot() {
        let mut page = Page::test_create_empty_heap();

        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 100,
            },
            SlotValues {
                offset: 196,
                len: 100,
            },
        ]);

        let new_data = vec![2; 100];

        let result = page.update_row(2, new_data).unwrap_err();

        let slot_error = result.source.expect_update_error().expect_slot_error();

        let SlotError::InvalidSlot { slot_index } = slot_error else {
            panic!("expected InvalidSlot, got {slot_error:?}")
        };
        assert_eq!(*slot_index, 2);
    }

    #[test]
    fn update_only_row_on_page_same_size_overwrites() {
        let mut page = Page::test_create_empty_heap();

        page.test_insert_rows(vec![SlotValues {
            offset: 96,
            len: 100,
        }]);

        let result = page.update_row(0, vec![2; 100]);

        page.assert_slot(0, 96, 100);
        page.assert_row_values(96, 100, 2);
    }

    #[test]
    fn update_only_row_on_page_smaller_size_overwrites() {
        let mut page = Page::test_create_empty_heap();

        page.test_insert_rows(vec![SlotValues {
            offset: 96,
            len: 100,
        }]);

        let result = page.update_row(0, vec![2; 50]);

        page.assert_slot(0, 96, 50);
        page.assert_row_values(96, 50, 2);
        // the last 50 bytes of the old row should still be there
        page.assert_row_values(146, 50, 1);
    }

    #[test]
    fn update_only_row_on_page_larger_size_added_after() {
        let mut page = Page::test_create_empty_heap();

        page.test_insert_rows(vec![SlotValues {
            offset: 96,
            len: 100,
        }]);

        let result = page.update_row(0, vec![2; 150]);

        // Maybe counter intuitive in the beginning, but if there is enough room in the free_region, the original row value will not be overwritten.
        page.assert_slot(0, 196, 150);
        page.assert_row_values(196, 150, 2);
        page.assert_row_values(96, 100, 1);
    }

    #[test]
    fn update_row_when_multiple_on_page_same_size_overwrites() {
        let mut page = Page::test_create_empty_heap();

        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 100,
            },
            SlotValues {
                offset: 196,
                len: 100,
            },
            SlotValues {
                offset: 296,
                len: 100,
            },
        ]);

        let result = page.update_row(0, vec![4; 100]);

        page.assert_slot(0, 96, 100);
        page.assert_row_values(96, 100, 4);
    }

    #[test]
    fn update_row_when_multiple_on_page_smaller_size_overwrites() {
        let mut page = Page::test_create_empty_heap();

        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 100,
            },
            SlotValues {
                offset: 196,
                len: 100,
            },
            SlotValues {
                offset: 296,
                len: 100,
            },
        ]);

        let result = page.update_row(0, vec![4; 50]);

        page.assert_slot(0, 96, 50);
        page.assert_row_values(96, 50, 4);
        // the last 50 bytes of the old row should still be there
        page.assert_row_values(146, 50, 1);
    }

    #[test]
    fn update_row_when_multiple_on_page_larger_size_appends_in_free_space() {
        let mut page = Page::test_create_empty_heap();

        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 100,
            },
            SlotValues {
                offset: 196,
                len: 100,
            },
            SlotValues {
                offset: 296,
                len: 100,
            },
        ]);

        let result = page.update_row(0, vec![4; 150]);

        // Maybe counter intuitive in the beginning, but if there is enough room in the free_region, the original row value will not be overwritten.
        page.assert_slot(0, 396, 150);
        page.assert_row_values(396, 150, 4);
        page.assert_row_values(96, 100, 1);
    }

    #[test]
    fn update_row_new_value_no_longer_fits() {
        let mut page = Page::test_create_empty_heap();

        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 2004,
            },
            SlotValues {
                offset: 2100,
                len: 1980,
            },
        ]);

        let result = page.update_row(0, vec![3; 2100]).unwrap_err();

        let update_error = result.source.expect_update_error();

        let UpdateError::NotEnoughSpace {
            row_len,
            page_free_space,
        } = update_error
        else {
            panic!("expected NotEnoughSpace, got {update_error:?}")
        };
        assert_eq!(*row_len, 2100);
        assert_eq!(*page_free_space, 2012);
    }

    #[test]
    fn update_row_new_larger_value_fits_in_place_of_old_plus_gap() {
        let mut page = Page::test_create_empty_heap();

        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 1904,
            },
            // an 100 bytes gap
            SlotValues {
                offset: 2100,
                len: 1980,
            },
        ]);

        let result = page.update_row(0, vec![3; 2004]);

        page.assert_slot(0, 96, 2004);
        page.assert_row_values(96, 2004, 3);
    }

    #[test]
    fn update_row_new_larger_value_fits_between_other_two_rows() {
        let mut page = Page::test_create_empty_heap();

        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 104, // 96->200
            },
            SlotValues {
                offset: 200,
                len: 100, // 200 -> 300
            },
            // 200 bytes gap
            SlotValues {
                offset: 500,
                len: 1600,
            },
            SlotValues {
                offset: 2100,
                len: 1980,
            },
        ]);

        let result = page.update_row(0, vec![5; 200]);

        page.assert_slot(0, 300, 200);
        page.assert_row_values(300, 200, 5);
        page.assert_row_values(96, 104, 1);
    }

    #[test]
    fn update_row_larger_requires_compaction() {
        let mut page = Page::test_create_empty_heap();

        page.test_insert_rows(vec![
            SlotValues {
                offset: 96,
                len: 1904, // 96->2000
            },
            SlotValues {
                offset: 2100,
                len: 1980, // 2100 -> 4080 => 7 bytes remaining from here + 100 from the gap
            },
        ]);

        // to force a compaction: old_row_len < new_row_len < old_row_len + free_space
        // meaning 1904 < new_row_len < 1904 + 107 = 2011
        // let's take it 2005
        let result = page.update_row(0, vec![3; 2005]);

        // old row is deleted
        // row 2 (2100-4090) moves to index 96 so it will be 96 - 2076
        // so updated row will be 2076 - 4081
        page.assert_slot(0, 2076, 2005);
        page.assert_row_values(2076, 2005, 3);
        page.assert_row_values(96, 1980, 2);
    }
}
