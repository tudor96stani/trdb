#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::delete_error::DeleteError;
    use crate::errors::page_error::PageError;
    use crate::errors::page_op_error::PageOpError;
    use crate::errors::slot_error::SlotError;
    use crate::impls::Page;
    use crate::tests::SlotValues;
    use crate::tests::tests_error_helpers;

    #[test]
    fn delete_row_without_compaction() {
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

        page.delete_row(0, false).unwrap();

        page.assert_row_values(96, 100, 1);
        page.assert_row_values(196, 100, 2);
        page.assert_slot(0, 0, 0);
        page.assert_slot(1, 196, 100);
    }

    #[test]
    fn delete_row_with_compaction() {
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

        page.delete_row(0, true).unwrap();

        page.assert_row_values(96, 50, 2);
        page.assert_slot(0, 0, 0);
        page.assert_slot(1, 96, 50);
    }

    #[test]
    fn delete_row_invalid_slot_returns_error() {
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

        let result = page.delete_row(2, false).unwrap_err();

        let slot_error = result.source.expect_delete_error().expect_slot_error();

        let SlotError::InvalidSlot { slot_index } = slot_error else {
            panic!("expected InvalidSlot, got {slot_error:?}")
        };
        assert_eq!(*slot_index, 2);
    }
}
