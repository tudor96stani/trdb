use crate::PAGE_SIZE;
use crate::impls::Page;
use crate::page_id::PageId;
use crate::page_type::PageType;
use crate::slot::SLOT_SIZE;

mod delete_row_tests;
mod insert_heap_tests;
mod plan_insert_tests;
mod read_row_tests;

/// This section defines helper methods for the test suite. They are defined as methods on the `Page` struct, but are only available in the test environment.
#[cfg(test)]
impl Page {
    /// Returns a new empty heap page for testing purposes
    pub(super) fn test_create_empty_heap() -> Page {
        Page::new_empty(PageId::new(1, 1), PageType::Unsorted).unwrap()
    }

    /// Sets the provided slot count in the header and updates the free_end field accordingly.
    pub(super) fn test_set_slot_count(&mut self, count: usize) {
        // 4096 - count*slot_size - 1
        let mut header_mut = self.header_mut().unwrap();
        header_mut.set_slot_count(count as u16).unwrap();

        let new_free_end = PAGE_SIZE - count * SLOT_SIZE - 1;
        header_mut.set_free_end(new_free_end as u16);
    }

    /// Populates the slot array with the provided slots. Sets up the header with the correct slot count as well.
    pub(super) fn test_create_slots(&mut self, slots: Vec<SlotValues>) {
        self.test_set_slot_count(slots.len());
        let mut slot_array_mut = self.slot_array_mut().unwrap();
        for (index, slot) in slots.iter().enumerate() {
            slot_array_mut
                .slot_mut(index as u32)
                .unwrap()
                .set_offset(slot.offset as u16)
                .unwrap();
            slot_array_mut
                .slot_mut(index as u32)
                .unwrap()
                .set_length(slot.len as u16)
                .unwrap();
        }
    }

    /// Inserts a set of rows in the page, as instructed by the slot array provided. Each row will have its `index_in_slot_array + 1` as binary value repeated for the `len` of the slot.
    /// ### Example
    /// ```rust
    /// let slots = vec![
    //         SlotValues { offset: 100, len: 10 },
    //         SlotValues { offset: 110, len: 20 },
    //     ];
    /// ```
    /// will provide the following arrays:
    /// ```rust
    /// [1u8; 10] // for the 1st slot
    /// [2u8; 20] // for the 2nd slot
    /// ```
    pub(super) fn test_insert_rows(&mut self, slots: Vec<SlotValues>) {
        self.test_set_slot_count(slots.len());
        for (index, slot) in slots.iter().enumerate() {
            let mut slot_array_mut = self.slot_array_mut().unwrap();
            slot_array_mut
                .slot_mut(index as u32)
                .unwrap()
                .set_offset(slot.offset as u16)
                .unwrap();
            slot_array_mut
                .slot_mut(index as u32)
                .unwrap()
                .set_length(slot.len as u16)
                .unwrap();
            let value = (index + 1) as u8;
            let len = slot.len;
            self.data_mut()[slot.offset..slot.offset + slot.len]
                .copy_from_slice(vec![value; len].as_slice())
        }
    }

    /// Asserts that at the provided `offset`, the `value` byte is repeated for `length` - basically that the row data is what is expected, circumventing going through the slot array.
    pub(super) fn assert_row(&mut self, offset: usize, length: usize, value: u8) {
        let mut actual_row_data = &mut self.data_mut()[offset..offset + length];

        assert_eq!(vec![value; length].as_slice(), actual_row_data);
    }

    pub(super) fn assert_slot(&mut self, slot_index: usize, offset: usize, length: usize) {
        let slot = self
            .slot_array_ref()
            .unwrap()
            .slot_ref(slot_index as u32)
            .unwrap();
        assert_eq!(slot.offset().unwrap(), offset as u16);
        assert_eq!(slot.length().unwrap(), length as u16);
    }
}

/// This helper struct is used to pass around slot values so that they can be set in the array.
#[cfg(test)]
pub(super) struct SlotValues {
    pub(super) offset: usize,
    pub(super) len: usize,
}

/// This part generates helper methods on the error types, to more easily break them apart during assertions and get to the inner most error.
#[cfg(test)]
mod tests_error_helpers {
    use crate::errors::delete_error::DeleteError;
    use crate::errors::header_error::HeaderError;
    use crate::errors::insert_error::InsertError;
    use crate::errors::page_op_error::PageOpError;
    use crate::errors::read_row_error::ReadRowError;
    use crate::errors::slot_error::SlotError;
    use binary_helpers::bin_error::BinaryError;

    // Macro to generate expect_* helpers
    macro_rules! impl_expect_ref {
        ($enum:ty, $fn_name:ident, $variant:ident => $inner:ty) => {
            impl $enum {
                #[track_caller]
                pub fn $fn_name(&self) -> &$inner {
                    match self {
                        Self::$variant(inner) => inner,
                        other => panic!(
                            "expected {}::{}, got {other:?}",
                            stringify!($enum),
                            stringify!($variant),
                        ),
                    }
                }
            }
        };
    }

    impl_expect_ref!(PageOpError, expect_header_error, Header => HeaderError);
    impl_expect_ref!(PageOpError, expect_slot_error, Slot => SlotError);
    impl_expect_ref!(PageOpError, expect_read_row_error, ReadRow => ReadRowError);
    impl_expect_ref!(PageOpError, expect_insert_error, Insert => InsertError);
    impl_expect_ref!(PageOpError, expect_delete_error, DeleteRow => DeleteError);

    impl_expect_ref!(HeaderError, expect_binary_error, BinaryError => BinaryError);

    impl_expect_ref!(SlotError, expect_binary_error, BinaryError => BinaryError);
    impl_expect_ref!(SlotError, expect_header_error, HeaderError => HeaderError);

    impl_expect_ref!(ReadRowError, expect_slot_error, SlotError => SlotError);

    impl_expect_ref!(InsertError, expect_slot_error, SlotError => SlotError);
    impl_expect_ref!(InsertError, expect_header_error, SlotError => SlotError);

    impl_expect_ref!(DeleteError, expect_slot_error, SlotError => SlotError);
    impl_expect_ref!(DeleteError, expect_header_error, HeaderError => HeaderError);
}
