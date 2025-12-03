use super::Page;
use crate::PAGE_SIZE;
use crate::errors::header_error::HeaderError;
use crate::errors::slot_error::SlotError;
use crate::slot::SLOT_SIZE;
use crate::slot_array::{SlotArrayMut, SlotArrayRef};

/// Private methods for the `Page` struct.
impl Page {
    /// Returns an immutable view of the slot array.
    #[inline]
    pub(crate) fn slot_array_ref(&'_ self) -> Result<SlotArrayRef<'_>, SlotError> {
        let free_end_offset = self.header_ref()?.get_free_end()? as usize;
        let slot_count = self.header_ref()?.get_slot_count()?;
        SlotArrayRef::new(&self.data[free_end_offset + 1..PAGE_SIZE], slot_count)
    }

    pub(crate) fn slot_array_mut(&'_ mut self) -> Result<SlotArrayMut<'_>, SlotError> {
        let free_end_offset = self.header_ref()?.get_free_end()? as usize;
        let slot_count = self.header_ref()?.get_slot_count()?;
        SlotArrayMut::new(&mut self.data[free_end_offset + 1..PAGE_SIZE], slot_count)
    }

    /// Determines whether the requested row size fits on the page.
    /// Does not account for fragmentation (i.e., row might fit only after a compaction of the page).
    /// Returns a boolean or error if something goes wrong while processing the header.
    #[inline]
    pub(super) fn row_size_fits(&self, row_size: usize) -> Result<bool, HeaderError> {
        Ok(self.header_ref()?.get_free_space()? >= (row_size + SLOT_SIZE) as u16)
    }
}

#[cfg(test)]
mod private_methods_tests {
    use super::*;
    use crate::page_id::PageId;
    use crate::page_type::PageType;

    // region Row fits
    #[test]
    fn row_fits_enough_space() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        assert!(page.row_size_fits(100).unwrap());
    }

    #[test]
    fn row_fits_at_limit() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        assert!(page.row_size_fits(3996).unwrap());
    }

    #[test]
    fn row_fits_slot_would_not_fit() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        assert!(!page.row_size_fits(3998).unwrap());
    }

    #[test]
    fn row_fits_would_not_fit_at_all() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        assert!(!page.row_size_fits(4000).unwrap());
    }
    // endregion

    // region Slot array
    #[test]
    fn slot_array_corrupted_header_returns_error() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted).unwrap();
        page.header_mut().unwrap().set_free_end(4090);
        page.header_mut().unwrap().set_slot_count(10);

        let result = page.slot_array_ref();
        assert!(matches!(
            result,
            Err(SlotError::SlotRegionSizeMismatch {
                expected_size: 40,
                actual_size: 5
            })
        ))
    }
    // endregion
}
