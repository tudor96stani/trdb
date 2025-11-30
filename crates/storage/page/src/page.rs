//! Module defining a fixed-size slotted page structure with its associated methods.
//!
//! # Memory Layout Overview
//!
//! A typical slotted page has this physical structure (generalized):
//!
//! ```text
//!   ┌───────────────────────────────────────────────────────────────┐
//!   │ Page Header (contains slot_count, free space ptrs, etc.)      │
//!   ├───────────────────────────────────────────────────────────────┤
//!   │ Tuple Data Region (grows upward)                              │
//!   │   records / row fragments                                     │
//!   │   variable sized                                              │
//!   │   aligned upwards                                             │
//!   ├───────────────────────────────────────────────────────────────┤
//!   │ Free Space                                                    │
//!   ├───────────────────────────────────────────────────────────────┤
//!   │ Slot Array Region (grows downward)                            │
//!   │   fixed-size SLOT_SIZE entries                                │
//!   │   indexed logically left-to-right,                            │
//!   │   stored physically right-to-left                             │
//!   └───────────────────────────────────────────────────────────────┘
//!
//!                     ↑ page_start                        page_end ↑
//! ```
//!
//! # Why This Design?
//!
//! - Adding a new slot does **not** require moving existing slots.
//! - Tuple movement and compaction only affect the data region.
//! - Both read and write operations are zero-copy and O(1).
//!
//! This module encapsulates that logic cleanly, exposing a safe and API for manipulating the slotted page.
//!
//!
//! Header access is provided via `header::HeaderRef` and `header::HeaderMut` types.
//! Slot array access is provided via `slot::SlotArrayRef` and `slot::SlotArrayMut` types.
use crate::errors::header_error::HeaderError;
use crate::errors::insert_error::InsertError;
use crate::errors::page_error::{PageError, PageResult, WithPageId};
use crate::errors::page_op_error::PageOpError;
use crate::errors::read_row_error::ReadRowError;
use crate::errors::slot_error::SlotError;
use crate::header::{HeaderMut, HeaderRef};
use crate::insertion_plan::InsertionPlan;
use crate::page_id::PageId;
use crate::page_type::PageType;
use crate::slot::SLOT_SIZE;
use crate::slot_array::{SlotArrayMut, SlotArrayRef};
use crate::{HEADER_SIZE, PAGE_SIZE, header};

/// Wrapper around a fixed-size byte array representing a page.
struct Page {
    page_id: PageId,
    data: Box<[u8; PAGE_SIZE]>,
}

/// Public APIs for the Page struct.
/// All public APIs use the `PageResult` type
impl Page {
    /// Retrieves a row from the page by its slot index.
    ///
    /// # Arguments
    ///
    /// * `slot_index` - The index of the slot to retrieve the row from. Indexing starts from 0.
    ///
    /// # Returns
    ///
    /// * `PageResult<&[u8]>` - A result containing a reference to the row data as a byte slice
    ///   if successful, or an error wrapped in `PageResult` if the operation fails.
    ///
    /// # Errors
    ///
    /// This method can return the following errors:
    /// * `PageError` - If there is an issue with the operation, such as an invalid slot index.
    ///
    /// The error is augmented with the `page_id` of the current page for better traceability.
    pub fn row(&self, slot_index: u32) -> PageResult<&[u8]> {
        self.read_row_internal(slot_index)
            .map_err(PageOpError::from)
            .with_page_id(self.page_id)
    }

    /// Plans the insertion of a row into the page.
    ///
    /// # Arguments
    ///
    /// * `row_len` - The length of the row to be inserted, in bytes.
    ///
    /// # Returns
    ///
    /// * `PageResult<InsertionPlan>` - A result containing the insertion plan if successful,
    ///   or an error wrapped in `PageResult` if the operation fails.
    ///
    /// # Errors
    ///
    /// This method can return the following errors:
    /// * `PageError` - If there is an issue with the operation, such as insufficient space
    ///   or other constraints preventing the insertion.
    ///
    /// The error is augmented with the `page_id` of the current page for better traceability.
    pub fn plan_insert(&self, row_len: usize) -> PageResult<InsertionPlan> {
        self.plan_insert_internal(row_len)
            .map_err(PageOpError::from)
            .with_page_id(self.page_id)
    }

    pub fn insert(&mut self, plan: InsertionPlan, row: Vec<u8>) -> PageResult<()> {
        self.insert_row_unsorted(plan, row)
            .map_err(PageOpError::from)
            .with_page_id(self.page_id)
    }
}

/// Methods for creating and initializing pages.
impl Page {
    /// Creates a new page with all bytes initialized to zero. Private constructor.
    fn new_zeroed(page_id: PageId) -> Self {
        Self {
            page_id,
            data: Box::new([0; PAGE_SIZE]),
        }
    }

    /// Creates a new page from an existing byte array.
    pub fn new_from_bytes(bytes: Box<[u8; 4096]>, page_id: PageId) -> Self {
        Self {
            data: bytes,
            page_id,
        }
    }

    /// Creates a new empty page with the specified page ID and page type.
    pub fn new_empty(page_id: PageId, page_type: PageType) -> Self {
        let mut page = Self::new_zeroed(page_id);

        page.header_mut().default(page_id.page_number, page_type);

        page
    }
}

/// Header access methods for the `Page` struct.
impl Page {
    /// Returns a read-only reference to the page header.
    fn header_ref(&'_ self) -> HeaderRef<'_> {
        HeaderRef::new(&self.data[..HEADER_SIZE]).unwrap() // todo remove this unwrap
    }

    /// Returns a mutable reference to the page header.
    fn header_mut(&'_ mut self) -> HeaderMut<'_> {
        HeaderMut::new(&mut self.data[..HEADER_SIZE]).unwrap() // todo remove this unwrap
    }
}

/// Accessor methods for the `Page` struct.
impl Page {
    /// Returns the unique identifier of the page.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }
}

/// Internal row access methods for the `Page` struct.
impl Page {
    /// Retrieves a row by its slot index.
    /// Returns a slice of bytes representing the row data.
    fn read_row_internal(&self, slot_index: u32) -> Result<&[u8], ReadRowError> {
        let slot = self.slot_array()?.slot_ref(slot_index)?;

        let (offset, length) = (slot.offset()? as usize, slot.length()? as usize);

        Ok(&self.data[offset..offset + length])
    }

    // TODO this method will require numerous tests
    fn plan_insert_internal(&self, row_len: usize) -> Result<InsertionPlan, InsertError> {
        let header = self.header_ref();

        if !self.row_size_fits(row_len)? {
            return Err(InsertError::NotEnoughSpace {
                row_len,
                page_free_space: header.get_free_space()? as usize,
            });
        }

        let mut unused_slot: Option<u32> = None;
        let mut available_space_start =
            if (header.get_free_end()? - header.get_free_start()?) as usize > row_len {
                Some(header.get_free_start()? as usize)
            } else {
                None
            };

        let mut inserting_at_free_start = available_space_start.is_some();

        for j in 0..header.get_slot_count()? {
            if unused_slot.is_some() && available_space_start.is_some() {
                break;
            }

            if unused_slot.is_none() {
                let current_slot = self.slot_array()?.slot_ref(j as u32)?;
                if current_slot.length()? == 0 && current_slot.offset()? == 0 {
                    unused_slot = Some(j as u32)
                }
            }

            if available_space_start.is_none() && j < header.get_slot_count()? - 1 {
                let current_slot = self.slot_array()?.slot_ref(j as u32)?;
                let next_slot = self.slot_array()?.slot_ref((j + 1) as u32)?;
                let gap = next_slot.offset()? - (current_slot.offset()? + current_slot.length()?);
                if gap >= row_len as u16 {
                    available_space_start =
                        Some((current_slot.offset()? + current_slot.length()?) as usize);
                }
            }
        }

        if unused_slot.is_none() {
            unused_slot = Some(header.get_slot_count()? as u32);
        }

        let needs_compaction = available_space_start.is_none()
            && header.get_can_compact()? != 0
            && header.needs_compaction(row_len)?;

        if needs_compaction {
            available_space_start = Some(header.get_free_start()? as usize);
            inserting_at_free_start = true;
        }

        if available_space_start.is_none() {
            return Err(InsertError::CannotFindSpace {
                required_space: row_len,
            });
        }

        let inserting_new_slot = unused_slot.unwrap() == header.get_slot_count()? as u32;

        Ok(InsertionPlan {
            slot_number: unused_slot.unwrap() as u16,
            start_offset: available_space_start.unwrap(),
            inserting_at_free_start,
            inserting_new_slot,
            needs_compaction,
        })
    }

    fn insert_row_unsorted(
        &mut self,
        plan: InsertionPlan,
        bytes: Vec<u8>,
    ) -> Result<(), InsertError> {
        let mut header_mut = self.header_mut();

        // Could not find available space during the insertion planning => compact now if needed
        if plan.needs_compaction {
            todo!("Compaction not yet implemented")
        }

        // If we are inserting at free start, use the *current* freeStart after potential compaction.
        // Otherwise, use the offset we planned in the gap.
        let start_offset = if plan.inserting_at_free_start {
            header_mut.get_free_start()? as usize
        } else {
            plan.start_offset
        };

        // Update the total slot count in the header - only increment it if we are generating a new slot
        let updated_total_slots =
            header_mut.get_slot_count()? + if plan.inserting_new_slot { 1 } else { 0 };
        header_mut.set_slot_count(updated_total_slots)?;

        // Only update free start if we are inserting there - if we are placing it in some free fragment, free start can stay the same
        if plan.inserting_at_free_start {
            let new_free_start = header_mut.get_free_start()? + bytes.len() as u16;
            header_mut.set_free_start(new_free_start)?
        }

        // Only update the free end if we are creating a new slot - if we are re-using one of the existing unused ones, the free end can stay the same
        if plan.inserting_new_slot {
            let new_free_end = header_mut.get_free_end()? - SLOT_SIZE as u16;
            header_mut.set_free_end(new_free_end)?
        }

        // freeSpace is always updated -> but depending on whether we are creating a new slot or reusing an existing one,
        // we need to subtract the size of a slot (for creating a new slot) or 0 (for reusing an existing slot)
        let new_free_space = header_mut.get_free_space()?
            - bytes.len() as u16
            - if plan.inserting_new_slot {
                SLOT_SIZE as u16
            } else {
                0
            };
        header_mut.set_free_space(new_free_space)?;

        // Insert the actual row in the page
        self.data[start_offset..(start_offset + bytes.len())].copy_from_slice(bytes.as_slice());

        // Right now, the header should contain the latest information regarding the number of slots (whether we added a new one or not),
        // so the slot array should include any new bytes segment. We place the new slot directly at the requested index
        self.slot_array_mut()?.set_slot(
            plan.slot_number as u32,
            start_offset as u16,
            bytes.len() as u16,
        );

        Ok(())
    }
}

/// Private methods for the `Page` struct.
impl Page {
    /// Returns an immutable view of the slot array.
    #[inline]
    fn slot_array(&'_ self) -> Result<SlotArrayRef<'_>, SlotError> {
        let free_end_offset = self.header_ref().get_free_end()? as usize;
        let slot_count = self.header_ref().get_slot_count()?;
        SlotArrayRef::new(&self.data[free_end_offset + 1..PAGE_SIZE], slot_count)
    }

    fn slot_array_mut(&'_ mut self) -> Result<SlotArrayMut<'_>, SlotError> {
        let free_end_offset = self.header_ref().get_free_end()? as usize;
        let slot_count = self.header_ref().get_slot_count()?;
        SlotArrayMut::new(&mut self.data[free_end_offset + 1..PAGE_SIZE], slot_count)
    }

    /// Determines whether the requested row size fits on the page.
    /// Does not account for fragmentation (i.e., row might fit only after a compaction of the page).
    /// Returns a boolean or error if something goes wrong while processing the header.
    #[inline]
    fn row_size_fits(&self, row_size: usize) -> Result<bool, HeaderError> {
        Ok(self.header_ref().get_free_space()? >= (row_size + SLOT_SIZE) as u16)
    }
}

#[cfg(test)]
mod new_and_accessors_tests {
    use super::*;
    use crate::page_type::PageType;

    #[test]
    fn test_new_empty_page() {
        let page_id = PageId::new(1, 0);
        let page = Page::new_empty(page_id, PageType::Unsorted);

        assert_eq!(page.page_id(), page_id);

        let header = page.header_ref();
        assert_eq!(header.get_page_number().unwrap(), 0);
        assert_eq!(
            header.get_page_type().unwrap(),
            u16::from(PageType::Unsorted)
        );
    }

    #[test]
    fn test_new_from_bytes() {
        let page_id = PageId::new(1, 1);
        let bytes = Box::new([5u8; PAGE_SIZE]);
        let page = Page::new_from_bytes(bytes, page_id);

        assert_eq!(page.page_id(), page_id);
        assert_eq!(page.data[..], [5u8; PAGE_SIZE][..]);
    }

    #[test]
    fn test_get_page_id() {
        let page_id = PageId::new(2, 5);
        let page = Page::new_empty(page_id, PageType::IndexLeaf);

        assert_eq!(page.page_id(), page_id);
    }
}

#[cfg(test)]
mod read_row_tests {
    use super::*;
    use crate::slot::SlotMut;

    #[test]
    fn read_row_out_of_bounds() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
        page.header_mut().set_slot_count(2);
        page.header_mut().set_free_end(4088);

        let result_internal = page.read_row_internal(3);
        assert!(matches!(
            result_internal,
            Err(ReadRowError::SlotError(SlotError::SlotRegionSizeMismatch {
                expected_size: 8,
                actual_size: 7
            }))
        ));

        let result = page.row(3);
        assert!(matches!(
            result,
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
        page.header_mut().set_free_end(4091);
        page.header_mut().set_slot_count(1);

        // Get the row via the slot number
        let row_internal = page.read_row_internal(0).unwrap();
        let row = page.row(0).unwrap();

        // Should be the same.
        assert_eq!([5u8; 10], *row_internal);
        assert_eq!([5u8; 10], *row);
    }
}

#[cfg(test)]
mod insert_tests {
    use super::*;

    #[test]
    fn plan_insert_empty_page() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
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

        let insert_result = page.insert(plan, row);

        assert!(insert_result.is_ok());
    }

    #[test]
    fn insert_fifth_row() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
        page.insert(page.plan_insert(100).unwrap(), vec![1u8; 100]);
        page.insert(page.plan_insert(200).unwrap(), vec![2u8; 200]);
        page.insert(page.plan_insert(300).unwrap(), vec![3u8; 300]);
        page.insert(page.plan_insert(400).unwrap(), vec![4u8; 400]);

        let plan = page.plan_insert(500).unwrap();

        assert_eq!(plan.slot_number, 4);
        assert_eq!(plan.start_offset, 1096);
    }
}

#[cfg(test)]
mod private_methods_tests {
    use super::*;

    // region Row fits
    #[test]
    fn row_fits_enough_space() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
        assert!(page.row_size_fits(100).unwrap());
    }

    #[test]
    fn row_fits_at_limit() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
        assert!(page.row_size_fits(3996).unwrap());
    }

    #[test]
    fn row_fits_slot_would_not_fit() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
        assert!(!page.row_size_fits(3998).unwrap());
    }

    #[test]
    fn row_fits_would_not_fit_at_all() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
        assert!(!page.row_size_fits(4000).unwrap());
    }
    // endregion

    // region Slot array
    #[test]
    fn slot_array_corrupted_header_returns_error() {
        let mut page = Page::new_empty(PageId::new(1, 0), PageType::Unsorted);
        page.header_mut().set_free_end(4090);
        page.header_mut().set_slot_count(10);

        let result = page.slot_array();
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
