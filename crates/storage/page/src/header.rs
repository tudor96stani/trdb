//! Page Header Definition and Accessors
//! ------------------------------------
//!
//! This module defines the binary layout, invariants, and safe accessors for
//! the **page header** of a slotted page. The header occupies the first
//! **96 bytes** of every page and stores all metadata required to manage
//! tuple storage, slot array allocation, and logical linkage between pages.
//!
//! All fields are stored in **little-endian** format and accessed via
//! zero-copy typed wrappers (`HeaderRef` and `HeaderMut`).
//!
//! # Binary Layout
//!
//! The header has the following structure:
//!
//! | Field          | Type  | Offset                  | Description |
//! |----------------|-------|--------------------------|-------------|
//! | `slot_count`   | u16   | [`SLOT_COUNT`]           | Number of allocated slots (active or unused). |
//! | `free_start`   | u16   | [`FREE_START`]           | Offset of the first free byte in the tuple/data region (grows upward). |
//! | `free_end`     | u16   | [`FREE_END`]             | Offset of the last free byte *before* the slot array region (grows downward). |
//! | `free_space`   | u16   | [`FREE_SPACE`]           | Total free space in bytes, including gaps and fragmented space. |
//! | `can_compact`  | u16   | [`CAN_COMPACT`]          | Whether fragmentation requires compaction (0 or 1). |
//! | `page_number`  | u32   | [`PAGE_NUMBER`]          | Logical page identifier within its file. |
//! | `page_type`    | u16   | [`PAGE_TYPE`]            | Page classification (data page, internal index page, etc.). |
//! | `left_page`    | u32   | [`LEFT_PAGE`]            | Pointer to the left sibling page (if applicable). |
//! | `right_page`   | u32   | [`RIGHT_PAGE`]           | Pointer to the right sibling page. |
//! | `last_lsn`     | u64   | [`LAST_LSN`]             | Last log sequence number applied to this page. |
//!
//! ## Notes on Format Compatibility
//!
//! - The layout mirrors the existing Java implementation for binary compatibility.
//! - The Java version included `parent_page` at offset 22; this Rust version
//!   **intentionally omits** it.
//! - `last_lsn` begins at offset 26 to maintain compatibility with the previous format.
//!
//! # Header Invariants
//!
//! The header defines three regions inside the page body:
//!
//! ```text
//! ┌───────────────┬───────────────────────────┬───────────────────────────┐
//! │   Header       │       Tuple/Data Region   │      Slot Array Region    │
//! │   (96 bytes)   │   (grows upward ↑)        │   (grows downward ↓)      │
//! └───────────────┴───────────────────────────┴───────────────────────────┘
//! ^ offset 0                         free_start          free_end         PAGE_SIZE
//! ```
//!
//! The following invariants always hold (assuming `PAGE_SIZE = N`):
//!
//! ### Free Space and Allocation Pointers
//!
//! - **`free_start`**
//!   Points to the first free byte in the tuple/data region.
//!   This region grows **upwards**, i.e., toward higher offsets.
//!
//! - **`free_end`**
//!   Points to the **last free byte** immediately *before* the slot array region.
//!   This value decreases as new slot headers are allocated.
//!
//! - **Slot array occupies** the byte range:<br>
//!   `data[(free_end + 1) .. PAGE_SIZE)`
//!
//! ### Slot Allocation Rules
//!
//! - When the page contains **no slots**,
//!   `free_end = PAGE_SIZE - 1`, therefore the slot array is empty.
//!
//! - Allocating new slot decrements `free_end` by `SLOT_SIZE`:
//!
//!   ```text
//!   free_end = free_end - SLOT_SIZE
//!   ```
//!
//!   This causes the slot array to grow **downward** from the end of the page
//!   (right-to-left in memory), avoiding the need to shift existing slot entries.
//!
//! ### Free Space Accounting
//!
//! - `free_space` tracks the total free bytes on the page, not necessarily
//!   the largest contiguous block. It includes fragmented space.
//!
//! - `can_compact = 1` signals that the page contains fragmentation and may
//!   need compaction to restore a contiguous free region.
//!
//! Together, these invariants define the core state machine for all page
//! modifications, insertion planning, compaction logic, and slot array access.
//!

use crate::errors::header_error::HeaderError;
use crate::page_type::PageType;
use crate::slot::SLOT_SIZE;
use crate::{HEADER_SIZE, PAGE_SIZE, impls};
use binary_helpers::le::{read_le, write_le};
use paste::paste;

/// The 'HeaderRef' struct provides an immutable view into the header of a page.
#[derive(Debug)]
pub(crate) struct HeaderRef<'a> {
    bytes: &'a [u8; HEADER_SIZE],
}

impl<'a> HeaderRef<'a> {
    /// Creates a new `HeaderRef` from a slice of bytes if it matches the required size.
    ///
    /// # Parameters
    /// - `bytes`: A reference to a slice of bytes that will be used to initialize the `HeaderRef`.
    ///
    /// # Returns
    /// - `Some(HeaderRef)` if the length of the `bytes` slice is equal to `HEADER_SIZE`.
    /// - `None` if the length of the `bytes` slice does not match `HEADER_SIZE`.
    pub fn new(bytes: &'a [u8]) -> Result<Self, HeaderError> {
        if bytes.len() != HEADER_SIZE {
            return Err(HeaderError::HeaderSliceSizeMismatch {
                actual: bytes.len(),
            });
        }

        Ok(HeaderRef {
            bytes: bytes.try_into().unwrap(),
        })
    }

    /// Returns a boolean indicating whether the page needs compaction.
    /// Mostly a helper method, since it only uses the values from the header to determine this.
    /// Returns `Some(bool)` if successful
    /// or `HeaderError` if something goes wrong.
    pub fn needs_compaction(&self, row_size: usize) -> Result<bool, HeaderError> {
        let diff = self
            .get_free_end()?
            .checked_sub(self.get_free_start()?)
            .ok_or(HeaderError::OffsetArithmetic)?;
        Ok(diff < (row_size + SLOT_SIZE) as u16)
    }
}

/// The `HeaderMut` struct provides a mutable view into the header of a page.
#[derive(Debug)]
pub(crate) struct HeaderMut<'a> {
    bytes: &'a mut [u8; HEADER_SIZE],
}

impl<'a> HeaderMut<'a> {
    /// Creates a new `HeaderMut` instance if the provided byte slice's length matches the expected `HEADER_SIZE`.
    ///
    /// # Parameters
    /// - `bytes`: A mutable reference to a byte slice (`[u8]`) that will be used to construct a `HeaderMut`.
    ///
    /// # Returns
    /// - `Some(Self)`: A`HeaderMut` if the length of the provided byte slice matches `HEADER_SIZE`
    /// - `None`: If the length of the provided byte slice does not match `HEADER_SIZE`.
    pub(crate) fn new(bytes: &'a mut [u8]) -> Result<Self, HeaderError> {
        if bytes.len() != HEADER_SIZE {
            return Err(HeaderError::HeaderSliceSizeMismatch {
                actual: bytes.len(),
            });
        }

        Ok(HeaderMut {
            bytes: bytes.try_into().unwrap(),
        })
    }

    /// Initializes the header with default values for a new empty page.
    pub(crate) fn default(
        &mut self,
        page_number: u32,
        page_type: PageType,
    ) -> Result<(), HeaderError> {
        self.set_page_number(page_number)?;
        self.set_free_start(HEADER_SIZE as u16)?;
        self.set_free_end((PAGE_SIZE - 1) as u16)?;
        self.set_free_space((PAGE_SIZE - HEADER_SIZE) as u16)?;
        self.set_page_type(u16::from(page_type))?;
        Ok(())
    }
}

/// Defines header field constants and getter/setter methods.
///
/// Pattern: `field_id(identifier): field_type(type) = field_offset(usize)`
macro_rules! impl_header_accessors {
    ( $( $field_name:ident : $field_type:ty = $field_offset:expr ; )* ) => {
        paste! {
            $(
                #[doc = concat!("Offset of ", stringify!($field_name), " — type ", stringify!($field_type))]
                pub(crate) const [<$field_name:upper>] : usize = $field_offset;

                impl<'a> HeaderRef<'a> {
                    #[doc = concat!(
                        "Getter for field `", stringify!($field_name), "`.\n",
                        "Type: `", stringify!($field_type), "`.\n",
                        "Offset: ", stringify!($field_offset), "."
                    )]
                    pub(crate) fn [<get_ $field_name>](&self)
                        -> Result<$field_type, HeaderError>
                    {
                        Ok(read_le::<$field_type>(self.bytes, $field_offset)?)
                    }
                }

                impl<'a> HeaderMut<'a> {
                    #[doc = concat!(
                        "Getter for field `", stringify!($field_name), "`.\n",
                        "Type: `", stringify!($field_type), "`.\n",
                        "Offset: ", stringify!($field_offset), "."
                    )]
                    pub(crate) fn [<get_ $field_name>](&self)
                        -> Result<$field_type, HeaderError>
                    {
                        Ok(read_le::<$field_type>(self.bytes, $field_offset)?)
                    }

                    #[doc = concat!(
                        "Setter for field `", stringify!($field_name), "`.\n",
                        "Type: `", stringify!($field_type), "`.\n",
                        "Offset: ", stringify!($field_offset), "."
                    )]
                    pub(crate) fn [<set_ $field_name>](&mut self, val: $field_type)
                        -> Result<(), HeaderError>
                    {
                        write_le::<$field_type>(self.bytes, $field_offset, val)?;
                        Ok(())
                    }
                }
            )*
        }
    };
}

// (Almost) identical implementation as in the Java version
impl_header_accessors! {
    slot_count : u16 = 0;
    free_start : u16 = 2;
    free_end   : u16 = 4;
    free_space : u16 = 6;
    can_compact : u16 = 8;
    page_number : u32 = 10;
    page_type : u16 = 14;
    left_page : u32 = 16;
    right_page : u32 = 20;
    // In the Java implementation, we had parent_page at offset 22, but we will not be including it in this implementation.
    last_lsn : u64 = 26; // TODO for now, we will use offset 26 for last_lsn to maintain consistency with the Java implementation
}

#[cfg(test)]
mod header_ref_tests {
    use super::*;

    #[test]
    fn test_header_ref_getters() {
        // Hardcoded header bytes (little-endian) with a distinct value per field.
        let mut header_bytes = [0u8; HEADER_SIZE];

        // Populate each field at its defined offset using the public offset constants.
        header_bytes[SLOT_COUNT..SLOT_COUNT + 2].copy_from_slice(&0x1122u16.to_le_bytes()); // slot_count
        header_bytes[FREE_START..FREE_START + 2].copy_from_slice(&0x3344u16.to_le_bytes()); // free_start
        header_bytes[FREE_END..FREE_END + 2].copy_from_slice(&0x5566u16.to_le_bytes()); // free_end
        header_bytes[FREE_SPACE..FREE_SPACE + 2].copy_from_slice(&0x7788u16.to_le_bytes()); // free_space
        header_bytes[CAN_COMPACT..CAN_COMPACT + 2].copy_from_slice(&0x99AAu16.to_le_bytes()); // can_compact
        header_bytes[PAGE_NUMBER..PAGE_NUMBER + 4].copy_from_slice(&0x11223344u32.to_le_bytes()); // page_number
        header_bytes[PAGE_TYPE..PAGE_TYPE + 2].copy_from_slice(&0x5566u16.to_le_bytes()); // page_type
        header_bytes[LEFT_PAGE..LEFT_PAGE + 4].copy_from_slice(&0x778899AAu32.to_le_bytes()); // left_page
        header_bytes[RIGHT_PAGE..RIGHT_PAGE + 4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes()); // right_page
        header_bytes[LAST_LSN..LAST_LSN + 8].copy_from_slice(&0x0123456789ABCDEFu64.to_le_bytes()); // last_lsn

        let header = HeaderRef::new(&header_bytes).unwrap();

        assert_eq!(header.get_slot_count().unwrap(), 0x1122u16);
        assert_eq!(header.get_free_start().unwrap(), 0x3344u16);
        assert_eq!(header.get_free_end().unwrap(), 0x5566u16);
        assert_eq!(header.get_free_space().unwrap(), 0x7788u16);
        assert_eq!(header.get_can_compact().unwrap(), 0x99AAu16);
        assert_eq!(header.get_page_number().unwrap(), 0x11223344u32);
        assert_eq!(header.get_page_type().unwrap(), 0x5566u16);
        assert_eq!(header.get_left_page().unwrap(), 0x778899AAu32);
        assert_eq!(header.get_right_page().unwrap(), 0xDEADBEEFu32);
        assert_eq!(header.get_last_lsn().unwrap(), 0x0123456789ABCDEFu64);
    }

    #[test]
    fn needs_compaction_returns_true_when_free_space_is_insufficient() {
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes[FREE_START..FREE_START + 2].copy_from_slice(&0x0010u16.to_le_bytes());
        header_bytes[FREE_END..FREE_END + 2].copy_from_slice(&0x0015u16.to_le_bytes());

        let header = HeaderRef::new(&header_bytes).unwrap();
        let row_size = 4;

        assert!(header.needs_compaction(row_size).unwrap());
    }

    #[test]
    fn needs_compaction_returns_false_when_free_space_is_sufficient() {
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes[FREE_START..FREE_START + 2].copy_from_slice(&0x0010u16.to_le_bytes());
        header_bytes[FREE_END..FREE_END + 2].copy_from_slice(&0x0020u16.to_le_bytes());

        let header = HeaderRef::new(&header_bytes).unwrap();
        let row_size = 4;

        assert!(!header.needs_compaction(row_size).unwrap());
    }

    #[test]
    fn new_incorrect_slice_size_error_returned() {
        let header_bytes = [0u8; HEADER_SIZE + 1];

        let result = HeaderRef::new(&header_bytes);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HeaderError::HeaderSliceSizeMismatch { actual: 97 }
        ))
    }
}

#[cfg(test)]
mod header_mut_tests {
    use super::*;

    #[test]
    fn test_header_mut_setters() {
        // 1) Prepare a zeroed header byte array.
        let mut header_bytes = [0u8; HEADER_SIZE];

        // 2) Obtain a mutable header view and set each field to a unique value.
        let mut header_mut = HeaderMut::new(&mut header_bytes).unwrap();

        header_mut.set_slot_count(0x0102u16).unwrap();
        header_mut.set_free_start(0x0304u16).unwrap();
        header_mut.set_free_end(0x0506u16).unwrap();
        header_mut.set_free_space(0x0708u16).unwrap();
        header_mut.set_can_compact(0x090Au16).unwrap();
        header_mut.set_page_number(0x0B0C0D0Eu32).unwrap();
        header_mut.set_page_type(0x0F10u16).unwrap();
        header_mut.set_left_page(0x11121314u32).unwrap();
        header_mut.set_right_page(0x15161718u32).unwrap();
        header_mut.set_last_lsn(0xDEADBEEFCAFEBABEu64).unwrap();

        assert_eq!(header_mut.get_slot_count().unwrap(), 0x0102u16);
        assert_eq!(header_mut.get_free_start().unwrap(), 0x0304u16);
        assert_eq!(header_mut.get_free_end().unwrap(), 0x0506u16);
        assert_eq!(header_mut.get_free_space().unwrap(), 0x0708u16);
        assert_eq!(header_mut.get_can_compact().unwrap(), 0x090Au16);
        assert_eq!(header_mut.get_page_number().unwrap(), 0x0B0C0D0Eu32);
        assert_eq!(header_mut.get_page_type().unwrap(), 0x0F10u16);
        assert_eq!(header_mut.get_left_page().unwrap(), 0x11121314u32);
        assert_eq!(header_mut.get_right_page().unwrap(), 0x15161718u32);
        assert_eq!(header_mut.get_last_lsn().unwrap(), 0xDEADBEEFCAFEBABEu64);
    }

    #[test]
    fn new_incorrect_slice_size_error_returned() {
        let mut header_bytes = [0u8; HEADER_SIZE + 1];

        let result = HeaderMut::new(&mut header_bytes);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HeaderError::HeaderSliceSizeMismatch { actual: 97 }
        ))
    }

    #[test]
    fn test_header_mut_default() {
        let mut header_bytes = [0u8; HEADER_SIZE];
        let mut header_mut = HeaderMut::new(&mut header_bytes).unwrap();

        header_mut.default(42, PageType::Unsorted);

        let header_ref = HeaderRef::new(&header_bytes).unwrap();

        assert_eq!(header_ref.get_page_number().unwrap(), 42);
        assert_eq!(header_ref.get_free_start().unwrap(), HEADER_SIZE as u16);
        assert_eq!(header_ref.get_free_end().unwrap(), (PAGE_SIZE - 1) as u16);
        assert_eq!(
            header_ref.get_page_type().unwrap(),
            u16::from(PageType::Unsorted)
        );
    }
}
