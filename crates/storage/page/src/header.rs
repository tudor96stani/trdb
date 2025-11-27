//! Module defining the layout and accessors for the page header in a slotted page.
//!
//! # Page Header Layout
//!
//! The page header occupies the first **128 bytes** of every slotted page and
//! contains metadata required to manage the page, free space, and its
//! relationship to neighboring pages. All fields are stored in **little-endian**
//! binary format.
//!
//! The layout is as follows:
//!
//! | Field          | Type  | Offset | Description |
//! |----------------|-------|--------|-------------|
//! | `slot_count`   | u16   | [`SLOT_COUNT`]   | Number of active slots in the page. |
//! | `free_start`   | u16   | [`FREE_START`]   | Beginning of the free-space region (grows upward). |
//! | `free_end`     | u16   | [`FREE_END`]     | End of the free-space region (grows downward). |
//! | `free_space`   | u16   | [`FREE_SPACE`]   | Total number of free bytes available. |
//! | `can_compact`  | u16   | [`CAN_COMPACT`]  | Whether the page requires compaction (0 or 1). |
//! | `page_number`  | u16   | [`PAGE_NUMBER`]  | Logical page identifier within the file. |
//! | `page_type`    | u16   | [`PAGE_TYPE`]    | Page classification (data, internal, etc.). |
//! | `left_page`    | u32   | [`LEFT_PAGE`]    | Pointer to the left sibling page (if any). |
//! | `right_page`   | u32   | [`RIGHT_PAGE`]   | Pointer to the right sibling page (if any). |
//! | `last_lsn`     | u64   | [`LAST_LSN`]     | Last log sequence number applied to this page. |
//!
//! ## Notes
//!
//! - The design mirrors the Java implementation for compatibility.
//! - The Java version included `parent_page` at offset 22, which this
//!   implementation **intentionally omits**.
//! - The `last_lsn` field currently starts at offset 26 to preserve binary
//!   compatibility with the previous layout.
//!
//! ## Memory Diagram
//!
//! ```text
//! +----------------------+-------------------+----------------------+
//! |      Header          |     Data Area     |      Slot Array      |
//! |       (128B)         |   (variable)      |     (grows left)     |
//! +----------------------+-------------------+----------------------+
//! ```
//!
//! Each constant below defines the byte offset where its corresponding field is
//! stored within the header.

use crate::page_error::PageError;
use binary_helpers::le::{read_le, write_le};
use paste::paste;

/// Size of the header in bytes.
pub const HEADER_SIZE: usize = 128;

/// The 'HeaderRef' struct provides an immutable view into the header of a page.
#[derive(Debug)]
pub struct HeaderRef<'a> {
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
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        (bytes.len() == HEADER_SIZE).then(|| HeaderRef {
            // Convert the slice reference into a fixed size array reference.
            // Should be safe to unwrap since we checked the size already
            bytes: bytes.try_into().unwrap(),
        })
    }
}

/// The `HeaderMut` struct provides a mutable view into the header of a page.
#[derive(Debug)]
pub struct HeaderMut<'a> {
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
    pub fn new(bytes: &'a mut [u8]) -> Option<Self> {
        (bytes.len() == HEADER_SIZE).then(|| HeaderMut {
            // Convert the slice reference into a fixed size array reference.
            // Should be safe to unwrap since we checked the size already
            bytes: bytes.try_into().unwrap(),
        })
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
                pub const [<$field_name:upper>] : usize = $field_offset;

                impl<'a> HeaderRef<'a> {
                    #[doc = concat!(
                        "Getter for field `", stringify!($field_name), "`.\n",
                        "Type: `", stringify!($field_type), "`.\n",
                        "Offset: ", stringify!($field_offset), "."
                    )]
                    pub fn [<get_ $field_name>](&self)
                        -> Result<$field_type, PageError>
                    {
                        Ok(read_le::<$field_type>(self.bytes, $field_offset)?)
                    }
                }

                impl<'a> HeaderMut<'a> {
                    #[doc = concat!(
                        "Setter for field `", stringify!($field_name), "`.\n",
                        "Type: `", stringify!($field_type), "`.\n",
                        "Offset: ", stringify!($field_offset), "."
                    )]
                    pub fn [<set_ $field_name>](&mut self, val: $field_type)
                        -> Result<(), PageError>
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
    page_number : u16 = 10;
    page_type : u16 = 12;
    left_page : u32 = 14;
    right_page : u32 = 18;
    // In the Java implementation, we had parent_page at offset 22, but we will not be including it in this implementation.
    last_lsn : u64 = 26; // TODO for now, we will use offset 26 for last_lsn to maintain consistency with the Java implementation
}
