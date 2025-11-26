use crate::page_error::PageError;
use binary_helpers::le::{read_le, write_le};
use paste::paste;

const HEADER_SIZE: usize = 128;

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
/// Pattern: `field_id(identifier) : field_type(type) = field_offset(usize)`
macro_rules! impl_header_accessors {
    ( $( $field_name:ident : $field_type:ty = $field_offset:expr ; )* ) => {
        paste! {
            $(
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

// TODO finish adding all the header fields
impl_header_accessors! {
    page_id    : u32 = 0;
    slot_count : u16 = 4;
    free_start : u16 = 6;
    free_end   : u16 = 8;
}