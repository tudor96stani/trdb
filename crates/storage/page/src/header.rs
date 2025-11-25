use crate::page_error::PageError;
use binary_helpers::le::{read_le, write_le};

// Header layout // TODO finish this
const HEADER_SIZE: usize = 128;
const OFF_PAGE_ID: usize = 0; // u32
const OFF_SLOT_COUNT: usize = 4; // u16
const OFF_FREE_START: usize = 6; // u16
const OFF_FREE_END: usize = 8; // u16
//

// An immutable view into the header of a page.
pub struct HeaderRef<'a> {
    bytes: &'a [u8; HEADER_SIZE],
}

impl<'a> HeaderRef<'a> {
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() == HEADER_SIZE {
            // Safely unwrap because the length is checked
            Some(HeaderRef {
                bytes: bytes.try_into().unwrap(),
            })
        } else {
            None
        }
    }

    pub fn page_id(&self) -> Result<u32, PageError> {
        Ok(read_le(self.bytes, OFF_PAGE_ID)?)
    }
    pub fn slot_count(&self) -> Result<u16, PageError> {
        Ok(read_le(self.bytes, OFF_SLOT_COUNT)?)
    }
    pub fn free_start(&self) -> Result<usize, PageError> {
        Ok(usize::from(read_le::<u16>(self.bytes, OFF_FREE_START)?))
    }
    pub fn free_end(&self) -> Result<usize, PageError> {
        Ok(usize::from(read_le::<u16>(self.bytes, OFF_FREE_END)?))
    }
}

// A mutable view into the header of a page.
pub struct HeaderMut<'a> {
    bytes: &'a mut [u8; HEADER_SIZE],
}

impl<'a> HeaderMut<'a> {
    pub fn new(bytes: &'a mut [u8]) -> Option<Self> {
        if bytes.len() == HEADER_SIZE {
            Some(HeaderMut {
                bytes: bytes.try_into().unwrap(),
            })
        } else {
            None
        }
    }
    pub fn set_page_id(&mut self, v: u32) -> Result<(), PageError> {
        Ok(write_le(self.bytes, OFF_PAGE_ID, v)?)
    }
    pub fn set_free_start(&mut self, v: u16) -> Result<(), PageError> {
        Ok(write_le::<u16>(self.bytes, OFF_FREE_START, v)?)
    }
    pub fn set_free_end(&mut self, v: u16) -> Result<(), PageError> {
        Ok(write_le::<u16>(self.bytes, OFF_FREE_END, v)?)
    }
    pub fn set_slot_count(&mut self, v: u16) -> Result<(), PageError> {
        Ok(write_le::<u16>(self.bytes, OFF_SLOT_COUNT, v)?)
    }
}
