//! Slotted structure and related functionality.
use crate::header;
use crate::header::{HEADER_SIZE, HeaderRef};

const PAGE_SIZE: usize = 4096;

struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    fn new_zeroed() -> Self {
        Self {
            data: [0; PAGE_SIZE],
        }
    }

    fn header_ref(&'_ self) -> HeaderRef<'_> {
        HeaderRef::new(&self.data[..HEADER_SIZE]).unwrap()
    }
}
