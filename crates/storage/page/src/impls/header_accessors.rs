use crate::HEADER_SIZE;
use crate::header::{HeaderMut, HeaderRef};
use crate::impls::Page;

/// Header access methods for the `Page` struct.
impl Page {
    /// Returns a read-only reference to the page header.
    pub(crate) fn header_ref(&'_ self) -> HeaderRef<'_> {
        HeaderRef::new(&self.data[..HEADER_SIZE]).unwrap() // todo remove this unwrap
    }

    /// Returns a mutable reference to the page header.
    pub(crate) fn header_mut(&'_ mut self) -> HeaderMut<'_> {
        HeaderMut::new(&mut self.data[..HEADER_SIZE]).unwrap() // todo remove this unwrap
    }
}
