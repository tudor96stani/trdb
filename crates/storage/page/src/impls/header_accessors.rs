use crate::HEADER_SIZE;
use crate::errors::header_error::HeaderError;
use crate::header::{HeaderMut, HeaderRef};
use crate::impls::Page;

/// Header access methods for the `Page` struct.
impl Page {
    /// Returns a read-only reference to the page header.
    pub(crate) fn header_ref(&'_ self) -> Result<HeaderRef<'_>, HeaderError> {
        HeaderRef::new(&self.data[..HEADER_SIZE])
    }

    /// Returns a mutable reference to the page header.
    pub(crate) fn header_mut(&'_ mut self) -> Result<HeaderMut<'_>, HeaderError> {
        HeaderMut::new(&mut self.data[..HEADER_SIZE])
    }
}
