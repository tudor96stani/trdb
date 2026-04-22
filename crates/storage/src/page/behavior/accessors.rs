use crate::page::HEADER_SIZE;
use crate::page::errors::HeaderError;
use crate::page::layout::header::{HeaderMut, HeaderRef};
use crate::page::layout::page::Page;
use crate::page::metadata::page_id::PageId;

/// Accessor methods for the `Page` struct.
impl Page {
    /// Returns the unique identifier of the page.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Sets the `PageId` field of the page instance
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    /// Returns a read-only reference to the page header.
    pub fn header_ref(&'_ self) -> Result<HeaderRef<'_>, HeaderError> {
        HeaderRef::new(&self.data[..HEADER_SIZE])
    }

    /// Returns a mutable reference to the page header.
    pub fn header_mut(&'_ mut self) -> Result<HeaderMut<'_>, HeaderError> {
        HeaderMut::new(&mut self.data[..HEADER_SIZE])
    }
}

#[cfg(test)]
mod new_and_accessors_tests {
    use super::*;
    use crate::page::metadata::page_type::PageType;

    #[test]
    fn test_get_page_id() {
        let page_id = PageId::new(2, 5);
        let page = Page::new_empty(page_id, PageType::IndexLeaf).unwrap();

        assert_eq!(page.page_id(), page_id);
    }
}
