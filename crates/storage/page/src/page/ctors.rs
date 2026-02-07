use crate::PAGE_SIZE;
use crate::errors::header_error::HeaderError;
use crate::errors::page_error::{PageResult, WithPageId};
use crate::errors::page_op_error::PageOpError;
use crate::page::api::Page;
use crate::page_id::PageId;
use crate::page_type::PageType;

/// Internal methods for creating and initializing pages.
impl Page {
    /// Creates a new page from an existing byte array.
    pub(crate) fn new_from_bytes(bytes: Box<[u8; 4096]>, page_id: PageId) -> Self {
        Self {
            data: bytes,
            page_id,
        }
    }

    /// Creates a new empty page with the specified page ID and page type.
    pub(crate) fn new_empty(page_id: PageId, page_type: PageType) -> Result<Self, HeaderError> {
        let mut page = Self::new_zeroed(page_id);

        page.header_mut()?.default(page_id.page_number, page_type);

        Ok(page)
    }
}

#[cfg(test)]
mod new_and_accessors_tests {
    use super::*;
    use crate::page_type::PageType;

    #[test]
    fn test_new_empty_page() {
        let page_id = PageId::new(1, 0);
        let page = Page::new_empty(page_id, PageType::Unsorted).unwrap();

        assert_eq!(page.page_id(), page_id);

        let header = page.header_ref().unwrap();
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
}
