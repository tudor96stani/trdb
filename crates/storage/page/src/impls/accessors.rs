use crate::impls::Page;
use crate::page_id::PageId;

/// Accessor methods for the `Page` struct.
impl Page {
    /// Returns the unique identifier of the page.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }
}

#[cfg(test)]
mod new_and_accessors_tests {
    use super::*;
    use crate::page_type::PageType;

    #[test]
    fn test_get_page_id() {
        let page_id = PageId::new(2, 5);
        let page = Page::new_empty(page_id, PageType::IndexLeaf).unwrap();

        assert_eq!(page.page_id(), page_id);
    }
}
