use std::fmt;

/// A simple type to define the unique FileId, which is at its core just a u32
pub type FileId = u32;

/// A unique identifier for any page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
    /// Unique identifier of the file containing the page.
    pub file_id: FileId,

    /// The specific page number within the file.
    pub page_number: u32,
}

impl PageId {
    /// Creates a new `PageId` instance with the given file ID and page number.
    pub fn new(file_id: u32, page_number: u32) -> Self {
        Self {
            file_id,
            page_number,
        }
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file_id, self.page_number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_id_creation() {
        let page_id = PageId::new(1, 42);
        assert_eq!(page_id.file_id, 1);
        assert_eq!(page_id.page_number, 42);
    }

    #[test]
    fn test_get_file_id() {
        let page_id = PageId::new(123, 456);
        assert_eq!(page_id.file_id, 123);
    }

    #[test]
    fn test_get_page_number() {
        let page_id = PageId::new(789, 1011);
        assert_eq!(page_id.page_number, 1011);
    }

    #[test]
    fn display_formats_correctly() {
        let page_id = PageId::new(123, 456);
        assert_eq!(page_id.to_string(), "123:456");
    }

    #[test]
    fn display_handles_zero_values() {
        let page_id = PageId::new(0, 0);
        assert_eq!(page_id.to_string(), "0:0");
    }

    #[test]
    fn display_handles_large_values() {
        let page_id = PageId::new(u32::MAX, u32::MAX);
        assert_eq!(page_id.to_string(), format!("{}:{}", u32::MAX, u32::MAX));
    }
}
