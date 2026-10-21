use shared::schemas::identifiers::{DatabaseId, ObjectId};
use std::fmt;

/// The ID of a file. It is also the ID of the object (table or nonclustered index) stored in it.
pub type FileId = ObjectId;

/// A unique identifier for any page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId {
    /// Unique identifier of the database owning the file containing the page.
    pub db_id: DatabaseId,

    /// Identifier of the file containing the page, unique within its database.
    pub file_id: FileId,

    /// The specific page number within the file.
    pub page_number: u32,
}

impl PageId {
    /// Creates a new `PageId` instance with the given database ID, file ID and page number.
    pub fn new(db_id: DatabaseId, file_id: FileId, page_number: u32) -> Self {
        Self {
            db_id,
            file_id,
            page_number,
        }
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.db_id, self.file_id, self.page_number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_id_creation() {
        let page_id = PageId::new(DatabaseId::new(2), 1, 42);
        assert_eq!(page_id.db_id, DatabaseId::new(2));
        assert_eq!(page_id.file_id, 1);
        assert_eq!(page_id.page_number, 42);
    }

    #[test]
    fn test_get_file_id() {
        let page_id = PageId::new(DatabaseId::new(1), 123, 456);
        assert_eq!(page_id.file_id, 123);
    }

    #[test]
    fn test_get_page_number() {
        let page_id = PageId::new(DatabaseId::new(1), 789, 1011);
        assert_eq!(page_id.page_number, 1011);
    }

    #[test]
    fn display_formats_correctly() {
        let page_id = PageId::new(DatabaseId::new(2), 123, 456);
        assert_eq!(page_id.to_string(), "2:123:456");
    }

    #[test]
    fn display_handles_zero_values() {
        let page_id = PageId::new(DatabaseId::new(0), 0, 0);
        assert_eq!(page_id.to_string(), "0:0:0");
    }

    #[test]
    fn display_handles_large_values() {
        let page_id = PageId::new(DatabaseId::new(ObjectId::MAX), ObjectId::MAX, u32::MAX);
        assert_eq!(
            page_id.to_string(),
            format!("{}:{}:{}", ObjectId::MAX, ObjectId::MAX, u32::MAX)
        );
    }
}
