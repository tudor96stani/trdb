use crate::errors::page_op_error::PageOpError;
use crate::page_id::PageId;

/// Public facing error type returned by the Page module.
///
#[derive(Debug, thiserror::Error)]
#[error("error on page {page_id}: {source}")]
pub struct PageError {
    /// The page ID on which the error occurred
    pub(crate) page_id: PageId,
    /// The source error
    pub(crate) source: PageOpError,
}

/// Public facing result type of page operations.
pub type PageResult<T> = Result<T, PageError>;

/// Helper trait to attach page_id context when surfacing errors.
pub(crate) trait WithPageId<T> {
    fn with_page_id(self, page_id: PageId) -> PageResult<T>;
}

impl<T> WithPageId<T> for Result<T, PageOpError> {
    fn with_page_id(self, page_id: PageId) -> PageResult<T> {
        self.map_err(|source| PageError { page_id, source })
    }
}
