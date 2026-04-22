use crate::page::metadata::page_id::PageId;
use shared::binary_helpers::conversions::ConversionError;
use thiserror::Error;

/// Public facing error type returned by the Page crate.
#[derive(Debug, Error)]
#[error("error on page {page_id}: {source}")]
pub struct PageError {
    /// The page ID on which the error occurred
    pub page_id: PageId,
    /// The source error
    pub source: PageOpError,
}

/// Public facing result type of page operations.
pub type PageResult<T> = Result<T, PageError>;

/// Helper trait to attach page_id context when surfacing errors.
pub(crate) trait WithPageId<T> {
    /// Attaches the `PageId` to the error
    fn with_page_id(self, page_id: PageId) -> PageResult<T>;
}

impl<T> WithPageId<T> for Result<T, PageOpError> {
    fn with_page_id(self, page_id: PageId) -> PageResult<T> {
        self.map_err(|source| PageError { page_id, source })
    }
}

/// Aggregator error type for all possible page related sub-errors
#[derive(Debug, Error)]
pub enum PageOpError {
    /// a
    #[error(transparent)]
    Header(#[from] HeaderError),
    /// a
    #[error(transparent)]
    Slot(#[from] SlotError),
    /// a
    #[error(transparent)]
    ReadRow(#[from] ReadRowError),
    /// a
    #[error(transparent)]
    Insert(#[from] InsertError),
    /// a
    #[error(transparent)]
    DeleteRow(#[from] DeleteError),
    /// a
    #[error(transparent)]
    UpdateRow(#[from] UpdateError),
}

/// Error while interacting with the slot array
#[derive(Debug, Error)]
pub enum SlotError {
    /// The slot array region was expected to be of a different size
    #[error("Slot array region expected to be {expected_size}, but was actually {actual_size}")]
    SlotRegionSizeMismatch {
        /// Expected size of the slot array region
        expected_size: usize,
        /// Actual size of the slot array region
        actual_size: usize,
    },
    /// The region for a slot to be read did not match the expected size
    #[error(
        "Attempted to read a slot from an invalid slice size. Expected {expected_size}, but was {actual_size}"
    )]
    SlotSizeMismatch {
        /// Expected size of the region
        expected_size: usize,
        /// Actual size of the region
        actual_size: usize,
    },
    /// Attempt to access an invalid slot
    #[error("Attempted to access an invalid slot index: {slot_index}")]
    InvalidSlot {
        /// The slot index that was accessed
        slot_index: usize,
    },
    #[error(transparent)]
    /// Error while working with the raw binary data
    BinaryError(#[from] shared::binary_helpers::bin_error::BinaryError),
    /// Error while working with the header of the page
    #[error(transparent)]
    HeaderError(#[from] HeaderError),
}

/// Error on interacting with the page header
#[derive(Debug, Error)]
pub enum HeaderError {
    /// Some binary operation on the bytes of the header failed
    #[error(transparent)]
    BinaryError(#[from] shared::binary_helpers::bin_error::BinaryError),
    /// Computing offsets for the header values resulted in an error
    #[error("Arithmetic error while computing offsets within header")]
    OffsetArithmetic,
    /// Expected a slice of bytes of a certain size, but found another length
    #[error("Provided slice length ({actual}) does not match the expected length")]
    HeaderSliceSizeMismatch {
        /// Actual length of the slice
        actual: usize,
        /// Expected length of the slice
        expected: usize,
    },
}

/// Error while inserting row in page
#[derive(Debug, Error)]
pub enum InsertError {
    /// There is not enough space on the page
    #[error("Unable to insert row of length {row_len} in page with {page_free_space} free bytes")]
    NotEnoughSpace {
        /// Required number of bytes to insert the row
        row_len: usize,
        /// Actual number of free bytes in the page
        page_free_space: usize,
    },
    /// Error while accessing the slot array
    #[error(transparent)]
    SlotError(#[from] SlotError),
    /// Error while accessing the header
    #[error(transparent)]
    HeaderError(#[from] HeaderError),
}

/// Error while reading the row from the page
#[derive(Debug, Error)]
pub enum ReadRowError {
    /// Error while accessing the slot array
    #[error(transparent)]
    SlotError(#[from] SlotError),
}

/// Errors while updating a row on the page
#[derive(Debug, Error)]
pub enum UpdateError {
    /// Not enough space left on the page for the new value of the row
    #[error("Unable to update row of length {row_len} in page with {page_free_space} free bytes")]
    NotEnoughSpace {
        /// Required number of bytes to insert the row
        row_len: usize,
        /// Actual number of free bytes in the page
        page_free_space: usize,
    },
    /// Error while working with the slot array of the page
    #[error(transparent)]
    SlotError(#[from] SlotError),
    /// Error while working with the header of the page
    #[error(transparent)]
    HeaderError(#[from] HeaderError),
    /// Error while trying to insert the new row value
    #[error(transparent)]
    InsertError(#[from] InsertError),
    /// Error while converting between data types
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    /// Error while trying to delete the old row value
    #[error(transparent)]
    DeleteError(#[from] DeleteError),
}

/// Error on delete operation from page
#[derive(Debug, Error)]
pub enum DeleteError {
    /// Error while accessing slot array
    #[error(transparent)]
    SlotError(#[from] SlotError),
    /// Error while accessing header
    #[error(transparent)]
    HeaderError(#[from] HeaderError),
}
