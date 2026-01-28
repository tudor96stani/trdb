use crate::errors::delete_error::DeleteError;
use crate::errors::header_error::HeaderError;
use crate::errors::insert_error::InsertError;
use crate::errors::slot_error::SlotError;
use binary_helpers::conversions::ConversionError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum UpdateError {
    #[error("Unable to update row of length {row_len} in page with {page_free_space} free bytes")]
    NotEnoughSpace {
        /// Required number of bytes to insert the row
        row_len: usize,
        /// Actual number of free bytes in the page
        page_free_space: usize,
    },
    #[error("Error while accessing slot array")]
    SlotError(#[from] SlotError),
    #[error("Error while accessing header")]
    HeaderError(#[from] HeaderError),
    #[error("Error while inserting the value for the updated row")]
    InsertError(#[from] InsertError),
    #[error("Error while converting between data types")]
    ConversionError(#[from] ConversionError),
    #[error("Error while deleting the old row")]
    DeleteError(#[from] DeleteError),
}
