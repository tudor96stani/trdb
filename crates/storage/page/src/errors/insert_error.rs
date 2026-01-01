use crate::errors::header_error::HeaderError;
use crate::errors::slot_error::SlotError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum InsertError {
    #[error("Unable to insert row of length {row_len} in page with {page_free_space} free bytes")]
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
}
