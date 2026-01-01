use crate::errors::delete_error::DeleteError;
use crate::errors::header_error::HeaderError;
use crate::errors::insert_error::InsertError;
use crate::errors::read_row_error::ReadRowError;
use crate::errors::slot_error::SlotError;
use thiserror::Error;

/// Aggregator error type for all possible page related sub-errors
#[derive(Debug, Error)]
pub(crate) enum PageOpError {
    #[error("Error while accessing header")]
    Header(#[from] HeaderError),
    #[error("Error while accessing slot array")]
    Slot(#[from] SlotError),
    #[error("Error while reading row")]
    ReadRow(#[from] ReadRowError),
    #[error("Error while inserting row")]
    Insert(#[from] InsertError),
    #[error("Error while deleting row")]
    DeleteRow(#[from] DeleteError),
}
