use crate::errors::header_error::HeaderError;
use crate::errors::slot_error::SlotError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum DeleteError {
    #[error("Error while accessing slot array")]
    SlotError(#[from] SlotError),
    #[error("Error while accessing header")]
    HeaderError(#[from] HeaderError),
}
