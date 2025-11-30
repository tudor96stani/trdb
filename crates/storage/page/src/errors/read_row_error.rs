use crate::errors::header_error::HeaderError;
use crate::errors::slot_error::SlotError;
use crate::header;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum ReadRowError {
    #[error("Error while reading slot array")]
    SlotError(#[from] SlotError),
}
