use crate::errors::header_error::HeaderError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum SlotError {
    #[error("Slot array region expected to be {expected_size}, but was actually {actual_size}")]
    SlotRegionSizeMismatch {
        expected_size: usize,
        actual_size: usize,
    },
    #[error(
        "Attempted to read a slot from an invalid slice size. Expected {expected_size}, but was {actual_size}"
    )]
    SlotSizeMismatch {
        expected_size: usize,
        actual_size: usize,
    },
    #[error("Attempted to access an invalid slot index: {slot_index}")]
    InvalidSlot { slot_index: usize },
    #[error("Error while interpreting binary data.")]
    BinaryError(#[from] binary_helpers::bin_error::BinaryError),
    #[error("Error while reading page header")]
    HeaderError(#[from] HeaderError),
}
