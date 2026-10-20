use crate::row_interpreter::{RowDeserializationError, RowSerializationError};
use thiserror::Error;

/// Public error type for engine crate
#[derive(Error, Debug)]
pub enum EngineError {
    /// Error while performing binary row interpretation
    #[error(transparent)]
    RowSerialization(#[from] RowSerializationError),
    /// Error while deserializing a binary row.
    #[error(transparent)]
    RowDeserialization(#[from] RowDeserializationError),
}
