use crate::row_interpreter::RowInterpreterError;
use thiserror::Error;

/// Public error type for engine crate
#[derive(Error, Debug)]
pub enum EngineError {
    /// Error while performing binary row interpretation
    #[error(transparent)]
    RowInterpreter(#[from] RowInterpreterError),
}
