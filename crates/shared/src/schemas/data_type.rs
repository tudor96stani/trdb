use std::fmt::Display;

/// Available data types for the columns in the tables
#[derive(Debug, Eq, PartialEq)]
pub enum DataType {
    /// Integer type
    Int,
    /// String type
    String,
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Int => write!(f, "Int"),
            DataType::String => write!(f, "String"),
        }
    }
}
