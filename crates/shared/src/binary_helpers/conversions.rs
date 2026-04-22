use thiserror::Error;

/// Defines the conversion from `usize` to other integer types
pub trait UsizeConversion {
    /// Conversion from `usize` to `u16`
    fn to_u16(self) -> Result<u16, ConversionError>;

    /// Conversion from `usize` to `u32`
    fn to_u32(self) -> Result<u32, ConversionError>;
}

impl UsizeConversion for usize {
    fn to_u16(self) -> Result<u16, ConversionError> {
        u16::try_from(self).map_err(|_| ConversionError::Overflow)
    }

    fn to_u32(self) -> Result<u32, ConversionError> {
        u32::try_from(self).map_err(|_| ConversionError::Overflow)
    }
}

/// Conversion error between data types
#[derive(Debug, Error)]
pub enum ConversionError {
    /// Source value would overflow in target type
    #[error("Value exceeds maximum for target type")]
    Overflow,
}
