use thiserror::Error;

pub trait UsizeConversion {
    fn to_u16(self) -> Result<u16, ConversionError>;
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

#[derive(Debug, Error)]
pub enum ConversionError {
    #[error("Value exceeds maximum for target type")]
    Overflow,
}
