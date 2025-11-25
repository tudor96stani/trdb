use crate::bin_error::BinaryError;

pub trait LittleEndianInteger: Sized + Copy {
    const SIZE: usize;

    /// Read the slice provided via the `bytes` param and convert it to the target integer type.
    /// Assumes `bytes` is the same size as `Self::SIZE`
    fn from_le(bytes: &[u8]) -> Result<Self, BinaryError>;

    /// Write `Self` to the provided slice. Assumes `target` is the exact same size as `Self` when converted to bytes.
    fn to_le(self, out: &mut [u8]) -> Result<(), BinaryError>;
}

macro_rules! impl_little_endian_integer {
    ($t:ty) => {
        impl LittleEndianInteger for $t {
            const SIZE: usize = std::mem::size_of::<$t>();

            fn from_le(bytes: &[u8]) -> Result<Self, BinaryError> {
                // this really should not happen, but we will check just in case
                if bytes.len() != Self::SIZE {
                    return Err(BinaryError::ReadErrorInvalidSliceSize {
                        // We don't really have any info as to where this slice appears in the main
                        // byte array, so we'll report offset 0 as the starting point
                        from_offset: 0usize,
                        expected: Self::SIZE
                    })
                }

                Ok(<$t>::from_le_bytes(bytes.try_into()?))
            }

            fn to_le(self, target: &mut [u8]) -> Result<(), BinaryError> {
                let self_bytes = &self.to_le_bytes();

                // Proactively compare sizes to avoid a panic
                if self_bytes.len() != target.len() {
                    return Err(BinaryError::WriteErrorSliceSizeMismatch {
                        src : self_bytes.len(),
                        target: target.len()
                    });
                }

                target.copy_from_slice(self_bytes);
                Ok(()) // It went fine
            }
        }
    };
}

impl_little_endian_integer!(u16);
impl_little_endian_integer!(u32);
impl_little_endian_integer!(u64);

/// Reads a little-endian integer from the `bytes` array, starting at `start_offset`.
/// Returns
pub fn read_le<T: LittleEndianInteger>(bytes: &[u8], start_offset: usize) -> Result<T, BinaryError> {
    // Is the range we are trying to read valid?
    let Some(slice) = bytes.get(start_offset..start_offset + T::SIZE) else {
        return Err(BinaryError::ReadErrorInvalidSliceSize {
            expected : T::SIZE,
            from_offset: start_offset
        })
    };

    // We can proceed with the conversion
    T::from_le(slice)
}

/// Converts the provided `value` to bytes and writes them at `offset` in the `bytes` array.
pub fn write_le<T: LittleEndianInteger>(bytes: &mut [u8], start_offset: usize, value: T) -> Result<(), BinaryError>{
    // Is the range we are trying to write into valid?
    let Some(slice) = bytes.get_mut(start_offset..start_offset + T::SIZE) else {
        return Err(BinaryError::ReadErrorInvalidSliceSize {
            expected : T::SIZE,
            from_offset: start_offset
        })
    };

    // We can proceed to write the data in the slice
    value.to_le(slice)
}

#[cfg(test)]
mod read_le_tests {
    use super::*;

    #[test]
    fn test_read_le_u16() {
        let bytes = [0x17, 0x00];
        let result = read_le::<u16>(&bytes, 0).unwrap();
        assert_eq!(result, 23);
    }

    #[test]
    fn test_read_le_u16_larger_array() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x17, 0x00];
        let result = read_le::<u16>(&bytes, 4).unwrap();
        assert_eq!(result, 23);
    }

    #[test]
    fn test_read_le_u32() {
        let bytes = [0x17, 0x00, 0x00, 0x00];
        let result = read_le::<u32>(&bytes, 0).unwrap();
        assert_eq!(result, 23);
    }

    #[test]
    fn test_read_le_u64() {
        let bytes = [0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let result = read_le::<u64>(&bytes, 0).unwrap();
        assert_eq!(result, 23);
    }
}