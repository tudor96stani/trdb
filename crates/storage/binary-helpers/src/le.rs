use crate::bin_error::BinaryError;

/// A trait for types that can be serialized and deserialized in little-endian format.
/// This trait is implemented for integer types like `u16`, `u32`, and `u64`.
pub trait LittleEndianInteger: Sized + Copy {
    /// The size of the type in bytes.
    const SIZE: usize;

    /// Converts a slice of bytes in little-endian format to the target integer type.
    ///
    /// # Parameters
    /// - `source_bytes`: A slice of bytes to be converted. Must be exactly `Self::SIZE` bytes-long.
    ///
    /// # Returns
    /// - `Ok(Self)`: The deserialized integer value.
    /// - `Err(BinaryError)`: If the slice size does not match `Self::SIZE`.
    fn from_le(source_bytes: &[u8]) -> Result<Self, BinaryError>;

    /// Serializes the integer into a slice of bytes in little-endian format.
    ///
    /// # Parameters
    /// - `target_buffer`: A mutable slice where the serialized bytes will be written. Must be exactly `Self::SIZE` bytes-long.
    ///
    /// # Returns
    /// - `Ok(())`: If the serialization is successful.
    /// - `Err(BinaryError)`: If the size of the output slice does not match `Self::SIZE`.
    fn to_le(self, target_buffer: &mut [u8]) -> Result<(), BinaryError>;
}

macro_rules! impl_little_endian_integer {
    ($t:ty) => {
        impl LittleEndianInteger for $t {
            const SIZE: usize = std::mem::size_of::<$t>();

            fn from_le(source_bytes: &[u8]) -> Result<Self, BinaryError> {
                // this really should not happen, but we will check just in case
                if source_bytes.len() != Self::SIZE {
                    return Err(BinaryError::BytesSliceSizeMismatch {
                        // We don't really have any info as to where this slice appears in the main
                        // byte array, so we'll report offset 0 as the starting point
                        from_offset: 0usize,
                        expected: Self::SIZE,
                    });
                }

                Ok(<$t>::from_le_bytes(source_bytes.try_into()?))
            }

            fn to_le(self, target_buffer: &mut [u8]) -> Result<(), BinaryError> {
                let self_bytes = &self.to_le_bytes();

                // Proactively compare sizes to avoid a panic
                if self_bytes.len() != target_buffer.len() {
                    return Err(BinaryError::WriteErrorSliceSizeMismatch {
                        src: self_bytes.len(),
                        target: target_buffer.len(),
                    });
                }

                target_buffer.copy_from_slice(self_bytes);
                Ok(()) // It went fine
            }
        }
    };
}

impl_little_endian_integer!(u16);
impl_little_endian_integer!(u32);
impl_little_endian_integer!(u64);

/// Reads a value of type `T` from a byte slice in little-endian format.
///
/// # Parameters
/// - `bytes`: The input byte slice containing the data to be read.
/// - `start_offset`: The starting position in the slice from which to read the value.
///
/// # Returns
/// - `Ok(T)`: The deserialized value of type `T`.
/// - `Err(BinaryError)`: If the slice range is invalid, or the size does not match `T::SIZE`.
pub fn read_le<T: LittleEndianInteger>(
    bytes: &[u8],
    start_offset: usize,
) -> Result<T, BinaryError> {
    // Is the range we are trying to read valid?
    let Some(slice) = bytes.get(start_offset..start_offset + T::SIZE) else {
        return Err(BinaryError::BytesSliceSizeMismatch {
            expected: T::SIZE,
            from_offset: start_offset,
        });
    };

    // We can proceed with the conversion
    T::from_le(slice)
}

/// Writes a value of type `T` into a byte slice in little-endian format.
///
/// # Parameters
/// - `bytes`: The output byte slice where the serialized value will be written.
/// - `start_offset`: The starting position in the slice to write the value.
/// - `value`: The value of type `T` to be serialized and written.
///
/// # Returns
/// - `Ok(())`: If the serialization and writing are successful.
/// - `Err(BinaryError)`: If the slice range is invalid or the size does not match `T::SIZE`.
pub fn write_le<T: LittleEndianInteger>(
    bytes: &mut [u8],
    start_offset: usize,
    value: T,
) -> Result<(), BinaryError> {
    // Is the range we are trying to write into valid?
    let Some(slice) = bytes.get_mut(start_offset..start_offset + T::SIZE) else {
        return Err(BinaryError::BytesSliceSizeMismatch {
            expected: T::SIZE,
            from_offset: start_offset,
        });
    };

    // We can proceed to write the data in the slice
    value.to_le(slice)
}

#[cfg(test)]
mod write_le_test {
    use crate::bin_error::BinaryError;
    use crate::le::write_le;

    #[test]
    fn test_write_le_success_u32() {
        let mut buf = [0u8; 8];
        write_le::<u32>(&mut buf, 2, 23u32).unwrap();
        assert_eq!(&buf[2..6], &[23u8, 0, 0, 0]);
    }

    #[test]
    fn test_write_le_success_u16_u64() {
        let mut buf = [0u8; 12];
        write_le::<u16>(&mut buf, 1, 0x1234u16).unwrap();
        assert_eq!(&buf[1..3], &[0x34u8, 0x12u8]);

        write_le::<u64>(&mut buf, 4, 0x1122334455667788u64).unwrap();
        assert_eq!(&buf[4..12], &0x1122334455667788u64.to_le_bytes());
    }

    #[test]
    fn test_write_le_out_of_bounds() {
        // Attempt to write a u32 into a 3-byte buffer -> should be BytesSliceSizeMismatch
        let mut buf = [0u8; 3];
        let err = write_le::<u32>(&mut buf, 0, 1u32).unwrap_err();
        match err {
            BinaryError::BytesSliceSizeMismatch {
                expected,
                from_offset,
            } => {
                assert_eq!(expected, 4);
                assert_eq!(from_offset, 0);
            }
            _ => panic!("unexpected error variant: {:?}", err),
        }
    }

    #[test]
    fn test_write_le_invalid_target_size_direct() {
        // replaced direct to_le call with write_le to exercise the public API.
        // write_le will return BytesSliceSizeMismatch when the target range is out-of-bounds.
        let val: u16 = 0x1234;
        let mut target = [0u8; 1]; // too small to hold a u16 when writing at offset 0
        let err = write_le::<u16>(&mut target, 0, val).unwrap_err();
        match err {
            BinaryError::BytesSliceSizeMismatch {
                expected,
                from_offset,
            } => {
                assert_eq!(expected, 2); // u16 is 2 bytes
                assert_eq!(from_offset, 0); // attempted from offset 0
            }
            _ => panic!("unexpected error variant: {:?}", err),
        }
    }
}

#[cfg(test)]
mod read_le_tests {
    use super::*;
    use crate::bin_error::BinaryError;

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

    #[test]
    fn test_read_le_out_of_bounds() {
        // Attempt to read a u32 from a 3-byte slice -> should be BytesSliceSizeMismatch
        let bytes = [0u8; 3];
        let err = read_le::<u32>(&bytes, 0).unwrap_err();
        match err {
            BinaryError::BytesSliceSizeMismatch {
                expected,
                from_offset,
            } => {
                assert_eq!(expected, 4);
                assert_eq!(from_offset, 0);
            }
            _ => panic!("unexpected error variant: {:?}", err),
        }
    }

    #[test]
    fn test_read_le_start_offset_out_of_bounds() {
        // 5 bytes total, reading u32 at offset 3 requires bytes 3..7 -> out of bounds
        let bytes = [0u8; 5];
        let err = read_le::<u32>(&bytes, 3).unwrap_err();
        match err {
            BinaryError::BytesSliceSizeMismatch {
                expected,
                from_offset,
            } => {
                assert_eq!(expected, 4);
                assert_eq!(from_offset, 3);
            }
            _ => panic!("unexpected error variant: {:?}", err),
        }
    }

    #[test]
    fn test_read_le_invalid_size_direct() {
        // replaced direct from_le call with read_le to exercise the public API
        let small = [0x01u8, 0x02u8]; // length 2, but u32 expects 4
        let err = read_le::<u32>(&small, 0).unwrap_err();
        match err {
            BinaryError::BytesSliceSizeMismatch {
                expected,
                from_offset,
            } => {
                assert_eq!(expected, 4);
                assert_eq!(from_offset, 0);
            }
            _ => panic!("unexpected error variant: {:?}", err),
        }
    }
}

#[cfg(test)]
mod from_le_tests {
    use super::*;
    use crate::bin_error::BinaryError;

    #[test]
    fn test_from_le_success_u32() {
        let bytes = [23u8, 0, 0, 0];
        let v = <u32 as LittleEndianInteger>::from_le(&bytes).unwrap();
        assert_eq!(v, 23u32);
    }

    #[test]
    fn test_from_le_invalid_size() {
        let small = [1u8, 2u8]; // length 2, but u32 expects 4
        let err = <u32 as LittleEndianInteger>::from_le(&small).unwrap_err();
        match err {
            BinaryError::BytesSliceSizeMismatch {
                expected,
                from_offset,
            } => {
                assert_eq!(expected, 4);
                assert_eq!(from_offset, 0);
            }
            _ => panic!("unexpected error variant: {:?}", err),
        }
    }
}

#[cfg(test)]
mod to_le_tests {
    use super::*;
    use crate::bin_error::BinaryError;

    #[test]
    fn test_to_le_success_u32() {
        let val: u32 = 0x11223344;
        let mut target = [0u8; 4];
        <u32 as LittleEndianInteger>::to_le(val, &mut target).unwrap();
        assert_eq!(target, val.to_le_bytes());
    }

    #[test]
    fn test_to_le_invalid_target_size() {
        let val: u32 = 0x11223344;
        let mut target = [0u8; 3]; // too small
        let err = <u32 as LittleEndianInteger>::to_le(val, &mut target).unwrap_err();
        match err {
            BinaryError::WriteErrorSliceSizeMismatch { src, target } => {
                assert_eq!(src, 4);
                assert_eq!(target, 3);
            }
            _ => panic!("unexpected error variant: {:?}", err),
        }
    }
}
