use crate::errors::slot_error::SlotError;
use binary_helpers::le::{read_le, write_le};

/// Fixed-size of a slot in bytes.
pub(crate) const SLOT_SIZE: usize = 4;

/// A zero-copy read-only view into a 4-byte slot (offset: u16, length: u16) in the byte array of the slotted page.
#[derive(Debug)]
pub(crate) struct SlotRef<'a> {
    /// Logical index of the slot within the page.
    slot_index: u32,
    /// View into the raw bytes of the slot.
    /// # Format
    /// | Offset | Length |
    /// |--------|--------|
    /// | 0      | 2      |
    ///
    /// Both fields are stored as u16 in little-endian format.
    /// - Offset: The starting byte position of the record within the page.
    /// - Length: The size of the record in bytes.
    bytes: &'a [u8; SLOT_SIZE],
}

impl<'a> SlotRef<'a> {
    /// Create a new SlotRef from raw bytes and slot number.
    /// If the byte slice is not exactly 4 bytes, it returns a SizeMismatchError.
    /// Otherwise, return Ok(SlotRef).
    #[inline]
    pub(super) fn from_raw(slot_index: u32, bytes: &'a [u8]) -> Result<Self, SlotError> {
        if bytes.len() != SLOT_SIZE {
            return Err(SlotError::SlotSizeMismatch {
                expected_size: SLOT_SIZE,
                actual_size: bytes.len(),
            });
        }

        // This conversion is now infallible after the length check.
        let bytes_array: &[u8; SLOT_SIZE] = match bytes.try_into() {
            Ok(arr) => arr,
            Err(_) => unreachable!("bytes.len() == SLOT_SIZE but try_into() failed"),
        };

        Ok(Self {
            slot_index,
            bytes: bytes_array,
        })
    }

    /// Get the logical slot index.
    #[inline]
    pub(crate) fn slot_index(&self) -> u32 {
        self.slot_index
    }

    /// Read the offset (u16) from the slot (little-endian).
    #[inline]
    pub(crate) fn offset(&self) -> Result<u16, SlotError> {
        Ok(read_le::<u16>(self.bytes, 0)?)
    }

    /// Read the length (u16) from the slot (little-endian).
    #[inline]
    pub(crate) fn length(&self) -> Result<u16, SlotError> {
        Ok(read_le::<u16>(self.bytes, 2)?)
    }
}

/// A zero-copy mutable view into a 4-byte slot (offset: u16, length: u16) in the byte array of the slotted page.
#[derive(Debug)]
pub(crate) struct SlotMut<'a> {
    /// Logical index of the slot within the page.
    slot_index: u32,
    /// Mutable view into the raw bytes of the slot.
    /// # Format
    /// | Offset | Length |
    /// |--------|--------|
    /// | 0      | 2      |
    ///
    /// Both fields are stored as u16 in little-endian format.
    /// - Offset: The starting byte position of the record within the page.
    /// - Length: The size of the record in bytes.
    bytes: &'a mut [u8; SLOT_SIZE],
}

impl<'a> SlotMut<'a> {
    /// Create a new SlotMut from raw bytes and slot number.
    /// If the byte slice is not exactly 4 bytes, it returns a SizeMismatchError.
    /// Otherwise, return Ok(SlotMut).
    #[inline]
    pub(super) fn from_raw(slot_index: u32, bytes: &'a mut [u8]) -> Result<Self, SlotError> {
        if bytes.len() != SLOT_SIZE {
            return Err(SlotError::SlotSizeMismatch {
                expected_size: SLOT_SIZE,
                actual_size: bytes.len(),
            });
        }

        // After checking length, this should never fail; treat failure as unreachable.
        let bytes_array: &mut [u8; SLOT_SIZE] = match bytes.try_into() {
            Ok(arr) => arr,
            Err(_) => unreachable!("bytes.len() == SLOT_SIZE but try_into() failed"),
        };

        Ok(Self {
            slot_index,
            bytes: bytes_array,
        })
    }

    /// Read the offset (u16) from the slot (little-endian).
    #[inline]
    pub(crate) fn offset(&self) -> Result<u16, SlotError> {
        Ok(read_le::<u16>(self.bytes, 0)?)
    }

    /// Read the length (u16) from the slot (little-endian).
    #[inline]
    pub(crate) fn length(&self) -> Result<u16, SlotError> {
        Ok(read_le::<u16>(self.bytes, 2)?)
    }

    /// Write the offset (u16) into the slot (little-endian).
    #[inline]
    pub(crate) fn set_offset(&mut self, offset: u16) -> Result<(), SlotError> {
        Ok(write_le::<u16>(self.bytes, 0, offset)?)
    }

    /// Write the length (u16) into the slot (little-endian).
    #[inline]
    pub(crate) fn set_length(&mut self, length: u16) -> Result<(), SlotError> {
        Ok(write_le::<u16>(self.bytes, 2, length)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use binary_helpers::bin_error::BinaryError;
    use std::fmt::Write as _; // for format tests if needed

    #[test]
    fn slot_size_constant_is_four() {
        assert_eq!(SLOT_SIZE, 4);
    }

    #[test]
    fn slot_ref_from_raw_with_valid_bytes_reads_fields_correctly() {
        let bytes = [0x01, 0x00, 0x02, 0x00];
        let slot = SlotRef::from_raw(5, &bytes).unwrap();
        assert_eq!(slot.slot_index(), 5);
        assert_eq!(slot.offset().unwrap(), 1);
        assert_eq!(slot.length().unwrap(), 2);
    }

    #[test]
    fn slot_ref_from_raw_with_invalid_size_returns_size_mismatch() {
        let bytes = [0x01, 0x02, 0x03];
        let res = SlotRef::from_raw(1, &bytes);
        assert!(matches!(
            res,
            Err(SlotError::SlotSizeMismatch {
                expected_size: 4,
                actual_size: 3
            })
        ));
    }

    #[test]
    fn slot_ref_from_raw_with_too_large_slice_returns_size_mismatch() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x00];
        let res = SlotRef::from_raw(2, &bytes);
        assert!(matches!(
            res,
            Err(SlotError::SlotSizeMismatch {
                expected_size: 4,
                actual_size: 5
            })
        ));
    }

    #[test]
    fn slot_ref_reads_zero_values() {
        let bytes = [0x00, 0x00, 0x00, 0x00];
        let slot = SlotRef::from_raw(0, &bytes).unwrap();
        assert_eq!(slot.offset().unwrap(), 0);
        assert_eq!(slot.length().unwrap(), 0);
    }

    #[test]
    fn slot_mut_from_raw_with_valid_bytes_reads_fields_correctly() {
        let mut bytes = [0x34, 0x12, 0x78, 0x56];
        {
            let slot = SlotMut::from_raw(3, &mut bytes).unwrap();
            // can access private field inside module tests
            assert_eq!(slot.slot_index, 3);
            assert_eq!(slot.offset().unwrap(), 0x1234);
            assert_eq!(slot.length().unwrap(), 0x5678);
        }
    }

    #[test]
    fn slot_mut_from_raw_with_invalid_size_returns_size_mismatch() {
        let mut bytes = [0x01, 0x02, 0x03];
        let res = SlotMut::from_raw(1, &mut bytes);
        assert!(matches!(
            res,
            Err(SlotError::SlotSizeMismatch {
                expected_size: 4,
                actual_size: 3
            })
        ));
    }

    #[test]
    fn slot_mut_set_offset_and_length_updates_underlying_bytes_and_reads_back() {
        let mut bytes = [0x00, 0x00, 0x00, 0x00];
        {
            let mut slot = SlotMut::from_raw(1, &mut bytes).unwrap();
            slot.set_offset(0x0102).unwrap();
            slot.set_length(0x0304).unwrap();
            assert_eq!(slot.offset().unwrap(), 0x0102);
            assert_eq!(slot.length().unwrap(), 0x0304);
        }
        assert_eq!(bytes, [0x02, 0x01, 0x04, 0x03]);
    }

    #[test]
    fn slot_mut_write_and_read_max_u16_values() {
        let mut bytes = [0x00, 0x00, 0x00, 0x00];
        {
            let mut slot = SlotMut::from_raw(7, &mut bytes).unwrap();
            slot.set_offset(u16::MAX).unwrap();
            slot.set_length(u16::MAX).unwrap();
            assert_eq!(slot.offset().unwrap(), u16::MAX);
            assert_eq!(slot.length().unwrap(), u16::MAX);
        }
        assert_eq!(bytes, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn debug_formatting_for_slotref_and_slotmut() {
        // SlotRef debug
        let bytes_ref = [0x01, 0x02, 0x03, 0x04];
        let slot_ref = SlotRef::from_raw(9, &bytes_ref).unwrap();
        let _s = format!("{:?}", slot_ref); // exercise Debug impl

        // SlotMut debug (scope to avoid borrow conflicts)
        let mut bytes_mut = [0x05, 0x06, 0x07, 0x08];
        {
            let slot_mut = SlotMut::from_raw(11, &mut bytes_mut).unwrap();
            let _s2 = format!("{:?}", slot_mut);
        }
    }
}
