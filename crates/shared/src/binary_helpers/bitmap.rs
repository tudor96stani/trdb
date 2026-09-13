use crate::binary_helpers::bin_error::BinaryError;
use std::array::TryFromSliceError;

/// A wrapper type around the concept of the null bitmap, backed by a `u64`.
#[derive(Debug)]
pub struct Bitmap {
    value: u64,
}

impl Default for Bitmap {
    fn default() -> Self {
        Self::new()
    }
}

impl Bitmap {
    // region CONSTRUCTORS
    /// Create an empty bitmap
    fn new() -> Self {
        Self { value: 0 }
    }

    /// Creates a new `Bitmap` from a provided `u64`
    pub fn from_u64(value: u64) -> Self {
        Self { value }
    }

    /// Creates a new `Bitmap` from a provided byte slice
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BinaryError> {
        let converted = u64::from_le_bytes(bytes.try_into()?);
        Ok(Self::from_u64(converted))
    }
    // endregion

    // region GETTERS

    /// Returns the bitmap as a byte array
    pub fn to_bytes(&self) -> Result<Vec<u8>, BinaryError> {
        let converted = u64::to_le_bytes(self.value);
        Ok(converted.to_vec())
    }

    // endregion

    /// Toggles the flag found at the provided index. Modification done in-place
    /// ## Parameters
    /// - `index`: the 0 indexed position of the flag to be flipped.
    pub fn toggle(&mut self, index: usize) {
        let mask = 1 << index;
        self.value ^= mask;
    }

    /// Sets the flag from the provided position to "enabled". Modification done in place.
    /// ## Parameters
    /// - `index`: the 0 indexed position of the flag to be enabled.
    pub fn set(&mut self, index: usize) {
        let mask = 1 << index;
        self.value |= mask;
    }

    /// Sets the flag from the provided position to "disabled". Modification done in place.
    /// ## Parameters
    /// - `index`: the 0 indexed position of the flag to be disabled.
    pub fn unset(&mut self, index: usize) {
        let mask = 1 << index;
        let negated_mask = !mask;
        self.value &= negated_mask;
    }

    /// Checks whether the value of the flag at the provided position is enabled or not.
    /// ## Parameters
    /// - `index`: the 0 indexed position of the flag to be disabled.
    ///
    /// ## Returns
    /// - `true` if the flag is set, `false` if not set
    pub fn check(&self, index: usize) -> bool {
        let mask = 1 << index;
        (self.value & mask) == mask
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn toggle_first_correctly_flips_flag() {
        let mut bitmap = Bitmap::new();
        bitmap.toggle(0);
        assert_eq!(bitmap.value, 1);
        bitmap.toggle(0);
        assert_eq!(bitmap.value, 0);
    }

    #[test]
    pub fn toggle_leaves_others_unaffected() {
        let mut bitmap = Bitmap::from_u64(5u64);
        bitmap.toggle(1);
        assert_eq!(bitmap.value, 7u64);
    }
}
