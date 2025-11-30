//! Slot Array Handling for Slotted Pages
//! -------------------------------------
//!
//! This module provides zero-copy immutable and mutable views
//! (`SlotArrayRef` and `SlotArrayMut`) into the *slot array region* of a slotted
//! database page. The slot array is a compact structure containing fixed-size
//! slots, each describing the offset and length of a tuple stored
//! elsewhere on the page.
//!
//! # Reverse (Right-to-Left) Slot Ordering
//!
//! The slot array grows **inward from the end of the page** toward the beginning.
//! This design avoids shifting large memory regions whenever a new slot is
//! added: new slots simply extend into free space from the *right side*.
//!
//! Consequently, **logical slot index 0 corresponds to the *last* SLOT_SIZE
//! chunk in the slot array slice**, and slot indices increase leftwards.
//!
//! In other words, if the slot array byte region is `[start .. end)`,
//! then the in-memory layout is reversed:
//!
//! ```text
//!   Lower memory addresses                             Higher addresses
//!   ┌──────────────────────────────────────────────────────────────────┐
//!   │                           Slot Array                             │
//!   └──────────────────────────────────────────────────────────────────┘
//!
//!   [slot_count - 1]   [slot_count - 2]          ...          [1]   [0]
//!        ┌───────┐         ┌───────┐                            ┌───────┐
//!        │ SlotN │         │ SlotN │                            │ Slot0 │
//!        └───────┘         └───────┘                            └───────┘
//!
//!        ^ increasing slot_index →
//!
//! Mapping formula:
//!
//!     physical_start = slot_array_len - (slot_index + 1) * SLOT_SIZE
//!     physical_end   = physical_start + SLOT_SIZE
//!
//! Each slot is therefore addressed relative to the *end* of the region.
//!
//! # Zero-Copy Access
//!
//! These types never own memory. They merely borrow a slice of the page:
//!
//! - `SlotArrayRef<'a>`: borrows `&'a [u8]`
//! - `SlotArrayMut<'a>`: borrows `&'a mut [u8]`
//!
//! Slot access returns lightweight zero-copy views:
//!
//! - `SlotRef<'a>` for reading a slot
//! - `SlotMut<'a>` for modifying it
//!
//! The module performs strict length and bounds verification at construction,
//! ensuring the provided byte region is exactly `slot_count * SLOT_SIZE`.

use crate::PAGE_SIZE;
use crate::errors::slot_error::SlotError;
use crate::slot::{SLOT_SIZE, SlotMut, SlotRef};
use std::ops::Range;

/// Immutable zero-copy view into the slot array of a slotted page.
#[derive(Debug)]
pub(crate) struct SlotArrayRef<'a> {
    /// View into the raw bytes of the slot array.
    /// Each slot is of length `SLOT_SIZE` bytes.
    bytes: &'a [u8],
}

impl<'a> SlotArrayRef<'a> {
    /// Creates a new SlotArrayRef.
    /// Validates that the slice length matches the expected slot count.
    pub(super) fn new(bytes: &'a [u8], slot_count: u16) -> Result<Self, SlotError> {
        let expected_len = slot_count as usize * SLOT_SIZE;

        if bytes.len() != expected_len {
            return Err(SlotError::SlotRegionSizeMismatch {
                expected_size: expected_len,
                actual_size: bytes.len(),
            });
        }

        Ok(Self { bytes })
    }

    /// Gets an immutable view of the slot at the given index.
    /// Slots are zero-indexed.
    /// Returns an error if the slot index is out of bounds.
    pub(crate) fn slot_ref(&self, slot_index: u32) -> Result<SlotRef<'a>, SlotError> {
        let range = get_slot_range(self.bytes.len(), slot_index)?;

        let slot_bytes = self.bytes.get(range).ok_or(SlotError::InvalidSlot {
            slot_index: slot_index as usize,
        })?;

        SlotRef::from_raw(slot_index, slot_bytes)
    }
}

/// Mutable zero-copy view into the slot array of a slotted page.
#[derive(Debug)]
pub(crate) struct SlotArrayMut<'a> {
    /// View into the raw bytes of the slot array.
    /// Each slot is of length `SLOT_SIZE` bytes.
    bytes: &'a mut [u8],
}

impl<'a> SlotArrayMut<'a> {
    /// Creates a new SlotArrayMut.
    /// Validates that the slice length matches the expected slot count.
    pub(super) fn new(bytes: &'a mut [u8], slot_count: u16) -> Result<Self, SlotError> {
        let expected_len = slot_count as usize * SLOT_SIZE;

        if bytes.len() != expected_len {
            return Err(SlotError::SlotRegionSizeMismatch {
                expected_size: expected_len,
                actual_size: bytes.len(),
            });
        }

        Ok(Self { bytes })
    }

    /// Gets an immutable view of the slot at the given index
    /// Slots are zero-indexed
    /// Returns an error if the slot index is out of bounds
    pub(crate) fn slot_ref(&self, slot_index: u32) -> Result<SlotRef<'_>, SlotError> {
        let range = get_slot_range(self.bytes.len(), slot_index)?;

        let slot_bytes = self.bytes.get(range).ok_or(SlotError::InvalidSlot {
            slot_index: slot_index as usize,
        })?;

        SlotRef::from_raw(slot_index, slot_bytes)
    }

    /// Gets a mutable view of the slot at the given index.
    /// Slots are zero-indexed.
    /// Returns an error if the slot index is out of bounds.
    pub(crate) fn slot_mut(&mut self, slot_index: u32) -> Result<SlotMut<'_>, SlotError> {
        let range = get_slot_range(self.bytes.len(), slot_index)?;

        let slot_bytes = self.bytes.get_mut(range).ok_or(SlotError::InvalidSlot {
            slot_index: slot_index as usize,
        })?;

        SlotMut::from_raw(slot_index, slot_bytes)
    }

    /// Sets the values of a slot in the slot array.
    pub(crate) fn set_slot(
        &mut self,
        slot_index: u32,
        slot_offset: u16,
        slot_length: u16,
    ) -> Result<(), SlotError> {
        let mut slot_mut = self.slot_mut(slot_index)?;
        slot_mut.set_length(slot_length)?;
        slot_mut.set_offset(slot_offset)?;
        Ok(())
    }
}

fn get_slot_start(slot_array_size: usize, slot_index: u32) -> Result<usize, SlotError> {
    slot_array_size
        .checked_sub((slot_index as usize + 1) * SLOT_SIZE)
        .ok_or(SlotError::InvalidSlot {
            slot_index: slot_index as usize,
        })
}

fn get_slot_range(slot_array_size: usize, slot_index: u32) -> Result<Range<usize>, SlotError> {
    let start = get_slot_start(slot_array_size, slot_index)?;
    Ok(start..(start + SLOT_SIZE))
}

#[cfg(test)]
mod slot_array_ref_test {
    use super::*;

    #[test]
    fn slot_array_ref_new_invalid_size() {
        let bytes = vec![0u8; 10]; // Not a multiple of SLOT_SIZE
        let result = SlotArrayRef::new(&bytes, 3); // Expecting 3 slots (12 bytes)
        assert!(matches!(
            result,
            Err(SlotError::SlotRegionSizeMismatch {
                expected_size: 12,
                actual_size: 10
            })
        ));
    }

    #[test]
    fn slot_array_ref_new_valid() {
        let bytes = vec![0u8; SLOT_SIZE * 2]; // 2 slots
        let result = SlotArrayRef::new(&bytes, 2);
        assert!(result.is_ok());
    }

    #[test]
    fn slot_array_ref_slot_ref_invalid_index() {
        let bytes = vec![0u8; SLOT_SIZE * 2]; // 2 slots
        let slot_array = SlotArrayRef::new(&bytes, 2).unwrap();
        let result = slot_array.slot_ref(3); // Invalid index
        assert!(matches!(
            result,
            Err(SlotError::InvalidSlot { slot_index: 3 })
        ));
    }

    #[test]
    fn slot_aray_ref_slot_ref_valid() {
        let mut bytes = vec![0u8; SLOT_SIZE * 2]; // 2 slots
        // Initialize first slot
        bytes[4..8].copy_from_slice(&[1, 0, 2, 0]); // offset=1, length=2
        // Initialize second slot
        bytes[0..4].copy_from_slice(&[3, 0, 4, 0]); // offset=3, length=4

        let slot_array = SlotArrayRef::new(&bytes, 2).unwrap();

        let slot0 = slot_array.slot_ref(0).unwrap();
        assert_eq!(slot0.offset().unwrap(), 1);
        assert_eq!(slot0.length().unwrap(), 2);

        let slot1 = slot_array.slot_ref(1).unwrap();
        assert_eq!(slot1.offset().unwrap(), 3);
        assert_eq!(slot1.length().unwrap(), 4);
    }
}

#[cfg(test)]
mod slot_array_mut_test {
    use super::*;

    #[test]
    fn slot_array_mut_new_invalid_size() {
        let mut bytes = vec![0u8; 10]; // Not a multiple of SLOT_SIZE
        let result = SlotArrayMut::new(&mut bytes, 3); // Expecting 3 slots (12 bytes)
        assert!(matches!(
            result,
            Err(SlotError::SlotRegionSizeMismatch {
                expected_size: 12,
                actual_size: 10
            })
        ));
    }

    #[test]
    fn slot_array_mut_new_valid() {
        let mut bytes = vec![0u8; SLOT_SIZE * 2]; // 2 slots
        let result = SlotArrayMut::new(&mut bytes, 2);
        assert!(result.is_ok());
    }

    #[test]
    fn slot_array_mut_slot_mut_invalid_index() {
        let mut bytes = vec![0u8; SLOT_SIZE * 2]; // 2 slots
        let mut slot_array = SlotArrayMut::new(&mut bytes, 2).unwrap();
        let result = slot_array.slot_mut(3); // Invalid index
        assert!(matches!(
            result,
            Err(SlotError::InvalidSlot { slot_index: 3 })
        ));
    }

    #[test]
    fn slot_array_mut_slot_mut_valid() {
        let mut bytes = vec![0u8; SLOT_SIZE * 2]; // 2 slots
        // Initialize first slot
        bytes[4..8].copy_from_slice(&[1, 0, 2, 0]); // offset=1, length=2
        // Initialize second slot
        bytes[0..4].copy_from_slice(&[3, 0, 4, 0]); // offset=3, length=4

        let mut slot_array = SlotArrayMut::new(&mut bytes, 2).unwrap();

        let mut slot0 = slot_array.slot_mut(0).unwrap();
        assert_eq!(slot0.offset().unwrap(), 1);
        assert_eq!(slot0.length().unwrap(), 2);

        let mut slot1 = slot_array.slot_mut(1).unwrap();
        assert_eq!(slot1.offset().unwrap(), 3);
        assert_eq!(slot1.length().unwrap(), 4);
    }

    #[test]
    fn slot_array_mut_slot_ref_invalid_index() {
        let mut bytes = vec![0u8; SLOT_SIZE * 2]; // 2 slots
        let mut slot_array = SlotArrayMut::new(&mut bytes, 2).unwrap();
        let result = slot_array.slot_ref(3); // Invalid index
        assert!(matches!(
            result,
            Err(SlotError::InvalidSlot { slot_index: 3 })
        ));
    }

    #[test]
    fn slot_aray_mut_slot_ref_valid() {
        let mut bytes = vec![0u8; SLOT_SIZE * 2]; // 2 slots
        // Initialize first slot
        bytes[4..8].copy_from_slice(&[1, 0, 2, 0]); // offset=1, length=2
        // Initialize second slot
        bytes[0..4].copy_from_slice(&[3, 0, 4, 0]); // offset=3, length=4

        let mut slot_array = SlotArrayMut::new(&mut bytes, 2).unwrap();

        let slot0 = slot_array.slot_ref(0).unwrap();
        assert_eq!(slot0.offset().unwrap(), 1);
        assert_eq!(slot0.length().unwrap(), 2);

        let slot1 = slot_array.slot_ref(1).unwrap();
        assert_eq!(slot1.offset().unwrap(), 3);
        assert_eq!(slot1.length().unwrap(), 4);
    }
}
