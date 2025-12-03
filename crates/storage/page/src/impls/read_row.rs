use crate::errors::read_row_error::ReadRowError;
use crate::impls::Page;

/// Internal row access methods for the `Page` struct.
impl Page {
    /// Retrieves a row by its slot index.
    /// Returns a slice of bytes representing the row data.
    pub(super) fn read_row_internal(&self, slot_index: u32) -> Result<&[u8], ReadRowError> {
        let slot = self.slot_array()?.slot_ref(slot_index)?;

        let (offset, length) = (slot.offset()? as usize, slot.length()? as usize);

        Ok(&self.data[offset..offset + length])
    }
}
