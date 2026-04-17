use crate::page::errors::ReadRowError;
use crate::page::layout::page::Page;

/// Internal row access methods for the `Page` struct.
impl Page {
    /// Retrieves a row by its slot index.
    /// Returns a slice of bytes representing the row data.
    pub fn read_row_internal(&self, slot_index: u32) -> Result<&[u8], ReadRowError> {
        let slot = self.slot_array_ref()?.slot_ref(slot_index)?;

        let (offset, length) = (slot.offset()? as usize, slot.length()? as usize);

        Ok(&self.data[offset..offset + length])
    }
}
