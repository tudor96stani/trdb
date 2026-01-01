use crate::errors::delete_error::DeleteError;
use crate::impls::Page;

impl Page {
    pub(super) fn delete_row_internal(
        &mut self,
        slot_index: usize,
        compact_requested: bool,
    ) -> Result<(), DeleteError> {
        let mut slot_array = self.slot_array_mut()?;
        let mut slot = slot_array.slot_mut(slot_index as u32)?;
        let row_size = slot.length()?;

        slot.set_length(0)?;
        slot.set_offset(0)?;

        let mut header = self.header_mut()?;
        header.set_can_compact(1)?;
        let current_free_space = header.get_free_space()?;
        let new_free_space = current_free_space + row_size;

        header.set_free_space(new_free_space)?;

        if (compact_requested) {
            self.compact()?;
        }

        Ok(())
    }
}
