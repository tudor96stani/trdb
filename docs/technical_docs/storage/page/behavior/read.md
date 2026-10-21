---
created: 2026-10-10
---

Reading a row from a slotted page means following its slot to the stored byte range. `Page::row(slot_index)` returns a slice borrowing that range, so this is a zero-copy read. Interpreting those bytes as column values is a separate concern from locating them.

## process

1. We read `free_end` and `slot_count` and construct a slot-array view over bytes `free_end + 1..4096`.
2. We select the zero-based slot, counting from the end of that slice, and read its little-endian offset and length.
3. We return `&data[offset..offset + length]`. The offset is absolute within the page.

The returned slice cannot outlive the page borrow. For a buffered page, the read/write guard must stay alive while we use the slice. Rust's borrow rules prevent mutating that page through the same borrow while the slice is in use.

## Invalid and out-of-range slots

An out-of-range slot index returns `SlotError::InvalidSlot`, wrapped by the read operation and page ID. Inconsistent slot-region size returns a slot-region error.

A deleted slot `(0, 0)` returns the empty slice `data[0..0]`. A zero-length slot at another in-bounds offset also reads as empty.

## See also

- [Layout](../layout.md)
- [Deletion](deletion.md)
