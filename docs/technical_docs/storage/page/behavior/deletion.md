---
created: 2026-01-04
---

Heap deletion invalidates the row's slot rather than shifting the other slots or erasing the payload. This is lazy deletion: the row stops being live immediately, while its old bytes become available for reuse.

`delete_row()` zeroes the slot's offset and length and adds the row's length to the free-space count. We keep the slot in the array so other rows retain their indices. If the deleted row was physically last, we can also move the append pointer backwards; otherwise the freed bytes form a gap.

## process

1. We find the requested slot. An out-of-range index or a slot with a zero offset or length returns `SlotError::InvalidSlot`.
2. We scan valid slots to see whether deleting this row lets us move `free_start` backwards.
3. We set the deleted slot's offset and length to zero.
4. We add the old row length to `free_space`. The slot's four bytes remain allocated.
5. If the scan found a new append pointer, we set `free_start` to it. Otherwise, we set `can_compact = 1`.
6. If `compact_requested` is true, we compact the remaining rows.

`delete_row()` wraps errors with the page ID. Neither the last-row case nor compaction clears an existing `can_compact` value.

## Slot invalidation

Deleting slot 1 in this page gives us:

```text
before: [(96, 100), (196, 50), (246, 50)]
after:  [(96, 100), (0, 0),    (246, 50)]
slot_count = 3 in both states
```

Slot 2 still identifies the same row. `slot_count` stays at 3 because reducing it would make that slot unreachable. The next insertion can reuse slot 1.

Reading the zeroed slot returns `data[0..0]`, an empty slice. The payload at `196..246` remains in the page.

## Deleting the last physical row

If we delete the row with the greatest starting offset, we can move `free_start` to the end of the next row. This also recovers any gap between those rows as append space.

We scan valid slots and keep the two greatest starting offsets with their lengths. If the deleted slot has the greatest start, the new pointer is `second_start + second_length`. If it was the only live row, the pointer becomes `HEADER_SIZE = 96`.

```text
rows: [(96, 104), (200, 100), (400, 100)]
gap:  300..400
free_start = 500

delete the row at 400
new_free_start = 200 + 100 = 300
```

The rows and their lengths determine the new pointer, so we also recover the gap instead of just subtracting the deleted row's length from the old pointer.

![Before deleting the last row](../../../../attachments/avoid%20fragmentation%20during%20deletion%20from%20slotted%20pages%20-%20before.png)

![After deleting the last row](../../../../attachments/avoid%20fragmentation%20during%20deletion%20from%20slotted%20pages%20-%20after.png)

The scan uses each row's starting offset to determine physical order.

## Compaction

Deleting an interior row leaves a gap and sets `can_compact`. We can leave that gap for a later insertion, or request [compaction](page%20compaction.md) immediately. Compaction costs a scan and copies the surviving rows, so deletion does not always do it.

## See also

- [Heap insertion](insertion%20-%20heaps.md)
- [Row reads](read.md)
