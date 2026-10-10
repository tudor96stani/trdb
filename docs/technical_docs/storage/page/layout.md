---
created: 2026-04-22
---

A slotted page separates a row's logical identity from its physical location. The slot index identifies the row within the page; the slot's offset and length tell us where its bytes currently sit. This means we can move a row without changing the index callers use to find it.

We implement this in a fixed 4096-byte array with a metadata header, row bytes and a slot array. Borrowed views read and update those regions directly.

## Layout

We use four regions. Offsets increase from left to right here:

```text
0         96                  free_start       free_end + 1    4096
| header | rows and gaps     | append space   | slot array    |
          rows grow right ->                   <- slots grow left
```

- The header occupies bytes `0..96` and stores the page's metadata.
- Each row occupies one contiguous range after the header. Rows can have gaps between them.
- `free_start` marks where the next append can begin. Free bytes can also exist in gaps before it.
- Slots grow backwards from the end of the page, four bytes at a time.

All ranges in this document are half-open unless stated otherwise. A row at offset 96 with length 100 occupies `96..196`.

## Header

All header numbers use little-endian encoding. These are absolute offsets within the page:

| Field | Offset | Type / width | Initial value |
| --- | --- | --- | --- |
| `slot_count` | 0 | `u16`, 2 bytes | 0 |
| `free_start` | 2 | `u16`, 2 bytes | 96 |
| `free_end` | 4 | `u16`, 2 bytes | 4095 |
| `free_space` | 6 | `u16`, 2 bytes | 4000 |
| `can_compact` | 8 | `u16`, 2 bytes | 0 |
| `page_number` | 10 | `u32`, 4 bytes | supplied page number |
| `page_type` | 14 | `u16`, 2 bytes | supplied type (1–4) |
| `left_page` | 16 | `u32`, 4 bytes | 0 |
| `right_page` | 20 | `u32`, 4 bytes | 0 |
| `last_lsn` | 26 | `u64`, 8 bytes | 0 |

`initialize()` clears the whole page before setting the nonzero values. `HeaderMut::default()` by itself only sets the page number, page type, `free_start`, `free_end` and `free_space`; it does not clear the other fields.

Bytes `24..26` and `34..96` are unused. `left_page` and `right_page` reserve links to neighboring pages. `last_lsn` reserves a log sequence number, which identifies a log record. Tree links and write-ahead logging (WAL) are not implemented. The `last_lsn` offset is retained from the earlier format.

### Free space

`free_end` is the last byte before the slots, so it is inclusive. With one slot, `free_end = 4091` and the slot occupies `4092..4096`.

Fragmentation means free bytes are split into gaps between live rows. `free_space` counts all unused bytes, including those gaps; the append region is just the contiguous space after `free_start`. Shrinking a row can leave `free_start` beyond that row's new end without changing the pointer.

The append placement check compares `free_end - free_start` with the row length; see [heap insertion](behavior/insertion%20-%20heaps.md).

### Header views

`HeaderRef` and `HeaderMut` borrow exactly 96 bytes. A different slice size returns `HeaderSliceSizeMismatch`.

These are zero-copy views: they borrow a slice instead of allocating a separate header object. Getters and setters use the little-endian helpers. A mutable view writes directly into the page.

## Slots

A slot is four bytes:

```text
relative byte:  0        2        4
               | offset | length |
                 u16      u16
```

Both fields are little-endian. The row offset is absolute within the page. A live slot has a nonzero offset and length; deletion zeroes both fields without clearing the row bytes.

Slots are numbered from the page's end. Slot 0 occupies `4092..4096`, slot 1 occupies `4088..4092`, and so on. For a slot-array slice of length `N`, slot `i` occupies `N - (i + 1) * 4 .. N - i * 4` within that slice.

Growing backwards lets us add a slot before the existing array without moving its entries. Row bytes grow from the other end, so both regions share the free space between them.

Logical slot order can differ from physical row order. A surviving heap row keeps its slot index across updates and compaction. Once deleted, its slot can be reused for another row, so the old index is no longer that row's identity.

### Slot views

`SlotRef` and `SlotMut` require exactly four bytes or return `SlotSizeMismatch`. The array views require exactly `slot_count * 4` bytes or return `SlotRegionSizeMismatch`; an out-of-range index returns `InvalidSlot`.

The views borrow the page bytes, and mutable views change those bytes in place.

## See also

- [Page construction and types](page.md)
- [Heap insertion](behavior/insertion%20-%20heaps.md)
- [Compaction](behavior/page%20compaction.md)
