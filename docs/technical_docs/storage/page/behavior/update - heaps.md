---
created: 2026-01-21
---

Updating a heap row replaces its bytes while preserving its logical slot index. Its physical offset can change, so callers follow the same slot to find the replacement.

`update_row(slot_index, row)` overwrites equal or smaller rows in place and uses the insertion placement search for larger rows. We can count the old row's bytes as available because the replacement does not need another slot.

## Checks

1. We read `free_space` and find the old slot. An out-of-range or invalidated slot returns `InvalidSlot`; checked index conversion can also fail.
2. We read the old row's offset and length.
3. We require `free_space + old_length >= new_length`. Otherwise, we return `UpdateError::NotEnoughSpace`, with the available count including the old row.

After these checks, we handle equal, smaller and larger rows separately.

## Equal or smaller rows

We write the replacement at the old starting offset. An equal-size write needs no header or slot changes.

For a smaller row, we update the slot length and set `free_space = old_free_space + old_length - new_length`. The offset and `free_start` stay unchanged, leaving the unused suffix available as a gap.

```text
before: slots [(96, 100), (196, 100)], free_start = 296
shrink slot 0 to 40 bytes
after:  slots [(96, 40),  (196, 100)], free_start = 296
gap:    136..196 = 60 bytes
```

`free_space` grows by 60, and both slots remain live. Shrinking the last physical row also leaves `free_start` unchanged. Later insertion or growth can use gaps or compact the page.

## Larger rows

1. We call `find_insertion_offset(new_length, Some(old_slot_index))`.
2. The search first tries the append region. If it scans rows, it treats the old row's end as its start, allowing the old bytes to contribute to a gap.
3. For `Exact(offset)`, we use that location. For `AfterCompactionFreeStart`, we delete the old row with compaction requested, then use the resulting `free_start`. Deleting first prevents compaction from copying bytes we are about to replace.
4. We check that the destination end fits in the page array, then copy the replacement bytes.
5. We set the old slot's offset and length to the replacement's values.
6. We set `free_space = old_free_space + old_length - new_length`, using the value saved before any temporary deletion.
7. We advance `free_start` only if the destination was that pointer, including after compaction.

The row keeps the same slot even on the temporary-deletion path. `slot_count` and `free_end` stay unchanged. Relocation can leave the old byte range unused because append placement is tried before gap placement.

For example, when the append region cannot fit the replacement, an old 100-byte row followed by a 50-byte gap gives the search a 150-byte range. Treating the old bytes as available lets a 150-byte replacement occupy that range without needing another slot.

## Errors

Header, slot, insertion, deletion and conversion errors propagate through `UpdateError` and the public page-ID wrapper.

The replacement is placed on the same page. If it does not fit, we return a space error.

## See also

- [Heap insertion](insertion%20-%20heaps.md)
- [Compaction](page%20compaction.md)
