---
created: 2026-01-04
---

Compaction combines scattered free space into one contiguous region. Deleting or shrinking rows can leave gaps, so a page may have enough free bytes in total without having a large enough range for the next row.

We pack live rows together after the 96-byte header and update their slots to the new offsets. Slot indices stay the same, so the rows keep their logical identities. Insertion and deletion call the internal `compact()` method when they need it; updates use it through deletion.

## process

1. We read `free_end` and allocate a zeroed temporary buffer of `free_end - HEADER_SIZE` bytes.
2. We go through slots in index order, skipping those with a zero offset or length.
3. For each live slot, we copy its row into the temporary buffer at `write_head`. We set the slot's page-relative offset to `HEADER_SIZE + write_head`, keep its length and advance `write_head`.
4. We copy the used temporary bytes back into the page, starting at byte 96.
5. We set `free_start = HEADER_SIZE + write_head`.

The temporary buffer keeps copying one row from overwriting another row's source bytes. We need it because slot order can differ from physical row order.

## Example

```text
before: slots [(296, 50), (0, 0), (96, 100)]
after:  slots [(96, 50),  (0, 0), (146, 100)]
free_start = 96 + 50 + 100 = 246
```

We copy slot 0 first, even though its bytes were physically after slot 2. Slot 1 stays invalid. All offsets above are absolute within the page.

## What stays unchanged

Compaction keeps slot indices, `slot_count`, `free_end` and `free_space`. We move the bytes without changing the amount of row or slot data. With no live rows, `free_start` becomes 96.

Trailing bytes and `can_compact` stay unchanged. The used row bytes are packed before the new append pointer.

## See also

- [Heap insertion](insertion%20-%20heaps.md)
- [Deletion](deletion.md)
- [Heap updates](update%20-%20heaps.md)
