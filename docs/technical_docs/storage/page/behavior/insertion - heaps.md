---
created: 2026-01-05
---

Heap insertion places a row without sorting it by a key. We need both a logical slot to identify the row and a contiguous byte range to hold it. Those are separate choices: a reused slot can point to newly appended bytes, and a new slot can point into an existing gap.

We choose both before changing the page. The caller first requests a plan, then passes that plan and the row bytes to `insert_heap()`.

The caller keeps the page unchanged between those calls and inserts the length used for planning. With buffered pages, we normally keep the same exclusive page guard for both.

## Planning phase

`plan_insert(row_len)` reads the page without changing it. We choose the slot, check total capacity, then find a place for the bytes.

### Slot and capacity

1. We go through the slots in index order, starting at slot 0 at the page's end.
2. The first slot with a zero offset or length becomes `Reuse(index)`. If all slots are live, we choose `New`.
3. We compare `free_space` with `row_len` for reuse, or `row_len + SLOT_SIZE` for a new slot.
4. If there are not enough total bytes, we return `InsertError::NotEnoughSpace`.

Reusing a slot avoids growing the slot array. The capacity check counts gaps as free space; it does not tell us whether one gap is large enough for the row.

### Physical location

`find_insertion_offset(row_len, None)` tries these locations in order:

1. If `free_end.saturating_sub(free_start) >= row_len`, we return `Exact(free_start)`.
2. Otherwise, we collect valid rows as `(start, end)` pairs and sort them by starting offset. We check each gap between adjacent rows, skipping overlapping pairs.
3. We check the tail from the physically last row's end to `free_end`.
4. If no location fits, or there are no valid rows after the first check fails, we return `AfterCompactionFreeStart`.

Physical offsets must be sorted because slot order can differ from row order after updates or gap inserts.

Header and slot errors also propagate through the public page error wrapper.

## Insertion phase

`insert_heap(plan, row)` applies the plan:

1. If the offset is `AfterCompactionFreeStart`, we compact the page.
2. We resolve the destination offset from `Exact(offset)` or the current `free_start`.
3. For a new slot, we increment `slot_count` and subtract `SLOT_SIZE` from `free_end`. For reuse, both stay unchanged.
4. If the destination is the current `free_start`, we advance it by the row length. A gap insertion leaves it unchanged.
5. We subtract the row length from `free_space`, plus `SLOT_SIZE` if we created a slot.
6. We copy the row bytes and write the slot's offset and length.

The method uses the supplied plan and row bytes directly.

## Reasoning

Write-ahead logging (WAL) records a change before we apply it. The split between planning and insertion came from the Java implementation's WAL work: a heap log entry needs the slot number before the insert, because the row ID includes that number. Planning gives the caller that information without changing the page, and keeps logging outside `Page`.

WAL is not implemented in Rust yet. This explains the API split, not an extra step in the current insertion flow.

## Placement order

Trying the append region first avoids scanning row offsets when there is enough room there. Gap search is used when the append region cannot fit the row.

Slot selection and offset search are separate steps, keeping the slot choice distinct from physical placement.

## See also

- [Insertion edge cases](insertion%20-%20edge%20cases.md)
- [Deletion](deletion.md)
- [Compaction](page%20compaction.md)
