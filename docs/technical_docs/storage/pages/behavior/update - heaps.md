---
created: 2026-01-21
---
the overall process of updating a row in a heap page is relatively straight forward:
1. Check if the page has enough free bytes to accommodate the new row by applying the following comparison (which represents a valid scenario): `free_space + old.len >= new.len`
   - basically, we should treat the space occupied by the old row as free, so we add it to the total free space
2. Check in which of the following scenarios we are:
   1. new row is equal to old row
   2. new row is smaller than old row
   3. new row is larger than old row
3. Based on the scenario:
   - For 1) and 2), we just overwrite the old row
   - For 3), we need to do the following:
     - Invoke the `find_insertion_slot` (used for inserts as well)
     - If needed based on its result, compact page
     - Place row at specified offset (free_start or exact location)
4. Based on the scenario:
   - For 1), skip
   - For 2) and 3), we need to do the following:
     - Update the slot with new length (2 & 3) & new offset (3)
     - Update free_space (2&3)
     - Update free_start (3)

## See also
- [insertion - heaps](insertion%20-%20heaps.md)
- [page compaction](page%20compaction.md)
