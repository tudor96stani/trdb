---
created: 2026-01-04
---
deleting a row from a slotted page is usually done in a lazy manner: instead of removing the row data, we just invalidate its entry in the slot array. this way, the actual bytes of the row are no longer accessible in any legal flow.

## process
the process is quite simple:
1. you get a slot number for the row to delete
2. you open up the slot array and find the corresponding slot
3. you invalidate it

#### invalidating
this can mean really any way of flagging to a future process that the slot does not contain any valid data. in my case, it is done by setting both `offset` and `length` to `0`, but keeping the `slot_count` field the same:
```
initial: [(96, 100), (196, 50), (246, 50)]
slot_count = 3
// delete slot index 1 (196, 50)
after: [(96, 100), (0, 0), (246, 50)]
slot_count = 3
```

this adheres to the rules of stable slot numbers:
- if we made `slot_index = 1` invalid, but also decremented `slot_count` to 2, we could never reach `slot_index = 2` anymore
- if we fully deleted `slot_index = 1` and shifted `slot_index = 2` one position to the left, we would have changed that row's slot number

## benefits
the main reasoning behind it is that while we could compact that page at this point, it would be done based on the assumption that more rows will be inserted in that page.

## avoiding fragmentation when deleting the last physical row
an edge case that occurs while deleting a row from a slotted page is when the row being deleted is the last one physically in the page byte array.

this allows for a small optimization: once the row has been deleted, the free space area has technically grown. this is due to the invariant that `free_start` is always a pointer to the right of the last physical row on the page.

Consider the following page:
![avoid fragmentation during deletion from slotted pages - before](avoid%20fragmentation%20during%20deletion%20from%20slotted%20pages%20-%20before.png)
we now remove `Row 3`. This means that the whole area will become available, and we can shift the `free_start` pointer to the end of the previous row:
![avoid fragmentation during deletion from slotted pages - after](avoid%20fragmentation%20during%20deletion%20from%20slotted%20pages%20-%20after.png)
by doing this, instead of just invalidating the corresponding slot, we postpone page fragmentation.

### how
High level, i can see two ways of doing this:
1. Reading the `free_start` pointer from the slotted page header and decrementing it by the current row's length
   - This does not sound great, as it is based on the assumption that the header will always have the correct pointer. I would rather not make that assumption
2. Compute the new offset that should be used for the pointer

I went with option (2). Here i can also see two possible approaches:
1. After ensuring that the row we are deleting is the rightmost/innermost on the page, use it's starting offset as the new `free_start`
this has the problem of prior fragmentation:
```
| row 1  | row 2 | ..free.. | row 3 |
96       200    300        400      500
                                    free_start
```
If we are deleting row3, we will set `free_start` to be it's starting offset, 400. but we have a fragment between 300-400 that we could have included in the `free_space` area.

2. Determine the new `free_start` by looking at the next to last row in the page.
This can be done by finding the two largest value in an array - we go through the slots and keep track not only of the highest ending offset (the offset at which a row ends), but also the 2nd highest one.
in the above example, we would get:
```
highest = 500 // row3.offset + row3.length = 400+100 = 500
second = 300 // row2.offset + row2.length = 200+100 = 300
```
after we have confirmed that we are indeed deleting the row that corresponds to `highest`, we can simply use `second` as the new free start.

### Other checks
We mostly just ensure that we are indeed deleting the last row, that the slots are valid (skipping over invalid ones), that we are not in a place where the entire page contains only invalid slots, etc.

Prefer false negatives over false positives.

### See also
- [page compaction](page%20compaction.md)
- [insertion - heaps](insertion%20-%20heaps.md)
