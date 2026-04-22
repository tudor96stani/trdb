---
created: 2026-01-04
---
mostly a remember me idea here.

while implementing [insertion - heaps](insertion%20-%20heaps.md), i encountered a few scenarios that initially seemed impossible (or not valid/legally reachable), but which on further inspection led me to realize that they were possible by implementing [deletion](deletion.md) correctly (which i had missed doing).

the scenarios are the following:
- insert a new row between two existing rows, but create a new slot for it.
- insert a new row at `free_start` after [page compaction](page%20compaction.md), but create a new slot

take the first one through an example (first without the fix for page fragmentation)
```
SETUP
insert row 1 @96, 100 bytes
data: | row 1  |
      96      196
free_start = 196
slots: [(96, 100)]
--------------------------------
insert row 2 @196, for 100 bytes
data: | row 1  |   row 2  |
      96      196        296
free_start = 296
slots: [(96, 100), (196,100)]
--------------------------------
delete row 2
data: | row 1  | free space |
      96      196          296
free_start = 296 // ! Important. this value stays the same since we are not compacting yet.
slots: [(96, 100), (0,0)]
--------------------------------
insert row 3 @free_start = 296, for 3704 bytes (until offset 4000)
data: | row 1  | free space |  row 3            |
      96      196          296                 4000
free_start = 4000
slots: [(96, 100), (296,3704)]
free_end = 4087 // since we have only 2 slots

TEST SCENARIO
insert row 4 of 100 bytes.
```
- for the test scenario of inserting row 4, it will first attempt to make the insertion at `free_start`.
- since there are only 87 bytes there, it will not be able to and will start looking for a fragment between rows.
- it will find the section `196-296` free and containing enough bytes to fit the row, so it will use it

I could have written a test for this scenario, but I realized implementing that fix for resetting the `free_start` when deleting the last row makes sense, so that invalidated the scenario again.
If we run it with the fix applied:
```
SETUP
insert row 1 @96, 100 bytes
data: | row 1  |
      96      196
free_start = 196
slots: [(96, 100)]
--------------------------------
insert row 2 @196, for 100 bytes
data: | row 1  |   row 2  |
      96      196        296
free_start = 296
slots: [(96, 100), (196,100)]
--------------------------------
delete row 2
data: | row 1  | free space |
      96      196          296
free_start = 196 // ! Important. this value is now set back to 196
slots: [(96, 100), (0,0)]
--------------------------------
insert row 3 @free_start = 196, for 3704 bytes (until offset 3900)
data: | row 1  | free space |  row 3            |
      96      196          296                 3900
free_start = 3900
slots: [(96, 100), (196,3704)]
free_end = 4087 // since we have only 2 slots

TEST SCENARIO
insert row 4 of 100 bytes.
```
Notice that now, we no longer have any fragments.

As of right now, i cannot think of any other setups that would get us to be able to insert a row between two existing rows, while also creating a new slot for it, mostly because a fragment usually involves a deletion, and a deletion involves an unused slot.

will need to revisit this once i implement [update - heaps](update%20-%20heaps.md) where fragments could appear by updating the row to a smaller size.
