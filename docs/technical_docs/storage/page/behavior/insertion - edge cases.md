---
created: 2026-01-04
---

Free row bytes and reusable slots are different resources. An insertion can need a new slot even when it uses a gap or follows compaction. Updates make these cases reachable: shrinking a row frees bytes without invalidating its slot.

The examples below follow the heap planner's two choices: which slot to use and where to put the bytes. Compaction packs live rows together when the total free space is sufficient but no single range fits.

## Deleting the last row

For rows `(96, 100)` and `(196, 100)`, deleting the second row resets `free_start` from 296 to 196. The next append can use the bytes directly after the surviving row.

With the current deletion behavior, appending 3704 bytes after that deletion gives us:

```text
slots = [(96, 100), (196, 3704)]
free_start = 196 + 3704 = 3900
free_end = 4087
```

The deleted slot is reused, and there is no interior gap. Shrinking an existing row can create a gap while keeping all slots live, as shown below. All row offsets are absolute within the page.

## Shrink followed by insertion into a gap

```text
insert slot 0: (96, 200)
insert slot 1: (296, 3700)
free_start = 3996, free_end = 4087, free_space = 92
shrink slot 0 to 100 bytes
slots = [(96, 100), (296, 3700)]
gap = 196..296 (100 bytes)
free_start = 3996, free_space = 192
```

We plan a 100-byte insertion. Both slots are live, so a new slot needs four bytes. Total space covers 104 bytes, but the append check sees only `4087 - 3996 = 91`. Sorting rows by offset exposes the 100-byte gap, so slot 2 becomes `(196, 100)`. `free_start` stays 3996, `free_end` becomes 4083 and `free_space` becomes 88. Physical row order is now slot 0, slot 2, slot 1.

## Shrink followed by compaction and a new slot

```text
before shrink: [(96, 100), (196, 100), (296, 3700)]
free_start = 3996, free_end = 4083, free_space = 88
after shrinking first two rows to 50 bytes:
slots = [(96, 50), (196, 50), (296, 3700)]
free_space = 188
```

A 150-byte insertion needs 154 bytes including its new slot. The append region has only 87 bytes, and the two interior gaps have 50 bytes each. We therefore compact: live rows become `(96, 50)`, `(146, 50)` and `(196, 3700)`, with `free_start = 3896`. The new slot 3 gets `(3896, 150)`, `free_start` becomes 4046, `free_end` becomes 4079 and `free_space` becomes 34.

Growth can also relocate a row and leave its old extent unused without adding an invalid slot. These cases explain why the planner's slot choice and physical placement are separate questions.
