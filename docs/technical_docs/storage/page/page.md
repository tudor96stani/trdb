---
created: 2026-01-05
---

A page is the fixed-size unit we read from and write to a data file. `Page` owns a boxed 4096-byte array and its in-memory identity. Within those bytes, a slotted page uses a header and a slot array to locate rows; the [layout doc](layout.md) describes that format.

The page methods initialize the bytes, expose borrowed views and apply heap operations. A heap stores rows without sorting them by a key, so their physical order follows placement rather than column values.

## Identity and type

`PageId` contains `db_id: DatabaseId`, `file_id: FileId` and `page_number: u32`. `FileId` is an alias for `ObjectId` (a `u16`), and the file ID is the ID of the table or nonclustered index stored in the file (see [identifiers](../../shared/schemas/identifiers.md)). File IDs are only unique within a database, so we need all three fields to find a page. The catalog maps `(db_id, file_id)` to a path. Equality and hashing use all three fields, and the display form is `db_id:file_id:page_number`, for example `2:5:0`. The byte header stores only the page number, because sibling pointers never leave their file.

`PageType` converts to these `u16` values:

- `Unsorted = 1`
- `IndexRoot = 2`
- `IndexInternal = 3`
- `IndexLeaf = 4`

Converting another value to `PageType` returns `Err(())`. Heap insertion, deletion, updates and compaction operate on unsorted pages. Index variants identify the index page kinds; index behavior is not yet implemented.

## Construction

We can create a page in three ways:

- `new_zeroed(id)` allocates all-zero bytes. These bytes do not form an initialized slotted page.
- `new_empty(id, type)` starts with zeroed bytes and sets the empty-page header.
- `new_from_bytes(bytes, id)` takes ownership of an existing boxed page array.

To initialize an existing page, we call `initialize(id, type)`:

1. We zero all 4096 bytes, discarding any previous contents.
2. We set the empty-page header, using the supplied page number and type.
3. We set the in-memory `PageId`.

`set_page_id()` only changes the in-memory ID. It does not update the header's page number.

## Access

Header and slot accessors return views borrowing the page bytes. `data()` and `data_mut()` expose the whole array. When we change raw bytes, we are responsible for keeping the [layout](layout.md) consistent.

`row(slot_index)` returns a borrowed row slice; the [read flow](behavior/read.md) describes its lifetime and invalid-slot behavior. `slot_count()` includes invalidated slots, so it is not a count of live rows.

The public row, slot-count and mutation methods return `PageResult`, which attaches the current page ID to the operation error. Constructors and lower-level views return their own header or slot errors.

