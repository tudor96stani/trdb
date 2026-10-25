---
created: 2026-10-24
---

Plan for implementing the sys tables and the in-memory catalog. The decisions come from grill-me session `2026-10-24 sys tables implementation`. The stories are in a sensible implementation order, and each one should be doable and testable on its own.

Things that apply everywhere:
- No WAL yet, so pages are flushed to disk right after they change.
- Disk is the source of truth. The in-memory catalog only mirrors it.
- Whatever is inconsistent at startup is fatal, and the fix is to wipe the data dir (except orphans, see US15).

---

### US1: Update SchemaEntry to support nullable
- add a nullable flag to `SchemaEntry` and to the `Schema` builder
- row building and encoding should reject a NULL in a non-nullable column (the bitmap side already exists)

### US2: Extend DataType for persistence
- give strings a max length (on the type or on the column, your choice). It's needed for `sys.schemas` and for knowing up front whether a row can fit in a page
- define a stable small-int code per type, with explicit values that don't depend on enum order, and conversions both ways. An unknown code is an error, not a panic

### US3: Storage file lifecycle primitives
Storage stays page-only. It creates, deletes and allocates, but never interprets headers or directories.
- **US3.1** explicitly create an empty file for `(db, file_id)`. It fails if the file already exists
- **US3.2** stop reading from implicitly creating missing files (the file manager currently opens with `create`). Otherwise startup can't detect a missing sys file
- **US3.3** create and delete a database folder under the data dir
- **US3.4** delete a file the way the identifiers doc requires, in this order:
  1. evict its pages from the buffer pool, discarding dirty ones
  2. close the handle
  3. delete the file
  4. remove its `FileCatalog` entry (`FileCatalog` has no remove yet)
- **US3.5** check what `new_page` needs to allocate a page past the current end of the file. The caller (the heap layer) picks the page number

### US4: Heap file layout (header + directory)
The on-disk structure of a heap file, owned by `engine`.
- the header is at page 0 and lists the directory pages. The first directory page is page 1
- directory pages hold one entry per data page with its free space. Store the page number, not the offset (offset = page number × 4096)
- header and directory rows use fixed internal schemas encoded with the normal row encoding
- decide whether header and directory pages get their own `PageType` or reuse `Unsorted`
- the next page ID is the max of every known page ID (header, directory pages, data pages) + 1
- the heap is bounded by how many directory pages the header can list. Hitting that limit is a normal error, not a panic
- the header is generic, so later a B+tree file can use it to point at its root instead of a directory

### US5: Heap operations
These are the temporary "manual operators" the catalog code uses until the query engine exists. Keep their API at the level of "rows of table X in db Y" so their internals can later be swapped for real operators.
- **US5.1** initialize a new heap file: create the empty file, allocate the header and the first directory page, and write both
- **US5.2** scan: header → directory pages → data pages → decoded rows. Ideally shaped like the future `HeapScan`, i.e. something you iterate over
- **US5.3** insert: find a data page with enough free space through the directory, or allocate a new one (registering it in a directory page, or allocating a new directory page and recording it in the header), then update the free space
- **US5.4** delete: rows need an address (page + slot) so the scan can hand one back and delete can use it. Update the free space in the directory
- assume the caller holds the table lock (US13). Allocation relies on that

### US6: Sys table definitions
- **US6.1** finalize the columns of each sys table:
  - `sys.indexes`: key columns as an ordered comma-separated string of column IDs, e.g. `"2,0"`
  - `sys.constraints`: the FK's own column, plus the referenced table and column. FKs are single-column only. Backing index and the referenced columns are nullable. A PK constraint row is metadata only, not the source of truth for the key
  - `sys.schemas`: name, type code, max length, nullable
  - resolve the open PK option for `sys.schemas`: an `is_pk` flag, a nullable `pk_pos`, or nothing (derived from the clustered index in `sys.indexes`)
- **US6.2** hardcode the IDs, file names and `Schema`s of the 5 sys tables. `sys.databases` is master-only

### US7: Implement the Database struct
- in-memory version of a `sys.databases` row (ID, name), plus conversion to and from a row

### US8: Implement the Table struct
- in-memory version of a `sys.tables` row (ID, name, heap/clustered, file name), plus conversion to and from a row
- links to its `Schema`, indexes and constraints. Decide whether they're held directly or looked up by ID in the catalog
- needs a way to tell sys tables apart (system marker), since they live in the same collections

### US9: Implement the Index struct
- in-memory version of a `sys.indexes` row, plus conversion to and from a row, including parsing the key column string
- a clustered index shares the table's ID and file

### US10: Implement the Constraint struct
- in-memory version of a `sys.constraints` row (PK, FK, unique as relevant), plus conversion to and from a row
- an FK has to be findable from both sides (its own table and the referenced table) for drop and delete checks

### US11: Rebuild Schema from sys.schemas rows
- group `sys.schemas` rows by table and build a `Schema` per table
- the PK comes from wherever US6.1 decided (most likely the clustered index's key columns)
- the reverse too: turn a `Schema` into `sys.schemas` rows for `CREATE TABLE`

### US12: Implement the Catalog
- per-database collections of databases, tables, indexes, constraints and schemas, with lookups by ID and by name
- entries are immutable `Arc`s. DDL builds new entries and swaps them in in one step, and nobody edits in place
- sys tables are included, built from the hardcoded definitions and not from disk
- computes next IDs:
  - per database: `max(sys tables, sys.indexes, highest sys table ID) + 1`
  - instance-wide: `max(db_id) + 1`

### US13: Locking
A coarse first version: concurrent readers, no reads and writes at the same time.
- instance-level lock for `CREATE DATABASE`
- per-database DDL lock for `CREATE TABLE`, covering name check, ID assignment, disk writes and cache swap
- table-level locks: shared for reads, exclusive for writes and `DROP`. Heap page allocation relies on them

### US14: First boot
Runs when the data dir has nothing in it.
- create the `master` folder and its 5 sys table files (initialized heaps)
- insert master's own row (`db_id = 1`) into `master.sys.databases`

### US15: Load at startup
1. register master's sys files in the `FileCatalog` from the hardcoded definitions
2. load master's sys tables
3. for each database in `sys.databases`, register its sys files, load its sys tables, and register the user table and nonclustered index files
4. delete orphan files and orphan database folders (on disk but not in the sys tables) and log a warning for each
5. anything else inconsistent (a missing file, a bad header, sys rows that don't add up) is a fatal error with a clear message
6. the result is a fully loaded `Catalog`

US14 and US15 together are the single bootstrap function that decides between first boot and load.

### US16: DDL operations
These are the operations that keep disk and catalog in sync. With no parser yet, they're called from code and tests. Each one uses the order: register the file catalog entry if needed → write everything to disk → swap the cache.
- **US16.1** `CREATE DATABASE`: under the instance lock, assign the ID, create the folder and sys files, insert the `sys.databases` row, update the catalog
- **US16.2** `CREATE TABLE`: under the database lock, check the name, assign the ID and column IDs, initialize the file, insert rows into `sys.tables` and `sys.schemas` (plus `sys.indexes`/`sys.constraints` for a PK), update the catalog
- **US16.3** `DROP TABLE`: under an exclusive table lock, follow the cleanup order from the identifiers doc (evict pages, close the handle, delete the file, remove the sys rows), then update the catalog. Respect FKs that reference the table

### US17: Wire it into the server
- `EngineEnvironment` calls bootstrap and holds the `Catalog` next to the storage singletons. It stays a thin holder
- remove `setup_test_data` and the dummy page logic in `main.rs`/`process_query`. They use `PageId(1, 1, 0)`, which is now page 0 of `master`'s `sys_tables.tbl`, so they would overwrite the sys.tables header

### US18: Update existing documentation
Done at the very end. These are changes to existing docs under `docs/technical_docs/` so they match the implementation.
- **`engine/sys_tables.md`**
  - `sys.indexes`: replace "key columns - TBD" with the ordered comma-separated column ID string, with an example (`pk(userid, orderid)` → `"2,0"`)
  - `sys.schemas`: replace "TBD column definitions" with name, type code, max length and nullable, plus whichever PK option was chosen
  - `sys.constraints`: add the FK's own column, mark backing index and referenced columns as nullable, state that FKs are single-column, and say that the PK constraint row is metadata while the key itself is defined by the clustered index in `sys.indexes`
  - add that sys tables are part of the in-memory catalog, marked as system
  - link the new catalog and bootstrap docs from US19
- **`shared/schemas/identifiers.md`**
  - `Pages`: "the file header stores the entry page" becomes: for a heap, the header lists the directory pages, and for a B+tree it points at the root
  - `Constraints`: FKs are single-column and store the child column, referenced table and referenced column
  - `Files`: the startup registration list gets the orphan cleanup step (or a link to the bootstrap doc)
- **`shared/schemas/schemas.md`**: `SchemaEntry` gets `nullable`. `DataType` gets the max length and type codes. Update the builder example and the validation section. Update `is_pk` and `.pk(...)` if US6.1 changed them
- **`engine/binary_row_representation.md`**: add the NULL check against `nullable` (and string max length, if enforced there) to the encoding steps
- **`storage/file/file_manager.md`**: remove "a read can create a missing file" and the open-with-create step. Add explicit file creation and deletion, database folders, and removal from the catalog
- **`storage/buffer/buffer_manager.md`**: add eviction of a file's pages that discards dirty pages
- **`storage/storage_manager/storage_manager.md`**: add the new file operations and update the `new_page` description if allocation changed
- **`apps/trdb/runtime.md`**: `Service ownership` gets the bootstrap step and the `Catalog`. Add the data dir layout (`<data_dir>/<db name>/`)
- **`engine/README.md`**: the description ("Row encoding in the `engine` crate") is no longer accurate. Index the new docs

### US19: Document the new components
A proposed split for new docs under `docs/technical_docs/`, following the "subfolder per topic, README as index" rule.

Technical (close to the code):
- `engine/heap/README.md`: index
- `engine/heap/heap file layout.md`: header page, directory pages, their row formats, page ID allocation, size limit
- `engine/heap/heap operations.md`: init, scan, insert, delete
- `engine/catalog/README.md`: index
- `engine/catalog/catalog.md`: the `Catalog` structure, `Arc` entries and swapping, system marker, next ID computation, locks
- `engine/catalog/catalog objects.md`: `Database`, `Table`, `Index`, `Constraint`, and how each maps to and from its sys row (including rebuilding `Schema`)
- `shared/schemas/data_types.md`: types, max length, persistent type codes. This could also be a section in `schemas.md` if it stays small

Functional (how the parts work together and how the server behaves):
- `engine/catalog/startup.md`: first boot vs load, step order, what counts as consistent, orphan cleanup, fatal errors
- `engine/catalog/ddl.md`: `CREATE DATABASE`, `CREATE TABLE` and `DROP TABLE` end to end: locks, write order, file catalog, cache swap, what a crash in the middle leaves behind
