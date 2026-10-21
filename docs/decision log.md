---
created: 2026-04-21
---
**A log of various technical decisions made during development. Context either points to the note in [implementation notes](implementation%20notes.md) on which the decision was based (and where the full context is provided), or describes it in-place**

---

## 261021-1800 object identifier slight redesign
Decided to add database IDs now instead of later. DatabaseId is the only ID unique across the server, and `master (ID 1)` is a normal database that also holds `sys.databases`. Every other ID is scoped to its database. Tables and nonclustered indexes share one ID pool per database, and we will keep the same approach as in the Java db with one file per object, so the file ID is just the object ID. A clustered index shares its table's ID (same as Java). Columns and constraints are local to their table. Pages are identified by `(db_id, file_id, page_number)`. Sys tables have the same hardcoded IDs and file names in every database and don't list themselves. Each database has its own folder, and user files get readable names stored in the sys rows.

There are no persisted counters: next IDs are max + 1, rebuilt at startup, which means a drop has to fully clean up (buffer pool, file handle, file) before its ID can be reused. We don't support converting heaps to clustered tables or renaming anything, and an ALTER on columns would rewrite rows eagerly. In code, each kind of ID gets its own wrapper type around `ObjectId = u16,` so the compiler catches mix-ups.

---

## 260428-1720 schemas & ids
**Decision** Uniquely identify a column by using the `table_id + column_id` (or `index_id`) pairing, instead of the `table_name + column_id` used previously in Java. This should reduce the overall footprint of the identifier (though will add complexity for printing/displaying execution plans, as we will need to retrieve the object names as well).
#### Context
[260428-1720 schemas](implementation%20notes.md#260428-1720-schemas)

---

## 260417-0640 Reorganize storage crates into a single one
**Decision**: Simplify architecture by doing a single crate/component for **now**. Aggregate existing crates under `crates/storage` (into a single one) and remove unnecessary structs, enums, error types, etc. 
#### Context
Initial choice for project structure was to separate each major component (storage engine, query engine, etc.) into separate small crates and define an `*-api` crate (e.g. `storage-api`) that acted as the gateway into that component. This might be too complex and overengineered.

---

## 260219-1533 store full file paths in file catalog
Keep the `FileManager` dumb - it should not maintain internally any base path or logic for path reconstruction. Instead, the `FileCatalog` should be able to fully construct the path to a file and pass it to the `FileManager` when requested
#### Context
- [260219-1533 file paths](implementation%20notes.md#260219-1533-file-paths)
---

## 260218-1308 add test-only code snippets to cover race condition tests in buffer
**Decision**: add some extra fields and methods on the buffer manager, hidden behind a `#[cfg(test)]`, to allow tests to set up various scenarios to verify race conditions. 
avoid (for now at least) external crates that do concurrent test setups or hiding the real implementation of the buffer behind a test one.
#### Context
- [implementation notes](implementation%20notes.md#260218-1308-testing-cfg-and-buffer)
---
## 260214-1852 Keep binaries in `src/apps` and libraries in `crates`
**Decision**: Keep application binaries under `src/apps` and `crates/` for engine.
#### Context
- [260214-1852 small reorg of crates](implementation%20notes.md#260214-1852-small-reorg-of-crates)

---
## 260213-0633 Use a hybrid async plus worker-thread server model
**Decision**: Use Tokio for connection handling and async network I/O, and execute request/query work on blocking worker threads via a bounded worker pool. Additionally, we will limit concurrent requests to a predefined number using a semaphore in front of the worker pool
#### Context
- [260213-0633 async or threads](implementation%20notes.md#260213-0633-async-or-threads)
