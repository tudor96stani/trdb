---
created: 2026-10-21
---
How we identify databases, tables, indexes, constraints and columns, and how those IDs map to files and pages.

## High level

There are two kinds of scope:
- **Instance level** - only databases. A `DatabaseId` is unique across the whole server.
- **Database level** - everything else. Table 5 in one database has nothing to do with table 5 in another one. To identify an object across the server we need the pair `(db_id, object_id)`.

Within a database, some objects are identified by a single ID and some are local to a table:
- Tables and nonclustered indexes share one ID pool per database. These are exactly the objects that have their own file.
- Columns and constraints are local to their table, so they are identified by `(table_id, column_id)` and `(table_id, constraint_id)`.

## Types

`ObjectId` is the raw number used by every kind of ID, an alias for `u16`. Each kind of ID wraps it in its own type, so the compiler stops us from passing a column ID where a table ID is expected:

```rust
pub type ObjectId = u16;

pub struct DatabaseId(ObjectId);
pub struct TableId(ObjectId);
pub struct IndexId(ObjectId);
pub struct ColumnId(ObjectId);
pub struct ConstraintId(ObjectId);
```

`u16` gives us 65,535 tables and nonclustered indexes per database, which is plenty for this project.

## Databases

- Each database gets its own folder under the data directory, named after the database: `<data_dir>/<db name>/`. Database names are unique across the server and can't be renamed, so the folder name never goes stale.
- `master` always has `db_id = 1`. It is a normal database with the usual sys tables, plus `sys.databases`, which lists every database.
- A new database gets `max(db_id in master.sys.databases) + 1`.

## Tables and indexes

Tables and nonclustered indexes take IDs from the same per-database pool, and every one of them is stored in its own file. Because of that, **the file ID is the object ID**. We don't need a separate file ID generator. The file name is a different story, see [Files](#files).

A clustered index is the table itself (its leaf pages hold the rows), so **a clustered index has the same ID as its table**. The ID shows up in both sys tables, but each row describes a different side of the same thing: `sys.tables` describes the table (name, type), while `sys.indexes` describes the index definition (key columns, unique, ...).

Index IDs are not local to a table. 

Example, with a heap `users`, a clustered `orders` and one nonclustered index on `orders`:

```text
sys.tables                              sys.indexes
id  name    type       file             id  name          table  clustered  file
5   users   heap       users.tbl        6   pk_orders     6      yes        orders.tbl
6   orders  clustered  orders.tbl       7   ix_orders_dt  6      no         ix_orders_dt.idx
```

This gives three files: 5 (`users.tbl`), 6 (`orders.tbl`) and 7 (`ix_orders_dt.idx`). The clustered index `pk_orders` has no file of its own, it points to its table's file.

A table's type (heap or clustered) is fixed when we create it. We don't support converting a heap to a clustered table, which is what makes "file = ID" hold. A conversion would need to build the new file while the old one still exists under the same ID.

## Pages

A page is identified by `(db_id, file_id, page_number)`, where `file_id` is the ID of the table or nonclustered index the file belongs to. Since `db_id` is part of the key, file IDs only have to be unique within a database, so the buffer pool never confuses page `(2, 5, 0)` with page `(3, 5, 0)`.

The file header stores the entry page of the structure (the heap directory page or the root of the B+tree). This means the sys tables don't store page addresses, only an object's ID, type and file name.

## Files

The file catalog maps `(db_id, file_id)` to a path. We want readable file names on disk (`users.tbl`, not `5.tbl`), so the name can't be computed from the ID. Instead, we store it in the object's sys row:
- `sys.tables` stores the table's file name, `<table name>.tbl`.
- `sys.indexes` stores the file name of a nonclustered index, `<index name>.idx`. Index names automatically include the table name, so two indexes in the same database never clash.
- A clustered index stores its table's file name, since they are the same file.

Table names are unique within a database, and dropping a table deletes its file before the name can be reused. Nothing can be renamed, so the file name is set at creation and never changes.

The sys tables aren't listed in `sys.tables`, so they can't store their own file names. They use hardcoded names inside the database folder, the same way they use hardcoded IDs: the table name with `_` instead of `.` (`sys.tables` -> `sys_tables.tbl`).

At startup, for each database, we register in the file catalog:
1. One entry per sys table, from the hardcoded IDs and names. For `master`, this needs nothing but the database name, so we can read it before anything else.
2. One entry per row of `sys.tables` and per nonclustered index row of `sys.indexes`: `(db_id, id) -> <data_dir>/<db name>/<file name>`.

## Constraints

Constraints are local to their table: `(table_id, constraint_id)`. Their names only need to be unique within the table.

A constraint backed by an index (a primary key backed by the clustered index, or a unique constraint backed by a unique index) gets its own ID. `sys.constraints` stores the ID of the backing index. This keeps "shared ID" limited to the table and clustered index case.

A foreign key belongs to the child table, since it constrains a column in that table. If `orders.user_id` references `users.id`, the constraint is stored under `orders` and keeps the ID of the referenced table (`users`). Operations like dropping a table or deleting rows check both directions: the table's own constraints and the constraints in other tables that reference it.

## Columns

Columns are local to their table: `(table_id, column_id)`. `sys.schemas` holds one row per column, including its ID.

1. At `CREATE TABLE`, we assign column IDs `0..N` in definition order.
2. A column added later gets `max(column_id in that table) + 1`.
3. Column IDs never change.

Stored rows are position based and don't contain column IDs, so the rows on disk always have to match `sys.schemas`. If we ever support `ALTER TABLE`, it will rewrite every row eagerly as part of the alter. Otherwise we won't support it.

## Sys tables

Sys tables use the same hardcoded IDs and file names in every database:

```text
id  table            file
1   sys.tables       sys_tables.tbl
2   sys.indexes      sys_indexes.tbl
3   sys.schemas      sys_schemas.tbl
4   sys.constraints  sys_constraints.tbl
5   sys.databases    sys_databases.tbl    (master only)
```

`sys.databases` only exists in `master`. Since these IDs are scoped to the database, the same number can be a user table in another database.

They still need IDs even though they aren't listed in `sys.tables`: their hardcoded schemas need an owner ID for their columns, and their files need a file ID. Their file names are hardcoded too (see [Files](#files)).

There's no reserved range for them. If we add a sys table later, we wipe the data directory and let the engine recreate it.

## Assigning IDs

We don't persist any counters. Every "next ID" is rebuilt at startup by scanning the sys tables:
- Next database ID: `max(db_id in master.sys.databases) + 1`.
- Next table or index ID within a database: `max(id in sys.tables and sys.indexes, highest sys table ID) + 1`. The sys tables aren't listed in `sys.tables`, so we have to start past them explicitly.

If the server crashes after handing out an ID but before writing the sys row, the ID simply gets handed out again. That's harmless, since nothing was saved for it.

`max + 1` reuses an ID when we drop the newest object. Drop table 9, create a new table, and it gets 9 again. So **dropping an object must clean up completely** before its ID is free:
1. Evict all its pages from the buffer pool, throwing away dirty pages instead of flushing them.
2. Close its file handle.
3. Delete its file.
4. Remove its sys rows.

Otherwise the new object 9 could read pages that belonged to the old one, or have them flushed over its file.

## See also

- [Schemas](schemas.md)
- [Sys tables](../../engine/sys_tables.md)
