---
created: 2026-10-21
---
## definition
The sys tables are a set of standard, automatically created tables that are part of every database and help the engine manage the catalog of objects (user defined tables, indexes, constraints, schemas, etc.).

The engine will automatically create them for a new database and maintain them during DDL operations. Users are allowed to perform queries on them, but they cannot directly manipulate either their structure or their contents.

For simplicity, the list of sys tables, along with their schemas and internal IDs is hardcoded and expected to by engine to stay constant. 

Additionally, a system database called `master` is present by default on any instance of the server, automatically created and maintained by the system. 

The following system tables are defined:
```text
id  table            file
1   sys.tables       sys_tables.tbl
2   sys.indexes      sys_indexes.tbl
3   sys.schemas      sys_schemas.tbl
4   sys.constraints  sys_constraints.tbl
5   sys.databases    sys_databases.tbl    (master only)
```

## behavior and usage
- the sys tables don't contain info about themselves. They are not listed in `sys.tables`, and the engine finds them through their hardcoded IDs and file names

### master
`master` is the default database and always has `db_id = 1`. It is a normal database with the same sys tables as any other, plus `sys.databases`. `sys.databases` is a sys table like the others (hardcoded schema, ID 5 and file `sys_databases.tbl`, not listed in `sys.tables`, can't be dropped). Only `master` has it.

Users can connect to this database and perform any normal operations within it - create tables, insert data & query it. It acts as a normal database, except it stores the `sys.databases` table as well.

### sys.databases
Only in `master`. One row per database:
- id - the `DatabaseId`
- name - the database name

### sys.tables
This sys table stores the list of all user defined tables. Initially empty. It has the following schema:
- id - short (the ID of the table, an `ObjectId`)
- name - the name of the table
- type - heap or clustered
- file - the file name, `<table name>.tbl`. Set at creation, never changes

### sys.indexes
One row per index, including the clustered index of a clustered table. A clustered index has the same ID as its table. Nonclustered indexes get their own ID from the same pool as tables.
- id - the index ID (an `ObjectId`)
- name - the name of the index
- table_id - the ID of the table it belongs to. for clustered indexes, it will be equal to `id`
- is clustered
- is unique
- file - the file name. `<index name>.idx` for a nonclustered index (index names include the table name), the table's file for a clustered index
- key columns - TBD

### sys.schemas
One row per column of every user table. Here a schema means the structure of a table, not a namespace like in SQL Server.
- table_id - the ID of the table
- column id - local to the table
- TBD column definitions

### sys.constraints
One row per constraint. Constraint IDs and names are local to their table.
- table_id - the ID of the table the constraint belongs to (for a foreign key, the child table)
- constraint id - local to the table
- name - unique within the table
- backing index - the ID of the index enforcing the constraint, if any
- referenced table - for foreign keys, the ID of the referenced table
- referenced column id - for foreign keys, the ID of the referenced column
- TBD

See [identifiers](../shared/schemas/identifiers.md) for how all these IDs are assigned.
