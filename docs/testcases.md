# Test Cases

## Active

### Schemas

#### Schema Builder

##### TC-SCHEMA-BLD-001 — Reject an empty schema

**Requirements:** SCHEMA-001, SCHEMA-BLD-001

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)` without adding columns and call `build()`. | Returns `Err(SchemaError::InvalidColumnCount { count: 0 })`; no schema is returned. |

##### TC-SCHEMA-BLD-002 — Accept a one-column schema

**Requirements:** SCHEMA-001, SCHEMA-004, SCHEMA-005, SCHEMA-BLD-002

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)`, add `.column(42, "Value", DataType::Int, 0, false)`, and call `build()` without `.pk(...)`. | Construction succeeds. |
| 2 | Inspect `owner_id()` and `columns()`, locating the entry by its schema entry ID. | Owner is `17`; exactly one column exists. Its ID has owner `17` and column ID `42`; its name is exactly `"Value"`, type is `Int`, position is `0`, and primary-key flag is `false`. |

##### TC-SCHEMA-BLD-003 — Accept the maximum column count

**Requirements:** SCHEMA-001, SCHEMA-002, SCHEMA-003, SCHEMA-004, SCHEMA-005, SCHEMA-BLD-002

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)`. For each `p` from `63` down to `0`, add a column with ID `1000 + 2*p`, name `"column_{p}"` using decimal `p`, type `Int` for even `p` and `String` for odd `p`, position `p`, and primary-key flag `false`. Call `build()`. | Construction succeeds with 64 columns. |
| 2 | Inspect the owner and all columns using their IDs, independently of map iteration order. | Owner is `17`. Exactly the 64 supplied entries exist, each preserving owner `17`, column ID, exact name, type, position, and primary-key flag. Positions include both `0` and `63`. |

##### TC-SCHEMA-BLD-004 — Reject a column count above the maximum

**Requirements:** SCHEMA-001, SCHEMA-BLD-001

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)`. For `p` from `0` through `64`, add a column with ID `1000 + p`, name `"column_{p}"` using decimal `p`, type `Int`, position `p`, and primary-key flag `false`. Call `build()`. | Returns `Err(SchemaError::InvalidColumnCount { count: 65 })`; no schema is returned. |

##### TC-SCHEMA-BLD-005 — Reject exactly duplicated column names

**Requirements:** SCHEMA-002, SCHEMA-BLD-001

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)` with `(ID, name, type, position, is_pk)` columns `(42, "value", Int, 0, false)` and `(7, "value", String, 1, false)`. Call `build()`. | Returns the `SchemaError::DuplicateColumnName` variant; no schema is returned. |

##### TC-SCHEMA-BLD-006 — Reject ASCII case variants of a column name

**Requirements:** SCHEMA-002, SCHEMA-BLD-001

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)` with `(42, "AccountID", Int, 0, false)` and `(7, "aCCOUNTid", String, 1, false)`. Call `build()`. | Returns the `SchemaError::DuplicateColumnName` variant; no schema is returned. |

##### TC-SCHEMA-BLD-010 — Reject duplicate column IDs with distinct metadata

**Requirements:** SCHEMA-003, SCHEMA-BLD-001

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)` with `(42, "first", Int, 0, false)` and `(42, "second", String, 1, false)`. Call `build()`. | Returns `Err(SchemaError::DuplicateColumnId { column_id: 42 })`; no schema is returned instead of overwriting either column. |

##### TC-SCHEMA-BLD-011 — Reject duplicate positions within the valid range

**Requirements:** SCHEMA-004, SCHEMA-BLD-001

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)` with `(42, "first", Int, 0, false)`, `(7, "second", String, 1, false)`, and `(100, "third", Int, 1, false)`. Call `build()`. | Returns `Err(SchemaError::DuplicateColumnPosition { position: 1 })`; no schema is returned. |

##### TC-SCHEMA-BLD-012 — Reject a position equal to the column count

**Requirements:** SCHEMA-004, SCHEMA-BLD-001

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)` with `(42, "first", Int, 0, false)`, `(7, "second", String, 1, false)`, and `(100, "third", Int, 3, false)`. Call `build()`. | Returns `Err(SchemaError::InvalidColumnPosition { position: 3, count: 3 })`; no schema is returned. |

##### TC-SCHEMA-BLD-014 — Preserve independent IDs and explicit composite primary-key order

**Requirements:** SCHEMA-001, SCHEMA-002, SCHEMA-003, SCHEMA-004, SCHEMA-005, SCHEMA-BLD-002, SCHEMA-BLD-003

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)`. Add `(42, "Tenant", Int, 2, true)`, `(7, "Label", String, 0, false)`, and `(100, "Record", String, 1, true)`, in that order. Supply `.pk(vec![100, 42])` and call `build()`. | Construction succeeds. |
| 2 | Inspect `owner_id()` and each entry in `columns()` by ID. | Owner is `17`. Exactly three entries exist, with owner `17` and the exact supplied column IDs, names, types, positions, and primary-key flags. Neither column insertion order nor numerical ID order changes metadata. |
| 3 | Inspect `primary_keys()` and each returned ID's owner and column ID. | Exactly `[(17, 100), (17, 42)]` is returned in that order, independent of insertion order, ascending position order, and ascending column ID order. |

##### TC-SCHEMA-BLD-016 — Require an ordering list when any column is a primary key

**Requirements:** SCHEMA-BLD-004

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)` with `(42, "label", String, 0, false)` and `(7, "key", Int, 1, true)`. Call `build()` without `.pk(...)`. | Returns `Err(SchemaError::MissingPrimaryKeyOrder)`; no schema is returned. The non-primary first column does not hide the marked second column. |

##### TC-SCHEMA-BLD-019 — Reject an unknown primary-key column ID

**Requirements:** SCHEMA-BLD-005

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)` with `(ID, name, type, position, is_pk)` columns `(42, "key", Int, 0, true)` and `(7, "label", String, 1, false)`. Supply `.pk(vec![42, 999])` and call `build()`. | Returns a schema validation error; no schema is returned. ID `999` identifies no supplied column, while every marked column is included exactly once. |

##### TC-SCHEMA-BLD-020 — Reject a repeated primary-key column ID

**Requirements:** SCHEMA-BLD-006

| Step | Action                                                                                                                                                                                         | Expected Result                                                                                                         |
| ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| 1    | Create `Schema::builder(17)` with `(ID, name, type, position, is_pk)` columns `(42, "key", Int, 0, true)` and `(7, "label", String, 1, false)`. Supply `.pk(vec![42, 42])` and call `build()`. | Returns a schema validation error; no schema is returned. The ordering list repeats the existing marked column ID `42`. |

##### TC-SCHEMA-BLD-021 — Reject an ordering list that omits a marked primary-key column

**Requirements:** SCHEMA-BLD-007

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)` with `(ID, name, type, position, is_pk)` columns `(42, "tenant", Int, 0, true)` and `(7, "record", String, 1, true)`. Supply `.pk(vec![42])` and call `build()`. | Returns a schema validation error; no schema is returned. An explicit ordering list exists but omits marked column ID `7`. |

##### TC-SCHEMA-BLD-022 — Reject an ordering list that includes an unmarked column

**Requirements:** SCHEMA-BLD-008

| Step | Action | Expected Result |
|---|---|---|
| 1 | Create `Schema::builder(17)` with `(ID, name, type, position, is_pk)` columns `(42, "key", Int, 0, true)` and `(7, "label", String, 1, false)`. Supply `.pk(vec![42, 7])` and call `build()`. | Returns a schema validation error; no schema is returned. The ordering list includes unmarked column ID `7`, while every marked column is included exactly once. |

#### Column Ordering

##### TC-SCHEMA-ORD-001 — Sort columns by position independently of insertion and ID order

Build a schema with owner `17` and `(ID, name, type, position, is_pk)` columns `(42, "third", Int, 2, false)`, `(7, "first", String, 0, false)`, `(100, "fourth", String, 3, false)`, and `(23, "second", Int, 1, false)`, added in that order. Collect `columns_sorted()` and verify that it returns exactly four entries with `(column ID, position)` pairs `[(7, 0), (23, 1), (42, 2), (100, 3)]`. This checks the position-ordered traversal of hash-map storage used to assign NULL bitmap bits, with insertion order, numerical ID order, and type grouping all differing from position order.

## Pending Implementation

## Rejected

### Schemas

#### Schema Builder

##### TC-SCHEMA-BLD-007 — Compare ASCII letters case-insensitively inside Unicode names

Check ASCII case-insensitive comparison inside Unicode names.

**Rejected:** 2026-10-11

**Reason:** User declined additional character-comparison coverage.

##### TC-SCHEMA-BLD-008 — Keep non-ASCII case variants distinct

Accept distinct non-ASCII case variants and preserve their names.

**Rejected:** 2026-10-11

**Reason:** User declined additional character-comparison coverage.

##### TC-SCHEMA-BLD-009 — Keep differently encoded Unicode names distinct

Accept composed and decomposed Unicode names as distinct and preserve their exact sequences.

**Rejected:** 2026-10-11

**Reason:** User declined additional character-comparison coverage.

##### TC-SCHEMA-BLD-013 — Reject the largest representable position

Reject a single column whose position is usize::MAX.

**Rejected:** 2026-10-11

**Reason:** User declined this additional position boundary case.

##### TC-SCHEMA-BLD-015 — Accept IDs at both ends of the public ID range

Accept and preserve column IDs 0 and 65535 with owner ID 65535.

**Rejected:** 2026-10-11

**Reason:** User declined this additional ID boundary case.

##### TC-SCHEMA-BLD-017 — Preserve a single-column primary-key ordering list

Accept and preserve a one-column primary-key ordering list.

**Rejected:** 2026-10-11

**Reason:** User declined this additional primary-key case.

##### TC-SCHEMA-BLD-018 — Preserve generated valid schemas across independent orderings

Generate valid schemas with independently randomized IDs, positions, insertion order, types, flags, and primary-key ordering; verify construction and preservation.

**Rejected:** 2026-10-11

**Reason:** User declined the generative case.
