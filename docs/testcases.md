# Test Cases

## Pending Review

## Approved
### Binary Row Representation
#### Serialization

#### Deserialization

## Active

### Binary Row Representation
#### Serialization

##### TC-ROW-SER-001 — Serialize valid rows, including NULL/empty distinction and UTF-8 lengths

**Requirements:** ROW-BIN-001, ROW-BIN-002

**Scenarios:**
1. Using schema owner `1` with columns `(column id, name, type, position, is_pk)` = `(0, "id", Int, 0, true)`, `(1, "name", String, 1, false)`, `(2, "age", Int, 2, false)`, `(3, "email", String, 3, false)`, `.pk([0])`, the ordinary row `{ id = 1, name = "Mary", age = 25, email = "mary@mail.com" }` → `to_binary` returns exactly 43 bytes:
   `2B 00 02 00 02 00 01 00 00 00 19 00 00 00 00 00 00 00 00 00 00 00 04 00 11 00 4D 61 72 79 6D 61 72 79 40 6D 61 69 6C 2E 63 6F 6D`.
2. The same schema with row `{ id = 1, name = NULL, age = 25, email = "" }` → returns exactly 26 bytes:
   `1A 00 02 00 02 00 01 00 00 00 19 00 00 00 02 00 00 00 00 00 00 00 00 00 00 00`. (Bitmap byte 0 is `02`, marking bit 1/`name` NULL; both string end offsets are `0`, distinguishing NULL `name` from the non-NULL empty `email`.)
3. The same schema with row `{ id = 1, name = "é", age = 25, email = "" }` → returns exactly 28 bytes:
   `1C 00 02 00 02 00 01 00 00 00 19 00 00 00 00 00 00 00 00 00 00 00 02 00 02 00 C3 A9`. (String end offsets are `[2, 2]`, counting the 2 UTF-8 bytes of `"é"`, not 1 character.)

##### TC-ROW-SER-002 — Reject rows with an invalid cell-to-schema correspondence

**Requirements:** ROW-BIN-002

**Scenarios:**
1. Using schema owner `1` with columns `(0, "id", Int, 0, true)`, `(1, "name", String, 1, false)`, `(2, "age", Int, 2, false)`, `(3, "email", String, 3, false)`, `.pk([0])`: build a row supplying explicit cells for `id`, `age`, and `email` only (omitting `name`) → `to_binary` returns `RowSerializationError::MissingRowCellForSchemaEntry` identifying the `name` schema entry.
2. Build a row against a *different* one-column schema (owner `2`, column `(7, "value", Int, 0, false)`, value `1`) and call `to_binary(&that_row, &schema)` using the four-column schema above → returns `RowSerializationError::MissingSchemaEntryForRowCell` identifying the `(owner 2, column 7)` entry.

##### TC-ROW-SER-004 — Enforce the 3996-byte maximum row size

**Requirements:** ROW-BIN-001, ROW-BIN-002

**Scenarios:**
1. Using schema owner `1` with columns `(7, "number", Int, 0, false)`, `(99, "text", String, 1, false)`, `(42, "empty", String, 2, false)`: build a row `{ number = NULL, text = "é" repeated 1987 times (3974 UTF-8 bytes), empty = NULL }`. The complete encoded size is `6 + 4 + 8 + 4 + 3974 = 3996` bytes → `to_binary` succeeds, returning exactly 3996 bytes whose header declares size `3996` (`9C 0F` little-endian) and whose last 3974 bytes equal the UTF-8 bytes of `text`.
2. Same schema and row shape, but `text` = `"é"` repeated 1987 times followed by one more ASCII character (3975 bytes) → the complete size would be 3997 bytes → `to_binary` returns `RowSerializationError::RowTooLarge`.

##### TC-ROW-SER-005 — Accept the maximum 64-column schema with every column NULL

**Requirements:** ROW-BIN-001, ROW-BIN-002

**Scenarios:**
1. Schema owner `1` with 64 columns: for `position` 0 through 63, column id = `position`, name = `"column_{position}"`, type `Int` when `position` is even and `String` when odd, `is_pk = true` for positions `0` and `1` only, `.pk([0, 1])`. Build a row with every column set to `RowCellValue::NULL` → `to_binary` succeeds, returning exactly 206 bytes (`6 + 4*32 + 8 + 2*32 + 0`): header bytes `0..6` are `CE 00 20 00 20 00` (size `206`, 32 integer columns, 32 string columns); integer bytes `6..134` are all `0x00`; bitmap bytes `134..142` are all `0xFF` (all 64 bits set); string-offset bytes `142..206` (the 32 string end offsets) are all `0x00`.

#### Deserialization

##### TC-ROW-DES-001 — Deserialize valid rows, including NULL/empty distinction and UTF-8 lengths

**Requirements:** ROW-BIN-001, ROW-BIN-003

**Scenarios:**
1. Using schema owner `1` with columns `(0, "id", Int, 0, true)`, `(1, "name", String, 1, false)`, `(2, "age", Int, 2, false)`, `(3, "email", String, 3, false)`, `.pk([0])`: decoding the 43-byte sequence `2B 00 02 00 02 00 01 00 00 00 19 00 00 00 00 00 00 00 00 00 00 00 04 00 11 00 4D 61 72 79 6D 61 72 79 40 6D 61 69 6C 2E 63 6F 6D` returns a row with cells `id = 1`, `name = "Mary"`, `age = 25`, `email = "mary@mail.com"`, keyed by schema owner `1` and the respective column ids `0`, `1`, `2`, `3`.
2. The same schema: decoding the 26-byte sequence `1A 00 02 00 02 00 01 00 00 00 19 00 00 00 02 00 00 00 00 00 00 00 00 00 00 00` returns `id = 1`, `name = NULL`, `age = 25`, `email = ""` (not NULL).
3. The same schema: decoding the 28-byte sequence `1C 00 02 00 02 00 01 00 00 00 19 00 00 00 00 00 00 00 00 00 00 00 02 00 02 00 C3 A9` returns `id = 1`, `name = "é"`, `age = 25`, `email = ""`.

##### TC-ROW-DES-002 — Round-trip is independent of row-cell insertion order

**Requirements:** ROW-BIN-002, ROW-BIN-003

**Scenarios:**
1. Using schema owner `1` with columns `(0, "id", Int, 0, true)`, `(1, "name", String, 1, false)`, `(2, "age", Int, 2, false)`, `(3, "email", String, 3, false)`, `.pk([0])`: build one row via `Row::builder(&schema).value(0, 1).value(1, "Mary").value(2, 25).value(3, "mary@mail.com")` and a second row via the same calls in reverse order (`value(3, ...)` first, `value(0, ...)` last). `to_binary` on both rows returns byte-for-byte identical output, equal to:
   `2B 00 02 00 02 00 01 00 00 00 19 00 00 00 00 00 00 00 00 00 00 00 04 00 11 00 4D 61 72 79 6D 61 72 79 40 6D 61 69 6C 2E 63 6F 6D`.
2. `from_binary` on that output, using the same schema, returns a row whose `id`, `name`, `age`, `email` cells (looked up by schema entry id) equal `1`, `"Mary"`, `25`, `"mary@mail.com"`, regardless of which builder produced the original row.

##### TC-ROW-DES-003 — Reject malformed row framing

**Requirements:** ROW-BIN-003

**Scenarios:**
1. Using schema owner `9` with columns `(5, "value", Int, 0, false)`, `(6, "label", String, 1, false)` (no primary key), the baseline valid 22-byte encoding of `{ value = 7, label = "hi" }` is:
   `16 00 01 00 01 00 07 00 00 00 00 00 00 00 00 00 00 00 02 00 68 69`.
2. A byte array of length 3997 (contents irrelevant) → `from_binary` returns `RowDeserializationError::RowTooLarge`.
3. A single byte `00` (too short to read even the header's size field) → returns `RowDeserializationError::Binary(_)`.
4. The baseline bytes with the first two header bytes changed to `17 00` (declaring size 23 while the array is still 22 bytes) → returns `RowDeserializationError::MismatchedSize { expected: 23, actual: 22 }`.
5. The baseline bytes with header bytes 2..4 changed to `02 00` (declaring 2 integer columns while the schema has 1) → returns `RowDeserializationError::MismatchedColumnCounts`.
6. The first 18 bytes of the baseline (header, integer, and bitmap only; offsets and blob dropped), with the header size field changed to `12 00` (18) so it matches the truncated length, counts left at `01 00 01 00` → returns `RowDeserializationError::MismatchedSize { expected: 20, actual: 18 }` (20 = header + integer + bitmap + one offset).

##### TC-ROW-DES-004 — Reject invalid NULL representations

**Requirements:** ROW-BIN-003

**Scenarios:**
1. Using schema owner `9` with columns `(5, "value", Int, 0, false)`, `(6, "label", String, 1, false)` (no primary key), the baseline valid 22-byte encoding of `{ value = 7, label = "hi" }` is:
   `16 00 01 00 01 00 07 00 00 00 00 00 00 00 00 00 00 00 02 00 68 69`.
2. Changing the bitmap bytes (offset 10..18) to `04 00 00 00 00 00 00 00` (bit 2 set, beyond the schema's 2 columns) → returns `RowDeserializationError::InvalidNullBitmap`.
3. Changing the bitmap bytes to `01 00 00 00 00 00 00 00` (bit 0 set, marking `value` NULL) while leaving the integer bytes as `07 00 00 00` (nonzero) → returns `RowDeserializationError::InvalidNullColumn { column_id: 5 }`.
4. Changing the bitmap bytes to `02 00 00 00 00 00 00 00` (bit 1 set, marking `label` NULL) while leaving the offset (`02 00`) and blob (`"hi"`) unchanged (nonempty) → returns `RowDeserializationError::InvalidNullColumn { column_id: 6 }`.


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


##### TC-ROW-DES-005 — Reject invalid string sections

**Requirements:** ROW-BIN-003

**Scenarios:**
1. Using schema owner `9` with columns `(10, "first", String, 0, false)`, `(11, "second", String, 1, false)` (no primary key), the valid 23-byte encoding of `{ first = "ab", second = "cde" }` is:
   `17 00 00 00 02 00 00 00 00 00 00 00 00 00 02 00 05 00 61 62 63 64 65`.
2. Swapping the offsets to `05 00 02 00` (decreasing) → returns `RowDeserializationError::InvalidStringOffsets`.
3. Changing the second offset to `09 00` (beyond the 5-byte blob) while the first stays `02 00` → returns `RowDeserializationError::InvalidStringOffsets`.
4. Changing the second offset to `03 00` while the blob keeps its full 5 bytes and the array length is unchanged → returns `RowDeserializationError::TrailingStringData`.
5. Using a separate schema owner `9` with one column `(6, "label", String, 0, false)`: the 17-byte encoding `11 00 00 00 01 00 00 00 00 00 00 00 00 00 01 00 FF` (offset `1`, blob byte `0xFF`, not valid UTF-8) → returns `RowDeserializationError::StringDeserialization { index: 0, .. }`.
6. Using a separate schema owner `9` with one column `(5, "value", Int, 0, false)`: the 18-byte valid encoding of `{ value = 7 }` (`12 00 01 00 00 00 07 00 00 00 00 00 00 00 00 00 00 00`) with one extra trailing byte `AA` appended and the header size field changed to `13 00` (19, matching the new length) → returns `RowDeserializationError::TrailingStringData`.
## Rejected

### Binary Row Representation

#### Serialization

##### TC-ROW-SER-003 — Reject type mismatches between cell values and schema column types

Check that serialization rejects a cell value whose type differs from its schema column type.

**Rejected:** 2026-10-21

**Reason:** User dropped the case because the public row builder panics on mismatched cell types, so the specified rows cannot be constructed through the available API.

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
