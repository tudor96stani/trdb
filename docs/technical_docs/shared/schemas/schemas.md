---
created: 2026-10-10
---

`Schema` describes the shape of a table or index: which columns it has, their types and their order. A schema is metadata; a row holds the values for those columns. It lives in the shared crate so row construction and the engine can use the same description.

We keep columns in a map keyed by their IDs and keep primary-key IDs in a separate ordered list. The builder collects this metadata, then returns the finished schema.

## Identifiers

In a `SchemaEntryId`, the `owner_id` is a plain `ObjectId` identifying the table or nonclustered index the schema belongs to, and the `column_id` is a `ColumnId` identifying a column within it. The owner stays an `ObjectId` because a schema can belong to either a table or an index, and we don't want an enum for that. Equality and hashing use both. Column 0 of owner 1 is therefore different from column 0 of owner 2. How the IDs are scoped and assigned is described in [identifiers](identifiers.md).

Callers supply column IDs explicitly. They can assign them starting from 0 in definition order, but IDs do not have to equal positions or be contiguous.

## Columns

Each `SchemaEntry` stores:

- Its `SchemaEntryId` and name.
- Its `DataType`, either `Int` or `String` for now.
- Its `position: usize` within the schema.
- Its `is_pk` flag.

The position gives the column's logical order, rather than a byte offset in a stored row. `is_pk` marks membership in the primary key, the columns used to identify a row. Here we store that description as metadata.

A fixed-size type occupies the same number of bytes for every value. `is_fixed_size()` is true for `Int` and false for `String`, whose byte length depends on its contents.

## Building a schema

The builder takes `ColumnId` values and builds the `SchemaEntry` and `SchemaEntryId` structs itself, so callers do not have to construct them. The owner stays a raw `ObjectId`.

```rust
use shared::schemas::data_type::DataType;
use shared::schemas::identifiers::ColumnId;
use shared::schemas::schema::Schema;

let schema = Schema::builder(42)
    .column(ColumnId::new(7), "name", DataType::String, 1, false)
    .column(ColumnId::new(100), "id", DataType::Int, 0, true)
    .pk([ColumnId::new(100)])
    .build()
    .expect("valid schema");
```

1. We supply the owner ID to `builder()`.
2. Each `column()` call takes the column ID, name, type, position and primary-key flag. The builder collects entries for validation; these calls produce identities `(42, 7)` and `(42, 100)` at positions 1 and 0.
3. We supply primary-key column IDs through `.pk(...)` in the order we want to keep. The example has one key, column 100.
4. `build()` validates the columns and primary-key list, then returns `Result<Schema, SchemaError>`. On success, it puts the columns in the map and preserves their metadata and the supplied key order.

### Validation

A schema has between 1 and 64 columns. Column IDs must be unique, and names must be unique under ASCII case-insensitive comparison: `Name` and `name` conflict, while non-ASCII characters are compared exactly. Names are stored as supplied.

For `N` columns, positions must be exactly `0..N`, each occurring once. Repeated IDs or positions return an error rather than replacing a column. Column insertion order does not determine position order.

If any column has `is_pk = true`, we must supply `.pk(...)`. The list must contain exactly those marked columns, each once; unknown, repeated, unmarked or omitted IDs return an error. With no marked columns, we can omit the list or supply an empty one. Calling `.pk(...)` again replaces the previous list.

## Access

`columns()` borrows the map, whose iteration order is unspecified. When we need position order, `columns_sorted()` collects entry references and sorts them on each call. `primary_keys()` borrows the list in the order supplied through `.pk(...)`, independently of column IDs, positions and insertion order.

The schema stores column metadata and identifies which columns make up the primary key.

## See also

- [In-memory rows](rows.md)
- [Binary row representation](../../engine/binary_row_representation.md)
