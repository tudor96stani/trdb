---
created: 2026-10-10
---

`Schema` describes the shape of a table or index: which columns it has, their types and their order. A schema is metadata; a row holds the values for those columns. It lives in the shared crate so row construction and the engine can use the same description.

We keep columns in a map keyed by their IDs and keep primary-key IDs in a separate ordered list. The builder collects this metadata, then returns the finished schema.

## Identifiers

`ObjectId` is a `u16` identifying a database object. In a `SchemaEntryId`, the `owner_id` identifies the table or index and the `column_id` identifies a column within it. Equality and hashing use both. Column 0 of owner 1 is therefore different from column 0 of owner 2.

The decision log describes this pairing as a way to identify columns across tables without storing the owner's name in each ID. The two IDs use four bytes in total. When displaying a readable name, we still need to look up the owner and column metadata.

## Columns

Each `SchemaEntry` stores:

- Its `SchemaEntryId` and name.
- Its `DataType`, either `Int` or `String` for now.
- Its `position: usize` within the schema.
- Its `is_pk` flag.

The position gives the column's logical order, rather than a byte offset in a stored row. `is_pk` marks membership in the primary key, the columns used to identify a row. Here we store that description as metadata.

A fixed-size type occupies the same number of bytes for every value. `is_fixed_size()` is true for `Int` and false for `String`, whose byte length depends on its contents.

## Building a schema

The builder accepts raw column values, so callers do not have to construct the inner metadata structs. This is the purpose recorded in the implementation notes.

```rust
let schema = Schema::builder(42)
    .column("name", DataType::String, 1, false)
    .column("id", DataType::Int, 0, true)
    .build();
```

1. We supply the owner ID to `builder()`.
2. Each `column()` call takes the name, type, position and primary-key flag. We convert the position to `u16` for the column ID, so these calls produce `(42, 1)` and `(42, 0)`.
3. We insert the entry into the column map and append its ID to the primary-key list if `is_pk` is true.
4. `build()` sorts the primary-key IDs by column ID and returns the schema.

A column position must fit in `u16`. Positions do not need to be contiguous.

### Repeated positions

The column map is keyed by ID. Adding a column at an existing position replaces the previous entry, because the owner and column IDs match.

## Access

`columns()` borrows the map, whose iteration order is unspecified. When we need position order, `columns_sorted()` collects entry references and sorts them on each call. `primary_keys()` borrows the list sorted during construction; position-derived column IDs make that position order too.

The schema stores column metadata and identifies which columns make up the primary key.

## See also

- [In-memory rows](rows.md)
- [Binary row representation](../../engine/binary_row_representation.md)
