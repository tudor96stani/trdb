---
created: 2026-10-10
---

`Row` is the in-memory representation of one record. A cell associates one column ID with one value; the row keeps those cells in a map keyed by `SchemaEntryId`. We build it against a schema so each supplied value has a column ID and the expected type.

This representation is convenient for looking up values by column. Storing it on a page requires a separate encoding step that turns the cells into contiguous bytes; see the [binary row format](../../engine/binary_row_representation.md).

## Values

`RowCellValue` supports `Int(i32)`, `String(String)` and `NULL`. `NULL` means the cell has no value; it is distinct from integer zero or an empty string. An `i32`, `String` or `&str` converts into a value; we supply `NULL` explicitly.

Each `RowCell` owns its ID and value. The finished row does not borrow the schema. Only `RowBuilder` keeps a schema reference while we construct the row.

## Building a row

```rust
use shared::schemas::data_type::DataType;
use shared::schemas::identifiers::ColumnId;
use shared::schemas::row::{Row, RowCellValue};
use shared::schemas::schema::Schema;

let schema = Schema::builder(1)
    .column(ColumnId::new(0), "id", DataType::Int, 0, true)
    .column(ColumnId::new(1), "name", DataType::String, 1, false)
    .pk([ColumnId::new(0)])
    .build()
    .expect("valid schema");
let row = Row::builder(&schema)
    .value(ColumnId::new(0), 7)
    .value_named("name", RowCellValue::NULL)
    .build();
```

We can provide a value for a column in four ways:

- `cell(entry, value)` uses a schema entry reference.
- `value(column_id, value)` looks up its column ID.
- `value_at_position(position, value)` looks up its position.
- `value_named(name, value)` looks up its exact name.

The three lookup methods scan the schema map. The selected column must belong to the schema; a failed lookup panics.

Once we have the entry, `cell()` handles the value:

1. We assert that the entry's ID occurs in the builder's schema.
2. We assert that the value matches the supplied entry's type. `NULL` matches either type, even for primary-key columns.
3. We insert the cell under that ID, replacing any value already supplied for it.

Failed assertions panic.

`build()` consumes the map and returns a row containing the supplied cells. To store NULL, we add a cell with `RowCellValue::NULL` explicitly.

## Access

`get(&SchemaEntryId)` returns `Option<&RowCell>`. A cell provides its copied ID through `schema_entry_id()` and a borrowed value through `value()`.

`values()` iterates cells in the map's unspecified order. To read in schema order, we walk the sorted schema entries and look up each cell.

## See also

- [Schemas](schemas.md)
- [Binary row representation](../../engine/binary_row_representation.md)
