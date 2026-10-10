---
created: 2026-05-19
---

Encoding turns an in-memory row's cells into bytes that a page can store. The binary row format groups fixed-size integers and variable-size strings so we can locate a column without scanning every value before it. Each integer occupies four bytes; a string's size depends on its UTF-8 contents.

A page stores the encoded row as one contiguous byte range. The row header describes that range, while the schema supplies column names, types and order. Offsets in the format count bytes; they are not memory addresses.

## Layout

Numeric fields use little-endian encoding. String lengths and offsets count UTF-8 bytes, not characters.

```text
| header | integers | NULL bitmap | string end offsets | string bytes |
 6 bytes   4 * I      8 bytes       2 * S               B bytes
```

`I` is the number of integer columns, `S` the number of string columns and `B` the total string byte length. The row size is `6 + 4 * I + 8 + 2 * S + B`.

### Header

The header contains three `u16` values:

- Total row size at byte offset 0.
- Integer column count at byte offset 2.
- String column count at byte offset 4.

### Integers

Starting at byte 6, we store each integer as an `i32`, ordered by its position in the schema. Integer `j` starts at row-relative offset `6 + 4 * j`. A NULL integer still occupies four bytes, filled with zero; the bitmap distinguishes it from integer zero.

### NULL bitmap

The bitmap occupies eight bytes after the integers. In the intended format, bit 0 describes the first column, bit 1 the second, and so on. A set bit means NULL.

We use a fixed `u64` to keep the layout simple. It can track at most 64 columns, and unused bits remain zero.

### String offsets and data

After the bitmap, we keep one `u16` end offset for each string column, in schema position order. Each offset is relative to the beginning of the string-data section, not to the row or page.

To locate string `j`, we use its end offset and the previous string's end offset. The first string starts at zero. Strings are stored back-to-back as UTF-8, without separators. NULL and empty strings both add zero bytes to the data section; the bitmap tells them apart.

For example, end offsets `[4, 17]` mean the first string uses blob bytes `0..4` and the second uses `4..17`. We only need these two offsets to find the second string.

## Encoding

`to_binary(row, schema)` runs these steps:

1. We compute the byte count from the supplied cells. Each cell's ID must occur in the schema. Column counts come from the schema.
2. We allocate a zeroed vector of that size.
3. We convert the size and counts to `u16` and write the header. An overflow returns a conversion error.
4. We sort integer columns by schema position and write their values, using zero for NULL.
5. We walk all schema columns in sorted order, build the NULL bitmap and write its eight bytes.
6. We return the vector. String offsets and data stay zeroed for now.

### Schema checks

The size calculation requires every supplied cell ID to occur in the schema. Integer encoding requires the integer cells and accepts `Int` or `NULL`; the bitmap pass looks up each schema column's cell. Missing entries return the corresponding row-interpreter error. The row is supplied with explicit cells, including explicit NULL values.

### Page-size limit

The format keeps a whole row on one page. The space budget before placement is `4096 - 96 - 4 = 3996`: page size minus page header and one slot. The page planner determines whether a row can be placed using the available space.

Large objects (LOBs) are values stored separately from the row's inline bytes. LOBs and rows spanning pages are not supported.

## Example

For this schema and row:

```text
id    int
name  string
age   int
email string

{ id = 1, name = "Mary", age = 25, email = "mary@mail.com" }
```

We have two integers, two strings and no NULLs. `Mary` uses four bytes and `mary@mail.com` uses thirteen, so the complete row would use `6 + 8 + 8 + 4 + 17 = 43` bytes.

```text
row-relative range   contents
0..6                 header: 43, 2, 2
6..14                integers: 1, 25
14..22               bitmap: 0
22..26               string end offsets: 4, 17
26..43               blob: "Mary" followed by "mary@mail.com"
```

### Complete bytes

The intended encoding is:

```text
2B 00 02 00 02 00                         header
01 00 00 00 19 00 00 00                   integers
00 00 00 00 00 00 00 00                   bitmap
04 00 11 00                               string end offsets
4D 61 72 79                               "Mary"
6D 61 72 79 40 6D 61 69 6C 2E 63 6F 6D    "mary@mail.com"
```

This example describes the complete format. String-offset and string-data encoding are not yet implemented.

### NULL and empty string

With `{ id = 1, name = NULL, age = 25, email = "" }`, the complete row would use `6 + 8 + 8 + 4 = 26` bytes. Both string end offsets are zero and there is no blob. In this format, bit 1 marks `name` as NULL, distinguishing it from the non-NULL empty `email`.

### UTF-8

With `{ id = 1, name = "é", age = 25, email = "" }`, the complete row would use 28 bytes. The string end offsets would be `[2, 2]` and the blob would be `C3 A9`. `String::len()` counts these two UTF-8 bytes when computing the allocation.

## See also

- [Schemas](../shared/schemas/schemas.md)
- [Rows](../shared/schemas/rows.md)
- [Page layout](../storage/page/layout.md)

## References

The layout is mainly inspired by the row format described in [Microsoft SQL Server 2012 Internals, pp. 257–270](https://www.microsoftpressstore.com/store/microsoft-sql-server-2012-internals-9780735658561).
