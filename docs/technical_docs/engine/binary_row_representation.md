---
created: 2026-05-19
---

Encoding turns an in-memory row's cells into bytes that a page can store. The binary row format groups fixed-size integers and variable-size strings so we can locate a column without scanning every value before it. Each integer occupies four bytes; a string's size depends on its UTF-8 contents.

A page stores the encoded row as one contiguous byte range. The row header describes that range, while the schema supplies column names, types and order. The bytes contain no schema identity or version, so the caller supplies the matching schema when decoding. Offsets in the format count bytes; they are not memory addresses.

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

Both column counts include NULL columns.

### Integers

Starting at byte 6, we store each integer as an `i32`, ordered by its position in the schema. Integer `j` starts at row-relative offset `6 + 4 * j`. A NULL integer still occupies four bytes, filled with zero; the bitmap distinguishes it from integer zero.

### NULL bitmap

The bitmap occupies eight bytes after the integers. Bit `p` describes the column at schema position `p`, independently of its ID or type. A set bit means NULL; a clear bit means non-NULL.

We use a fixed `u64` to keep the layout simple. It can track at most 64 columns, and unused bits remain zero.

### String offsets and data

After the bitmap, we keep one `u16` end offset for each string column, in schema position order. Each offset is relative to the beginning of the string-data section, not to the row or page.

To locate string `j`, we use its end offset and the previous string's end offset. The first string starts at zero. Strings are stored back-to-back as UTF-8, without separators. NULL and empty strings both add zero bytes to the data section; the bitmap tells them apart.

For example, end offsets `[4, 17]` mean the first string uses blob bytes `0..4` and the second uses `4..17`. We only need these two offsets to find the second string.

## Encoding

`to_binary(row, schema)` runs these steps:

1. We validate the schema column count and check the cells against the schema, then compute the complete byte count. Column counts come from the schema. A size above 3996 bytes returns `RowTooLarge` before allocating the output.
2. We allocate a zeroed vector of that size.
3. We convert the size and counts to `u16` and write the header. An overflow returns a conversion error.
4. We sort integer columns by schema position and write their values, using zero for NULL.
5. We walk all schema columns in sorted order, build the NULL bitmap and write its eight bytes.
6. We sort string columns by schema position. For each string, we add its UTF-8 byte length to the preceding end offset, write that offset and copy its bytes into the string-data section. NULL and empty strings repeat the preceding offset and add no bytes.
7. We return the completed vector.

### Schema checks

We match cells by the complete `SchemaEntryId`, including owner and column ID. Every schema column needs an explicit cell, including NULL values, and every supplied cell must belong to the schema. Non-NULL values must match the column's type. NULL is accepted for either type, including primary-key columns. Unknown identities, missing cells and type mismatches return a `RowSerializationError`.

Cells can arrive in any order: the encoder uses schema position order within each type group and for the bitmap. Serialization leaves the supplied row and schema unchanged.

### Page-size limit

The format keeps a whole row on one page. The complete size limit is `4096 - 96 - 4 = 3996` bytes: page size minus page header and one slot. Both encoding and decoding enforce this limit, including all row sections; exactly 3996 bytes is accepted for an otherwise valid row. The page planner separately determines whether the target page has enough available space for placement.

Large objects (LOBs) are values stored separately from the row's inline bytes. LOBs and rows spanning pages are not supported.

## Decoding

`from_binary(bytes, schema)` reads exactly one complete encoded row:

1. We reject input longer than 3996 bytes and read the header. Truncated fields return a binary error. The declared size must equal the supplied byte length, and the integer and string counts must match the schema. We also check that the bytes cover the header, integers, bitmap and offset array.
2. We read the NULL bitmap and reject any set bit beyond the schema's column count.
3. We read the integer section as little-endian `i32` values.
4. We read the string end offsets and use them to slice the string-data section. Offsets must be nondecreasing and within that section, and each individual string range must be valid UTF-8. The final offset must consume the entire section; with no string columns, no string-data bytes may remain.
5. We walk integer and string columns in schema position order within each group, using the bitmap to choose NULL or the decoded value. A NULL integer must have zero stored bytes, and a NULL string must have an empty byte range; otherwise we return `InvalidNullColumn`.
6. We build a row with one explicit cell per column, using the supplied schema's owner and column IDs. String cells own copies of the decoded text.

Malformed input returns a `RowDeserializationError` without a decoded row. Decoding leaves the supplied bytes and schema unchanged. Integer zero and empty strings remain distinct from NULL because the bitmap records nullness separately.

## Example

For this schema and row:

```text
id    int
name  string
age   int
email string

{ id = 1, name = "Mary", age = 25, email = "mary@mail.com" }
```

We have two integers, two strings and no NULLs. `Mary` uses four bytes and `mary@mail.com` uses thirteen, so the complete row uses `6 + 8 + 8 + 4 + 17 = 43` bytes.

```text
row-relative range   contents
0..6                 header: 43, 2, 2
6..14                integers: 1, 25
14..22               bitmap: 0
22..26               string end offsets: 4, 17
26..43               blob: "Mary" followed by "mary@mail.com"
```

### Complete bytes

The encoder returns these bytes, which the decoder reads back into the same values:

```text
2B 00 02 00 02 00                         header
01 00 00 00 19 00 00 00                   integers
00 00 00 00 00 00 00 00                   bitmap
04 00 11 00                               string end offsets
4D 61 72 79                               "Mary"
6D 61 72 79 40 6D 61 69 6C 2E 63 6F 6D    "mary@mail.com"
```


### NULL and empty string

With `{ id = 1, name = NULL, age = 25, email = "" }`, the complete row uses `6 + 8 + 8 + 4 = 26` bytes. Both string end offsets are zero and there is no blob. In this format, bit 1 marks `name` as NULL, distinguishing it from the non-NULL empty `email`.

### UTF-8

With `{ id = 1, name = "é", age = 25, email = "" }`, the complete row uses 28 bytes. The string end offsets are `[2, 2]` and the blob is `C3 A9`. `String::len()` counts these two UTF-8 bytes when computing the allocation.

## See also

- [Schemas](../shared/schemas/schemas.md)
- [Rows](../shared/schemas/rows.md)
- [Page layout](../storage/page/layout.md)

## References

The layout is mainly inspired by the row format described in [Microsoft SQL Server 2012 Internals, pp. 257–270](https://www.microsoftpressstore.com/store/microsoft-sql-server-2012-internals-9780735658561).
