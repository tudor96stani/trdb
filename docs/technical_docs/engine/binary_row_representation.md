---
created: 2026-05-19
---

## High level
This document describes the protocol for the binary representation of records based on the schema of the table they belong to.
It goes over the actual format, provides the reasoning for it and shows a few examples.

Overall, the format is inspired by multiple existing RDBMS implementations, most heavily influenced by SQL Server - see (Delaney et al., 2013, p. 257-270). It follows the philosophy of separating the columns and storing them grouped by their data type. 

The overall goal is to allow the storage and random access retrieval of both fixed sized (integer) and variable length (string) columns.

**Note:** LOBs and rows that would not fit within a single page are outside the scope of this implementation.
This means that the largest record size allowed is one that would fit completely in an empty page:
`PAGE_SIZE - HEADER_SIZE - 1 * SLOT_SIZE = 4096 - 96 - 4 = 3996 bytes max`.
This will be enforced via max number of columns & max length for string columns. LOBs and `nvarchar(max)`-style features will be considered for future improvements.

## Layout
A record is stored on disk in a data page as a contiguous array of bytes tightly packed together - all the data necessary to decode the row is found in that array.

As with most other formats, it contains a header section and a data section:
```text
| header | fixed_size_columns | null_bitmap | var_len_column_offsets | var_len_data_blob |
 6 bytes   4 * number of int     8 bytes        2 * number of string   sum(len(str_cols))
             columns bytes                        columns bytes              bytes
```

### Header
The 6 byte header contains 3 small integers (`short`):
- total size of entire row, in bytes
- number of fixed sized (integer) columns
- number of variable length (string) columns

### Fixed sized section
Right after the header we have the fixed sized column section. In here, each integer is placed in a contiguous array, with no separators or markers. The columns are placed in the order in which they are defined in the schema. Because we only support integers, offset computation is trivial: each column is 4 bytes long, and we know from the header the total number of integer columns. 

### NULL bitmap
This is an 8 byte chunk (big int) representing a bitmap of all the null values within the row. This allows for a maximum of 64 columns to be tracked.
The bitmap itself is relatively straightforward: each column has a corresponding bit at the index that corresponds with its position within the schema - so the first column is tracked by bit with `index = 0`, 2nd column by bit with `index = 1` and so on. 
The bit is set if the value of that column is NULL and the row decoder consults the bitmap before interpreting the value of a column.

### Variable length column offset section
This section contains one 2-byte (short) entry for each string column in the schema. Namely, it stores the end offset of the corresponding string column relative to the start of the variable length data blob section start offset. Similarly to the fixed sized section, the string columns are placed in the order defined in the schema. 
Since we know the total number of string columns, we can compute the exact placement of this array and perform random access on it (because every entry in the array is fixed sized at 2 bytes). Once we have retrieved the information for the string column we are interested in, we can also compute the exact position of the data itself: the `array[index]` provides us with the end offset of the column, while `array[index-1]` will give us the end offset of the previous column - which is the start offset of the column we are interested in. 
This allows us to do random access to the variable length columns as well, with only some offset computation required. No sequential scan of the entire array is needed.

### Variable length data blob
This section stores the actual string values. Similarly to the fixed sized ones, they are stored contiguously, in the order defined in the schema, with no separators between them. The previous section gives us the necessary information to be able to decode and randomly access entries in this section.

## Example
The best way to illustrate this is with an example.


## Reasoning
- null bitmap: we chose a bigint for simplicity. max 64 columns. for fewer columns, we waste some bits, but it is what it is. could have gone with var-sized bitmap (based on number of columns), but meh.

## References
- [Delaney, K., Beauchemin, B., Cunningham, C., Kehayias, J., Freeman, C., Nevarez, B., & Randal, P. S. (2013). _Microsoft SQL Server 2012 Internals_](https://www.microsoftpressstore.com/store/microsoft-sql-server-2012-internals-9780735658561)