# Requirements

## Active

### Binary Row Representation

| ID | Requirement |
|---|---|
| ROW-BIN-001 | Serialization shall return an error unless the row contains exactly one cell for every supplied schema column, identified by both owner ID and column ID, including explicit cells for NULL values. |
| ROW-BIN-002 | Serialization shall produce exactly one contiguous byte array consisting, in order, of a six-byte header, four bytes per schema integer column, an eight-byte NULL bitmap, two bytes per schema string column for end offsets, and the string data, without padding; the header shall contain little-endian u16 values for the complete byte-array length, integer-column count, and string-column count at byte offsets 0, 2, and 4, respectively. |
| ROW-BIN-003 | Serialization shall encode integer columns in ascending schema-position order as four-byte little-endian i32 values, using four zero bytes for a NULL integer. |
| ROW-BIN-004 | Serialization shall encode the NULL bitmap as a little-endian u64 whose bit at each schema column's position is set exactly when that column is NULL, with all unused bits zero. |
| ROW-BIN-005 | Serialization shall concatenate non-NULL strings' exact UTF-8 bytes in ascending schema-position order and write one little-endian u16 end offset per string column, equal to the cumulative string byte count through that column and relative to the beginning of the string data; NULL and empty strings shall contribute zero bytes. |
| ROW-BIN-006 | Serialization shall return a row-too-large error when the complete encoded row exceeds 3,996 bytes and shall permit exactly 3,996 bytes when the row otherwise satisfies the serialization requirements. |
| ROW-BIN-007 | Given bytes conforming to the binary row format and the schema used to encode them, deserialization shall reconstruct exactly one cell per schema column using that column's owner ID and column ID and preserving its exact integer, string, or NULL value. |
| ROW-BIN-008 | Given a valid schema, deserialization shall return an error without panicking or returning a partial row if the supplied bytes do not conform to the binary row format in ROW-BIN-002 through ROW-BIN-005 or exceed the size limit in ROW-BIN-006. |

### Schemas

| ID | Requirement |
|---|---|
| SCHEMA-001 | A valid schema shall contain between 1 and 64 columns, inclusive. |
| SCHEMA-002 | Column names within a schema shall be unique under ASCII case-insensitive comparison, treating ASCII letters A-Z as equivalent to a-z and comparing all other characters exactly. |
| SCHEMA-003 | Column IDs within a schema shall be unique. |
| SCHEMA-004 | In a schema containing N columns, column positions shall be exactly the integers from 0 through N - 1, each occurring once. |
| SCHEMA-005 | Column IDs shall be independent of column positions: otherwise valid schemas shall permit distinct column IDs assigned to positions in any order, without requiring IDs to equal positions or to be contiguous. |

#### Schema Builder

| ID | Requirement |
|---|---|
| SCHEMA-BLD-001 | `build()` shall return an error when the supplied columns violate SCHEMA-001 through SCHEMA-004. |
| SCHEMA-BLD-002 | Successful construction shall preserve the supplied owner ID and each column's ID, name, type, position, and primary-key flag. |
| SCHEMA-BLD-003 | The builder shall accept and store an ordered list of primary-key column IDs through `.pk(...)` and pass that order unchanged to the built schema. |
| SCHEMA-BLD-004 | `build()` shall return an error if any column is marked as a primary key and no primary-key ordering list was supplied. |
| SCHEMA-BLD-005 | `build()` shall return an error if the primary-key ordering list supplied through `.pk(...)` contains a column ID that does not identify any supplied column. |
| SCHEMA-BLD-006 | `build()` shall return an error if the primary-key ordering list supplied through `.pk(...)` contains the same column ID more than once. |
| SCHEMA-BLD-007 | `build()` shall return an error if the primary-key ordering list supplied through `.pk(...)` omits the ID of any supplied column whose `is_pk` flag is true. |
| SCHEMA-BLD-008 | `build()` shall return an error if the primary-key ordering list supplied through `.pk(...)` includes the ID of any supplied column whose `is_pk` flag is false. |

## Pending Review

## Superseded

## Rejected
