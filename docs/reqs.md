# Requirements

## Active

### Binary Row Representation

| ID | Requirement |
|---|---|
| ROW-BIN-001 | Serialization and deserialization shall reject schemas containing zero columns or more than 64 columns. |
| ROW-BIN-002 | Serialization shall require exactly one cell for each schema column, including an explicit cell for each NULL value, and shall reject missing cells, unknown column IDs, and duplicate column IDs. |
| ROW-BIN-003 | Serialization shall reject any non-NULL cell whose value type differs from its schema column's type. |
| ROW-BIN-004 | Serialization shall accept NULL values for either supported column type, including columns marked as primary keys. |
| ROW-BIN-005 | Serialization shall produce one contiguous byte array laid out as a six-byte header, integer values, an eight-byte NULL bitmap, string end offsets, and string data, with no padding or other bytes. |
| ROW-BIN-006 | The header shall contain three little-endian u16 fields at byte offsets 0, 2, and 4: total encoded row size, schema integer-column count, and schema string-column count, respectively. |
| ROW-BIN-007 | For I integer columns, S string columns, and B bytes of non-NULL UTF-8 string data, the encoded row size shall be exactly 6 + 4 * I + 8 + 2 * S + B bytes. |
| ROW-BIN-008 | Serialization shall reject a row whose complete encoded size exceeds 3,996 bytes with a row-too-large error; a size of exactly 3,996 bytes shall be permitted when all other input conditions hold. |
| ROW-BIN-009 | Integer columns shall be encoded in ascending schema-position order within the integer category, using consecutive category indices starting at zero; each non-NULL integer shall occupy four bytes containing its little-endian i32 value. |
| ROW-BIN-010 | A NULL integer shall occupy four zero bytes. |
| ROW-BIN-011 | Bit k of the little-endian u64 NULL bitmap shall describe the column at index k in overall ascending schema-position order and shall be set exactly when that column is NULL. |
| ROW-BIN-012 | All NULL bitmap bits beyond the schema's column count shall be zero. |
| ROW-BIN-013 | String columns shall use consecutive category indices starting at zero in ascending schema-position order within the string category; each shall have one little-endian u16 end offset relative to the start of the string-data section. |
| ROW-BIN-014 | Non-NULL strings shall be stored as their exact UTF-8 byte sequences, concatenated in string-category order without separators; each end offset shall equal the cumulative string byte count through its column. |
| ROW-BIN-015 | NULL and non-NULL empty strings shall each contribute zero string-data bytes and retain the preceding end offset, or zero for the first string; their NULL bitmap bits shall distinguish them. |
| ROW-BIN-016 | Deserialization shall reject an input byte array whose length does not exactly match the header's total row size, including truncated input and trailing bytes. |
| ROW-BIN-017 | Deserialization shall reject an encoded row larger than 3,996 bytes. |
| ROW-BIN-018 | Deserialization shall reject header integer or string counts that differ from the supplied schema's corresponding counts. |
| ROW-BIN-019 | Deserialization shall reject a row whose declared layout cannot contain its complete header, integer section, NULL bitmap, and string-offset section. |
| ROW-BIN-020 | Deserialization shall reject string end offsets that decrease, exceed the string-data section's length, or fail to account for the entire string-data section; with no string columns, the string-data section shall be empty. |
| ROW-BIN-021 | Deserialization shall reject any non-NULL string's byte range that is not valid UTF-8. |
| ROW-BIN-022 | Deserialization shall reject nonzero unused NULL bitmap bits, nonzero bytes in a NULL integer, and a NULL string whose end offset differs from its preceding end offset, or from zero when it is the first string. |
| ROW-BIN-023 | Successful deserialization shall reconstruct exactly one cell per supplied schema column with that column's ID and the encoded integer, string, or NULL value. |
| ROW-BIN-024 | Serializing and then deserializing a valid row using the same schema shall preserve every column's exact value, including NULL, integer zero, empty strings, and UTF-8 strings, independently of in-memory cell iteration order. |
| ROW-BIN-025 | Invalid serialization inputs and malformed deserialization inputs shall return an error without panicking or returning a partial result. |

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

## Pending Review

## Superseded

## Rejected
