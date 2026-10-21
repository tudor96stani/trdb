//! This module contains the implementation for the row interpreter helper functions
//!
//! These functions will convert an in memory representation of a data row (a `Row` instance) to and from a byte array as defined by the binary representation of a row.
use shared::binary_helpers::bin_error::BinaryError;
use shared::binary_helpers::bitmap::Bitmap;
use shared::binary_helpers::conversions::{ConversionError, UsizeConversion};
use shared::schemas::data_type::DataType;
use shared::schemas::identifiers::ColumnId;
use shared::schemas::row::{FIXED_LEN_SIZE, Row, RowCellValue};
use shared::schemas::schema::{Schema, SchemaEntryId};
use std::collections::HashSet;
use std::str::Utf8Error;
use thiserror::Error;

/// The size of the header of a row, in bytes
const HEADER_SIZE: usize = 6;

/// The size of the null bitmap, in bytes
const NULL_BITMAP_SIZE: usize = 8;

/// The size of an offset, used in the offsets array to store the end offset of each string from the row, in bytes
const OFFSET_SIZE: usize = 2;

/// Maximum complete encoded row size (ROW-BIN-008).
const MAX_ROW_SIZE: usize = 3_996;

/// Errors that can occur during the serialization of rows to binary
#[derive(Debug, Error)]
pub enum RowSerializationError {
    /// The schema has an invalid number of columns
    #[error("schema must contain between 1 and 64 columns, got {count}")]
    InvalidColumnCount {
        /// Number of schema columns.
        count: usize,
    },
    /// More than one cell references the same column.
    #[error("duplicate row cell for schema entry {schema_entry_id:?}")]
    DuplicateRowCell {
        /// ID referenced by multiple cells.
        schema_entry_id: SchemaEntryId,
    },
    /// The complete encoded row exceeds the maximum size a row can have
    #[error("encoded row exceeds the maximum size of {} bytes", MAX_ROW_SIZE)]
    RowTooLarge,
    /// The row references a schema entry that is not present in the provided schema.
    #[error(
        "row cell references schema entry {schema_entry_id:?}, but that entry is not present in the provided schema"
    )]
    MissingSchemaEntryForRowCell {
        /// The schema entry id referenced by the row cell.
        schema_entry_id: SchemaEntryId,
    },
    /// The row is missing a cell for a column in the provided schema.
    #[error(
        "row cell does not contain schema entry {schema_entry_id:?}, but that entry is present in the provided schema"
    )]
    MissingRowCellForSchemaEntry {
        /// The schema entry id missing the row cell.
        schema_entry_id: SchemaEntryId,
    },
    /// Error during conversion between data types
    #[error("Error while converting between data types: {0}")]
    Conversion(#[from] ConversionError),
    /// Mismatch between the data type that was expected and the one that was found
    #[error(
        "schema entry {schema_entry_id:?} has mismatched data type: expected {expected}, got {found}"
    )]
    MismatchedTypes {
        /// Identity of the column whose value has the wrong type.
        schema_entry_id: SchemaEntryId,
        /// Expected type
        expected: DataType,
        /// Found type
        found: DataType,
    },
    /// The target binary buffer is not large enough for the requested write.
    #[error("Buffer too small: needed {needed} bytes, got {actual} bytes")]
    BufferTooSmall {
        /// Minimum required buffer length.
        needed: usize,
        /// Actual buffer length.
        actual: usize,
    },
    /// An error occurred while performing binary operations on the row
    #[error("Error while performing binary operations: {0}")]
    Binary(#[from] BinaryError),
}

/// Errors that can occur during the serialization of a row from binary.
#[derive(Debug, Error)]
pub enum RowDeserializationError {
    /// The unused bits from the NULL bitmap (from schema.len()..64) are set instead of 0
    #[error("unused NULL bitmap bits must be zero")]
    InvalidNullBitmap,
    /// String end offsets decrease or exceed the available string data.
    #[error(
        "string at index {index} has invalid byte range {start_offset}..{end_offset} in string data of length {string_data_len}; offsets must be nondecreasing and within the string data"
    )]
    InvalidStringOffsets {
        /// Zero-based index among string columns in schema position order.
        index: usize,
        /// Start offset relative to the beginning of string data.
        start_offset: usize,
        /// Supplied end offset relative to the beginning of string data.
        end_offset: usize,
        /// Available string-data bytes.
        string_data_len: usize,
    },
    /// Bytes remain after the final string end offset.
    #[error("row contains bytes beyond the string data")]
    TrailingStringData,
    /// A column marked as NULL contains a nonzero integer or a nonempty string.
    #[error("column {column_id} is marked as NULL but contains data")]
    InvalidNullColumn {
        /// ID of the column with an invalid NULL representation.
        column_id: ColumnId,
    },
    /// Header column counts differ from the supplied schema.
    #[error(
        "header column counts do not match the schema: expected {expected_integers} integer and {expected_strings} string columns, got {actual_integers} integer and {actual_strings} string columns"
    )]
    MismatchedColumnCounts {
        /// Integer column count required by the schema.
        expected_integers: usize,
        /// Integer column count declared in the header.
        actual_integers: usize,
        /// String column count required by the schema.
        expected_strings: usize,
        /// String column count declared in the header.
        actual_strings: usize,
    },
    /// The complete encoded row exceeds the maximum size a row can have
    #[error("encoded row exceeds the maximum size of {} bytes", MAX_ROW_SIZE)]
    RowTooLarge,
    /// An error occurred while performing binary operations on the row
    #[error("Error while performing binary operations: {0}")]
    Binary(#[from] BinaryError),
    /// The length of the supplied bytes does not match what is declared in the header.
    #[error("Invalid row size: expected {expected} bytes (from header), got {actual} bytes")]
    MismatchedSize {
        /// Size in bytes read from the header of the row
        expected: usize,
        /// Actual number of supplied bytes.
        actual: usize,
    },
    /// Error while trying to deserialize an integer column from the binary array of a row
    #[error("Failed to deserialize integer column at index {index}: {source}")]
    IntegerDeserialization {
        /// Index of the integer column that could not be deserialized
        index: usize,
        /// Actual error source
        #[source]
        source: BinaryError,
    },
    /// Error while deserializing the string offset from the binary array of a row
    #[error("Failed to deserialize string end offset for column at index {index}: {source}")]
    StringOffsetDeserialization {
        /// Index of the string offset that could not be deserialized
        index: usize,
        /// Actual error source
        #[source]
        source: BinaryError,
    },
    /// Error while trying to deserialize a string column from the binary array of a row
    #[error("Error while reading the string column at index {index}: {source}")]
    StringDeserialization {
        /// Index of the column that could not be deserialized
        index: usize,
        /// Actual error source
        #[source]
        source: Utf8Error,
    },
}

struct RowMetadata {
    row_size_bytes: usize,
    nb_fixed: usize,
    nb_var: usize,
}

/// Encode the provided `Row` of the provided `Schema` into a raw byte array.
pub fn to_binary(row: &Row, schema: &Schema) -> Result<Vec<u8>, RowSerializationError> {
    let metadata = validate_before_serialization(row, schema)?;
    let mut result_bytes: Vec<u8> = vec![0u8; metadata.row_size_bytes];

    let values = [
        metadata.row_size_bytes.to_u16()?,
        metadata.nb_fixed.to_u16()?,
        metadata.nb_var.to_u16()?,
    ];
    for (index, value) in values.iter().enumerate() {
        result_bytes[index * 2..index * 2 + 2].copy_from_slice(&value.to_le_bytes());
    }

    // Populate fixed sized columns
    let mut fixed_sized_schema_entries: Vec<_> = schema
        .columns()
        .iter()
        .filter(|(_, entry)| entry.is_fixed_size())
        .map(|(_, entry)| entry)
        .collect();
    fixed_sized_schema_entries.sort_by_key(|entry| entry.position());
    // The fixed sized columns start from this index
    let base_index = HEADER_SIZE;
    for (index, entry) in fixed_sized_schema_entries.iter().enumerate() {
        let row_cell =
            row.get(&entry.id())
                .ok_or(RowSerializationError::MissingSchemaEntryForRowCell {
                    schema_entry_id: entry.id(),
                })?;
        let value = match row_cell.value() {
            RowCellValue::Int(value) => *value,
            RowCellValue::NULL => 0,
            RowCellValue::String(_) => {
                return Err(RowSerializationError::MismatchedTypes {
                    schema_entry_id: entry.id(),
                    expected: DataType::Int,
                    found: DataType::String,
                });
            }
        };
        // Start writing this integer at base + all previous integers
        let start_offset = base_index + index * FIXED_LEN_SIZE;
        result_bytes[start_offset..start_offset + FIXED_LEN_SIZE]
            .copy_from_slice(&value.to_le_bytes());
    }

    // Populate null bitmap
    let mut bitmap = Bitmap::new();
    for (index, entry) in schema.columns_sorted().enumerate() {
        let value =
            row.get(&entry.id())
                .ok_or(RowSerializationError::MissingRowCellForSchemaEntry {
                    schema_entry_id: entry.id(),
                })?;
        if matches!(value.value(), RowCellValue::NULL) {
            bitmap.set(index);
        }
    }
    let bitmap_bytes = bitmap.to_bytes()?;
    let bitmap_start = HEADER_SIZE + FIXED_LEN_SIZE * metadata.nb_fixed;
    result_bytes[bitmap_start..bitmap_start + NULL_BITMAP_SIZE].copy_from_slice(&bitmap_bytes);

    // Populate var len offsets array
    let mut string_schema_entries: Vec<_> = schema
        .columns()
        .iter()
        .filter(|(_, entry)| !entry.is_fixed_size())
        .map(|(_, entry)| entry)
        .collect();
    string_schema_entries.sort_by_key(|entry| entry.position());
    // this is where the offsets array will start - used to know where to write the offset in the byte array. virtual/relative index, not actual position in the main byte array
    let offsets_array_base_index =
        HEADER_SIZE + metadata.nb_fixed * FIXED_LEN_SIZE + NULL_BITMAP_SIZE;
    // this index is used to track what is the end offset (relative to the start of the array of offsets) for the column we are currently tracking.
    let mut offsets_array_collector_index = 0;
    // this is where we will write the actual strings (initialized to the place where we will write the first string, updated by the loop as more string are added). concrete index, actual position in the main byte array
    let mut string_blob_collector = offsets_array_base_index + metadata.nb_var * OFFSET_SIZE;
    for (index, entry) in string_schema_entries.iter().enumerate() {
        let row_cell =
            row.get(&entry.id())
                .ok_or(RowSerializationError::MissingSchemaEntryForRowCell {
                    schema_entry_id: entry.id(),
                })?;
        let string_value = match row_cell.value() {
            RowCellValue::String(value) => value,
            RowCellValue::NULL => "",
            RowCellValue::Int(_) => {
                return Err(RowSerializationError::MismatchedTypes {
                    schema_entry_id: entry.id(),
                    expected: DataType::String,
                    found: DataType::Int,
                });
            }
        };
        // First let's write the entry in the array
        let offset_starting_index = offsets_array_base_index + index * OFFSET_SIZE;
        // Since this string will be placed right after the previous one, we take the previous end offset we had and add this one's size (first run collector will be 0)
        let offset_to_write = (offsets_array_collector_index + string_value.len()).to_u16()?;
        result_bytes[offset_starting_index..offset_starting_index + OFFSET_SIZE]
            .copy_from_slice(&offset_to_write.to_le_bytes());
        offsets_array_collector_index += string_value.len();

        // Now let's write the actual string
        result_bytes[string_blob_collector..string_blob_collector + string_value.len()]
            .copy_from_slice(string_value.as_bytes());
        string_blob_collector += string_value.len();
    }

    Ok(result_bytes)
}

/// Decodes a raw binary array into a `Row` of the provided `Schema`.
pub fn from_binary(bytes: &[u8], schema: &Schema) -> Result<Row, RowDeserializationError> {
    let metadata = validate_before_deserialization(bytes, schema)?;

    // Read NULL bitmap
    let bitmap: Bitmap;
    {
        let null_bitmap_start = HEADER_SIZE + metadata.nb_fixed * FIXED_LEN_SIZE;
        let null_bitmap_value =
            shared::binary_helpers::le::read_le::<u64>(bytes, null_bitmap_start)?;
        let column_count = schema.columns().len();
        if column_count < 64 && null_bitmap_value >> column_count != 0 {
            return Err(RowDeserializationError::InvalidNullBitmap);
        }
        bitmap = Bitmap::from_u64(null_bitmap_value);
    }

    // Read integers
    let mut integers: Vec<i32> = vec![];
    {
        let integers_start_offset = HEADER_SIZE;
        for i in 0..metadata.nb_fixed {
            // Transform the error if it occurs, just for nicer debugging
            let value = shared::binary_helpers::le::read_le::<i32>(
                bytes,
                integers_start_offset + i * FIXED_LEN_SIZE,
            )
            .map_err(|source| RowDeserializationError::IntegerDeserialization {
                index: i,
                source,
            })?;
            integers.push(value);
        }
    }

    // Read strings
    let mut strings: Vec<&str> = vec![];
    {
        // this is where the offsets array starts
        let offsets_array_start =
            HEADER_SIZE + metadata.nb_fixed * FIXED_LEN_SIZE + NULL_BITMAP_SIZE;
        // this is where the string blob starts
        let string_blob_start = offsets_array_start + metadata.nb_var * OFFSET_SIZE;
        // the position to which we currently read in the string blob (basically, the end index of the previous string we just read) - starts at the start of the blob since we did not read anything yet
        let mut cursor_for_already_read_strings = string_blob_start;
        for i in 0..metadata.nb_var {
            // where the offsets array tells us the current string ends
            let end_offset_of_current_string = shared::binary_helpers::le::read_le::<u16>(
                bytes,
                offsets_array_start + i * OFFSET_SIZE,
            )
            .map_err(|source| {
                RowDeserializationError::StringOffsetDeserialization { index: i, source }
            })?;

            // make it usize
            let end_offset_of_current_string = usize::from(end_offset_of_current_string);

            // we must read from the end of the previous string (stored in `cursor_for_current_string`) to the offset from the array
            // the value from the array must be added to the start of the string blob (always) to get the real end offset within the main byte array
            // if, however, the current string ends at the same offset as the previous, then we know it's either an empty string or NULL - we will determine that later.
            let end = string_blob_start + end_offset_of_current_string;

            // Checked slicing rejects decreasing offsets and offsets beyond the blob.
            let string_bytes = bytes.get(cursor_for_already_read_strings..end).ok_or(
                RowDeserializationError::InvalidStringOffsets {
                    index: i,
                    start_offset: cursor_for_already_read_strings - string_blob_start,
                    end_offset: end_offset_of_current_string,
                    string_data_len: bytes.len() - string_blob_start,
                },
            )?;

            // At this point, both NULL values and actual empty strings values will produce an empty string in this array
            // we will determine later one which one is it using the bitmap
            let string = str::from_utf8(string_bytes).map_err(|source| {
                RowDeserializationError::StringDeserialization { index: i, source }
            })?;

            strings.push(string);
            // update the cursor
            cursor_for_already_read_strings = string_blob_start + end_offset_of_current_string;
        }
        if cursor_for_already_read_strings != bytes.len() {
            return Err(RowDeserializationError::TrailingStringData);
        }
    }

    let mut builder = Row::builder(schema);

    let mut fixed_columns: Vec<_> = schema
        .columns()
        .iter()
        .filter(|(_, s)| s.is_fixed_size())
        .collect();
    fixed_columns.sort_by_key(|(_, e)| e.position());
    for (i, (_, entry)) in fixed_columns.iter().enumerate() {
        let value = if bitmap.check(entry.position()) {
            // expected a NULL at this position, but the array stores non-zero bytes. error !
            if integers[i] != 0 {
                return Err(RowDeserializationError::InvalidNullColumn {
                    column_id: entry.id().column_id(),
                });
            }
            RowCellValue::NULL
        } else {
            RowCellValue::Int(integers[i])
        };
        builder = builder.cell(entry, value);
    }

    let mut string_columns: Vec<_> = schema
        .columns()
        .iter()
        .filter(|(_, s)| !s.is_fixed_size())
        .collect();
    string_columns.sort_by_key(|(_, e)| e.position());
    for (i, (_, entry)) in string_columns.iter().enumerate() {
        let value = if bitmap.check(entry.position()) {
            // we were expecting a null string
            if !strings[i].is_empty() {
                return Err(RowDeserializationError::InvalidNullColumn {
                    column_id: entry.id().column_id(),
                });
            }
            RowCellValue::NULL
        } else {
            RowCellValue::String(strings[i].to_string())
        };
        builder = builder.cell(entry, value);
    }

    Ok(builder.build())
}

fn compute_metadata(row: &Row, schema: &Schema) -> Result<RowMetadata, RowSerializationError> {
    let size = compute_row_size_in_bytes(row, schema)?;

    let nb_fixed_cols = schema
        .columns()
        .iter()
        .filter(|(_, entry)| entry.is_fixed_size())
        .count();

    let nb_var_len_cols = schema
        .columns()
        .iter()
        .filter(|(_, entry)| !entry.is_fixed_size())
        .count();

    Ok(RowMetadata {
        row_size_bytes: size,
        nb_fixed: nb_fixed_cols,
        nb_var: nb_var_len_cols,
    })
}

fn compute_row_size_in_bytes(row: &Row, schema: &Schema) -> Result<usize, RowSerializationError> {
    // start with the minimum we know we will have - the header and the bitmap
    let mut total = HEADER_SIZE + NULL_BITMAP_SIZE;

    for cell in row.values() {
        let schema_entry = schema.columns().get(&cell.schema_entry_id()).ok_or(
            RowSerializationError::MissingSchemaEntryForRowCell {
                schema_entry_id: cell.schema_entry_id(),
            },
        )?;

        let cell_size = match schema_entry.data_type() {
            // for integers, we only need to add 4 bytes each.
            DataType::Int => FIXED_LEN_SIZE,
            DataType::String => match cell.value() {
                // we have to drill into the actual value to find its length
                RowCellValue::String(s) => {
                    // Bound the length before adding the offset bytes.
                    if s.len() > MAX_ROW_SIZE {
                        return Err(RowSerializationError::RowTooLarge);
                    }
                    // add both its actual length
                    // and two bytes for the offsets array
                    s.len() + OFFSET_SIZE
                }
                RowCellValue::NULL => {
                    // for a NULL string, only add 2 bytes for the offsets array
                    OFFSET_SIZE
                }
                RowCellValue::Int(_) => {
                    return Err(RowSerializationError::MismatchedTypes {
                        schema_entry_id: schema_entry.id(),
                        expected: DataType::String,
                        found: DataType::Int,
                    });
                }
            },
        };
        total += cell_size;
        if total > MAX_ROW_SIZE {
            return Err(RowSerializationError::RowTooLarge);
        }
    }

    Ok(total)
}

fn validate_before_serialization(
    row: &Row,
    schema: &Schema,
) -> Result<RowMetadata, RowSerializationError> {
    let count = schema.columns().len();
    if count == 0 || count > 64 {
        return Err(RowSerializationError::InvalidColumnCount { count });
    }

    let mut seen = HashSet::new();
    for cell in row.values() {
        let schema_entry_id = cell.schema_entry_id();
        let entry = schema
            .columns()
            .get(&schema_entry_id)
            .ok_or(RowSerializationError::MissingSchemaEntryForRowCell { schema_entry_id })?;
        if !seen.insert(schema_entry_id) {
            return Err(RowSerializationError::DuplicateRowCell { schema_entry_id });
        }
        match (entry.data_type(), cell.value()) {
            (DataType::Int, RowCellValue::String(_)) => {
                return Err(RowSerializationError::MismatchedTypes {
                    schema_entry_id,
                    expected: DataType::Int,
                    found: DataType::String,
                });
            }
            (DataType::String, RowCellValue::Int(_)) => {
                return Err(RowSerializationError::MismatchedTypes {
                    schema_entry_id,
                    expected: DataType::String,
                    found: DataType::Int,
                });
            }
            // NULL is valid for every column, including primary keys.
            _ => {}
        }
    }

    for entry in schema.columns().values() {
        if row.get(&entry.id()).is_none() {
            return Err(RowSerializationError::MissingRowCellForSchemaEntry {
                schema_entry_id: entry.id(),
            });
        }
    }

    compute_metadata(row, schema)
}

fn validate_before_deserialization(
    bytes: &[u8],
    schema: &Schema,
) -> Result<RowMetadata, RowDeserializationError> {
    if bytes.len() > MAX_ROW_SIZE {
        return Err(RowDeserializationError::RowTooLarge);
    }

    // extract row size from header - stored as a u16 - then convert it to desired usize
    let row_size_bytes = usize::from(shared::binary_helpers::le::read_le::<u16>(bytes, 0)?);

    // Ensure the declared row size in the header matches the actual length of the byte array
    {
        if row_size_bytes != bytes.len() {
            return Err(RowDeserializationError::MismatchedSize {
                expected: row_size_bytes,
                actual: bytes.len(),
            });
        }
    }

    let nb_fixed = usize::from(shared::binary_helpers::le::read_le::<u16>(bytes, 2)?);
    let nb_var = usize::from(shared::binary_helpers::le::read_le::<u16>(bytes, 4)?);

    let expected_fixed = schema
        .columns()
        .values()
        .filter(|entry| entry.is_fixed_size())
        .count();
    let expected_var = schema.columns().len() - expected_fixed;
    if nb_fixed != expected_fixed || nb_var != expected_var {
        return Err(RowDeserializationError::MismatchedColumnCounts {
            expected_integers: expected_fixed,
            actual_integers: nb_fixed,
            expected_strings: expected_var,
            actual_strings: nb_var,
        });
    }

    let minimum_size =
        HEADER_SIZE + nb_fixed * FIXED_LEN_SIZE + NULL_BITMAP_SIZE + nb_var * OFFSET_SIZE;
    if bytes.len() < minimum_size {
        return Err(RowDeserializationError::MismatchedSize {
            expected: minimum_size,
            actual: bytes.len(),
        });
    }

    Ok(RowMetadata {
        row_size_bytes,
        nb_fixed,
        nb_var,
    })
}

#[cfg(test)]
mod test_cases {
    use super::*;
    use crate::row_interpreter::test_helpers::{
        create_row, create_schema, encoded_row_with_value_7_and_label_hi, entry_named,
        schema_with_int_value_and_string_label,
    };

    fn hex(s: &str) -> Vec<u8> {
        s.split_whitespace()
            .map(|byte| u8::from_str_radix(byte, 16).unwrap())
            .collect()
    }

    // TC-ROW-SER-001 — Serialize valid rows, including NULL/empty distinction and UTF-8 lengths
    #[test]
    fn tc_row_ser_001_valid_rows() {
        let schema = create_schema();
        let row = create_row(&schema);
        assert_eq!(
            to_binary(&row, &schema).unwrap(),
            hex(
                "2B 00 02 00 02 00 01 00 00 00 19 00 00 00 00 00 00 00 00 00 00 00 04 00 11 00 4D 61 72 79 6D 61 72 79 40 6D 61 69 6C 2E 63 6F 6D"
            )
        );

        let row = Row::builder(&schema)
            .value_named("id", 1)
            .value_named("name", RowCellValue::NULL)
            .value_named("age", 25)
            .value_named("email", "")
            .build();
        assert_eq!(
            to_binary(&row, &schema).unwrap(),
            hex("1A 00 02 00 02 00 01 00 00 00 19 00 00 00 02 00 00 00 00 00 00 00 00 00 00 00")
        );

        let row = Row::builder(&schema)
            .value_named("id", 1)
            .value_named("name", "é")
            .value_named("age", 25)
            .value_named("email", "")
            .build();
        assert_eq!(
            to_binary(&row, &schema).unwrap(),
            hex(
                "1C 00 02 00 02 00 01 00 00 00 19 00 00 00 00 00 00 00 00 00 00 00 02 00 02 00 C3 A9"
            )
        );
    }

    // TC-ROW-SER-002 — Reject rows with an invalid cell-to-schema correspondence
    #[test]
    fn tc_row_ser_002_invalid_correspondence() {
        let schema = create_schema();
        let row = Row::builder(&schema)
            .value(ColumnId::new(0), 1)
            .value(ColumnId::new(2), 25)
            .value(ColumnId::new(3), "x")
            .build();
        assert!(
            matches!(to_binary(&row, &schema), Err(RowSerializationError::MissingRowCellForSchemaEntry { schema_entry_id }) if schema_entry_id == entry_named(&schema, "name").id())
        );
        let other = Schema::builder(2)
            .column(ColumnId::new(7), "value", DataType::Int, 0, false)
            .build()
            .unwrap();
        let row = Row::builder(&other).value(ColumnId::new(7), 1).build();
        assert!(
            matches!(to_binary(&row, &schema), Err(RowSerializationError::MissingSchemaEntryForRowCell { schema_entry_id }) if schema_entry_id.owner_id() == 2 && schema_entry_id.column_id() == ColumnId::new(7))
        );
    }

    #[test]
    fn type_mismatch_reports_column_identity_and_types() {
        for (expected_type, supplied_type, value) in [
            (
                DataType::Int,
                DataType::String,
                RowCellValue::from("wrong type"),
            ),
            (DataType::String, DataType::Int, RowCellValue::from(7)),
        ] {
            // Equal identities in different schemas let us supply a value of the wrong type.
            let target = Schema::builder(17)
                .column(ColumnId::new(42), "value", expected_type, 0, false)
                .build()
                .unwrap();
            let source = Schema::builder(17)
                .column(ColumnId::new(42), "value", supplied_type, 0, false)
                .build()
                .unwrap();
            let row = Row::builder(&source)
                .value(ColumnId::new(42), value)
                .build();
            let error = to_binary(&row, &target).unwrap_err();
            let target_entry = entry_named(&target, "value");
            let source_entry = entry_named(&source, "value");
            match &error {
                RowSerializationError::MismatchedTypes {
                    schema_entry_id,
                    expected,
                    found,
                } => {
                    assert_eq!(*schema_entry_id, target_entry.id());
                    assert_eq!(expected, target_entry.data_type());
                    assert_eq!(found, source_entry.data_type());
                }
                other => panic!("expected type mismatch, got {other:?}"),
            }
            assert_eq!(
                error.to_string(),
                format!(
                    "schema entry {:?} has mismatched data type: expected {}, got {}",
                    target_entry.id(),
                    target_entry.data_type(),
                    source_entry.data_type(),
                )
            );
        }
    }

    // TC-ROW-SER-004 — Enforce the 3996-byte maximum row size
    #[test]
    fn tc_row_ser_004_maximum_size() {
        let schema = Schema::builder(1)
            .column(ColumnId::new(7), "number", DataType::Int, 0, false)
            .column(ColumnId::new(99), "text", DataType::String, 1, false)
            .column(ColumnId::new(42), "empty", DataType::String, 2, false)
            .build()
            .unwrap();
        let text = "é".repeat(1987);
        let row = Row::builder(&schema)
            .value(ColumnId::new(7), RowCellValue::NULL)
            .value(ColumnId::new(99), text.clone())
            .value(ColumnId::new(42), RowCellValue::NULL)
            .build();
        let encoded = to_binary(&row, &schema).unwrap();
        assert_eq!(encoded.len(), 3996);
        assert_eq!(&encoded[..2], &[0x9C, 0x0F]);
        assert_eq!(&encoded[22..], text.as_bytes());
        let text = format!("{text}a");
        let row = Row::builder(&schema)
            .value(ColumnId::new(7), RowCellValue::NULL)
            .value(ColumnId::new(99), text)
            .value(ColumnId::new(42), RowCellValue::NULL)
            .build();
        assert!(matches!(
            to_binary(&row, &schema),
            Err(RowSerializationError::RowTooLarge)
        ));
    }

    // TC-ROW-SER-005 — Accept the maximum 64-column schema with every column NULL
    #[test]
    fn tc_row_ser_005_64_null_columns() {
        let mut builder = Schema::builder(1);
        for position in 0..64 {
            builder = builder.column(
                ColumnId::new(position),
                format!("column_{position}"),
                if position % 2 == 0 {
                    DataType::Int
                } else {
                    DataType::String
                },
                usize::from(position),
                position < 2,
            );
        }
        let schema = builder
            .pk([ColumnId::new(0), ColumnId::new(1)])
            .build()
            .unwrap();
        let mut row = Row::builder(&schema);
        for position in 0..64 {
            row = row.value(ColumnId::new(position), RowCellValue::NULL);
        }
        let bytes = to_binary(&row.build(), &schema).unwrap();
        assert_eq!(bytes.len(), 206);
        assert_eq!(&bytes[..6], &[0xCE, 0, 0x20, 0, 0x20, 0]);
        assert!(bytes[6..134].iter().all(|b| *b == 0));
        assert!(bytes[134..142].iter().all(|b| *b == 0xFF));
        assert!(bytes[142..].iter().all(|b| *b == 0));
        assert!(from_binary(&bytes, &schema).is_ok());
    }

    // TC-ROW-DES-001 — Deserialize valid rows, including NULL/empty distinction and UTF-8 lengths
    #[test]
    fn tc_row_des_001_valid_rows() {
        let schema = create_schema();
        for (bytes, expected) in [
            (
                "2B 00 02 00 02 00 01 00 00 00 19 00 00 00 00 00 00 00 00 00 00 00 04 00 11 00 4D 61 72 79 6D 61 72 79 40 6D 61 69 6C 2E 63 6F 6D",
                vec!["1", "Mary", "25", "mary@mail.com"],
            ),
            (
                "1A 00 02 00 02 00 01 00 00 00 19 00 00 00 02 00 00 00 00 00 00 00 00 00 00 00",
                vec!["1", "NULL", "25", ""],
            ),
            (
                "1C 00 02 00 02 00 01 00 00 00 19 00 00 00 00 00 00 00 00 00 00 00 02 00 02 00 C3 A9",
                vec!["1", "é", "25", ""],
            ),
        ] {
            let row = from_binary(&hex(bytes), &schema).unwrap();
            for (name, value) in ["id", "name", "age", "email"].into_iter().zip(expected) {
                let cell = row.get(&entry_named(&schema, name).id()).unwrap();
                match value {
                    "NULL" => assert!(matches!(cell.value(), RowCellValue::NULL)),
                    _ if name == "id" || name == "age" => assert!(
                        matches!(cell.value(), RowCellValue::Int(actual) if *actual == value.parse::<i32>().unwrap())
                    ),
                    _ => assert!(
                        matches!(cell.value(), RowCellValue::String(actual) if actual == value)
                    ),
                }
            }
        }
    }

    // TC-ROW-DES-002 — Round-trip is independent of row-cell insertion order
    #[test]
    fn tc_row_des_002_order_independent_round_trip() {
        let schema = create_schema();
        let first = Row::builder(&schema)
            .value(ColumnId::new(0), 1)
            .value(ColumnId::new(1), "Mary")
            .value(ColumnId::new(2), 25)
            .value(ColumnId::new(3), "mary@mail.com")
            .build();
        let second = Row::builder(&schema)
            .value(ColumnId::new(3), "mary@mail.com")
            .value(ColumnId::new(2), 25)
            .value(ColumnId::new(1), "Mary")
            .value(ColumnId::new(0), 1)
            .build();
        let expected = hex(
            "2B 00 02 00 02 00 01 00 00 00 19 00 00 00 00 00 00 00 00 00 00 00 04 00 11 00 4D 61 72 79 6D 61 72 79 40 6D 61 69 6C 2E 63 6F 6D",
        );
        assert_eq!(to_binary(&first, &schema).unwrap(), expected);
        assert_eq!(to_binary(&second, &schema).unwrap(), expected);
        for row in [
            from_binary(&to_binary(&first, &schema).unwrap(), &schema).unwrap(),
            from_binary(&to_binary(&second, &schema).unwrap(), &schema).unwrap(),
        ] {
            assert!(matches!(
                row.get(&entry_named(&schema, "id").id()).unwrap().value(),
                RowCellValue::Int(1)
            ));
            assert!(
                matches!(row.get(&entry_named(&schema, "name").id()).unwrap().value(), RowCellValue::String(value) if value == "Mary")
            );
            assert!(matches!(
                row.get(&entry_named(&schema, "age").id()).unwrap().value(),
                RowCellValue::Int(25)
            ));
            assert!(
                matches!(row.get(&entry_named(&schema, "email").id()).unwrap().value(), RowCellValue::String(value) if value == "mary@mail.com")
            );
        }
    }

    // TC-ROW-DES-003 — Reject malformed row framing
    #[test]
    fn tc_row_des_003_malformed_framing() {
        let schema = schema_with_int_value_and_string_label();
        assert!(matches!(
            from_binary(&vec![0; 3997], &schema),
            Err(RowDeserializationError::RowTooLarge)
        ));
        assert!(matches!(
            from_binary(&[0], &schema),
            Err(RowDeserializationError::Binary(_))
        ));
        let mut bytes = encoded_row_with_value_7_and_label_hi();
        bytes[..2].copy_from_slice(&[0x17, 0]);
        assert!(matches!(
            from_binary(&bytes, &schema),
            Err(RowDeserializationError::MismatchedSize {
                expected: 23,
                actual: 22
            })
        ));
        let mut bytes = encoded_row_with_value_7_and_label_hi();
        bytes[2..4].copy_from_slice(&[2, 0]);
        assert!(matches!(
            from_binary(&bytes, &schema),
            Err(RowDeserializationError::MismatchedColumnCounts {
                expected_integers: 1,
                actual_integers: 2,
                expected_strings: 1,
                actual_strings: 1,
            })
        ));
        let mut bytes = encoded_row_with_value_7_and_label_hi()[..18].to_vec();
        bytes[..2].copy_from_slice(&[0x12, 0]);
        assert!(matches!(
            from_binary(&bytes, &schema),
            Err(RowDeserializationError::MismatchedSize {
                expected: 20,
                actual: 18
            })
        ));
    }

    #[test]
    fn column_count_mismatch_reports_expected_and_actual_counts() {
        let schema = schema_with_int_value_and_string_label();
        for (header_offset, actual_integers, actual_strings) in [(2, 2, 1), (4, 1, 2)] {
            let mut bytes = encoded_row_with_value_7_and_label_hi();
            bytes[header_offset..header_offset + 2].copy_from_slice(&2u16.to_le_bytes());
            let error = from_binary(&bytes, &schema).unwrap_err();
            assert!(matches!(
                error,
                RowDeserializationError::MismatchedColumnCounts {
                    expected_integers: 1,
                    actual_integers: integers,
                    expected_strings: 1,
                    actual_strings: strings,
                } if integers == actual_integers && strings == actual_strings
            ));
            assert_eq!(
                error.to_string(),
                format!(
                    "header column counts do not match the schema: expected 1 integer and 1 string columns, got {actual_integers} integer and {actual_strings} string columns"
                )
            );
        }
    }

    // TC-ROW-DES-004 — Reject invalid NULL representations
    #[test]
    fn tc_row_des_004_invalid_null_representations() {
        let schema = schema_with_int_value_and_string_label();
        let mut bytes = encoded_row_with_value_7_and_label_hi();
        bytes[10..18].copy_from_slice(&[4, 0, 0, 0, 0, 0, 0, 0]);
        assert!(matches!(
            from_binary(&bytes, &schema),
            Err(RowDeserializationError::InvalidNullBitmap)
        ));
        let mut bytes = encoded_row_with_value_7_and_label_hi();
        bytes[10..18].copy_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0]);
        assert!(matches!(
            from_binary(&bytes, &schema),
            Err(RowDeserializationError::InvalidNullColumn { column_id }) if column_id == ColumnId::new(5)
        ));
        let mut bytes = encoded_row_with_value_7_and_label_hi();
        bytes[10..18].copy_from_slice(&[2, 0, 0, 0, 0, 0, 0, 0]);
        assert!(matches!(
            from_binary(&bytes, &schema),
            Err(RowDeserializationError::InvalidNullColumn { column_id }) if column_id == ColumnId::new(6)
        ));
    }

    // TC-ROW-DES-005 — Reject invalid string sections
    #[test]
    fn tc_row_des_005_invalid_string_sections() {
        let schema = Schema::builder(9)
            .column(ColumnId::new(10), "first", DataType::String, 0, false)
            .column(ColumnId::new(11), "second", DataType::String, 1, false)
            .build()
            .unwrap();
        let baseline = hex("17 00 00 00 02 00 00 00 00 00 00 00 00 00 02 00 05 00 61 62 63 64 65");
        assert_eq!(baseline.len(), 23);
        let row = from_binary(&baseline, &schema).unwrap();
        assert!(matches!(
            row.get(&schema.columns().values().find(|entry| entry.name() == "first").unwrap().id()).unwrap().value(),
            RowCellValue::String(value) if value == "ab"
        ));
        assert!(matches!(
            row.get(&schema.columns().values().find(|entry| entry.name() == "second").unwrap().id()).unwrap().value(),
            RowCellValue::String(value) if value == "cde"
        ));

        let mut bytes = baseline.clone();
        bytes[14..18].copy_from_slice(&[5, 0, 2, 0]);
        let error = from_binary(&bytes, &schema).unwrap_err();
        assert!(matches!(
            error,
            RowDeserializationError::InvalidStringOffsets {
                index: 1,
                start_offset: 5,
                end_offset: 2,
                string_data_len: 5,
            }
        ));
        assert_eq!(
            error.to_string(),
            "string at index 1 has invalid byte range 5..2 in string data of length 5; offsets must be nondecreasing and within the string data"
        );
        let mut bytes = baseline.clone();
        bytes[16..18].copy_from_slice(&[9, 0]);
        let error = from_binary(&bytes, &schema).unwrap_err();
        assert!(matches!(
            error,
            RowDeserializationError::InvalidStringOffsets {
                index: 1,
                start_offset: 2,
                end_offset: 9,
                string_data_len: 5,
            }
        ));
        assert_eq!(
            error.to_string(),
            "string at index 1 has invalid byte range 2..9 in string data of length 5; offsets must be nondecreasing and within the string data"
        );
        let mut bytes = baseline;
        bytes[16..18].copy_from_slice(&[3, 0]);
        assert!(matches!(
            from_binary(&bytes, &schema),
            Err(RowDeserializationError::TrailingStringData)
        ));

        let one_string = Schema::builder(9)
            .column(ColumnId::new(6), "label", DataType::String, 0, false)
            .build()
            .unwrap();
        let bytes = hex("11 00 00 00 01 00 00 00 00 00 00 00 00 00 01 00 FF");
        assert!(matches!(
            from_binary(&bytes, &one_string),
            Err(RowDeserializationError::StringDeserialization { index: 0, .. })
        ));

        let one_int = Schema::builder(9)
            .column(ColumnId::new(5), "value", DataType::Int, 0, false)
            .build()
            .unwrap();
        let mut bytes = hex("12 00 01 00 00 00 07 00 00 00 00 00 00 00 00 00 00 00");
        bytes.push(0xAA);
        bytes[..2].copy_from_slice(&[0x13, 0]);
        assert!(matches!(
            from_binary(&bytes, &one_int),
            Err(RowDeserializationError::TrailingStringData)
        ));
    }
}

#[cfg(test)]
mod test_helpers {
    use super::*;
    use shared::schemas::schema::SchemaEntry;

    pub(super) fn schema_with_int_value_and_string_label() -> Schema {
        Schema::builder(9)
            .column(ColumnId::new(5), "value", DataType::Int, 0, false)
            .column(ColumnId::new(6), "label", DataType::String, 1, false)
            .build()
            .unwrap()
    }

    pub(super) fn encoded_row_with_value_7_and_label_hi() -> Vec<u8> {
        [
            0x16, 0x00, 0x01, 0x00, 0x01, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x68, 0x69,
        ]
        .to_vec()
    }

    pub(super) fn create_schema() -> Schema {
        Schema::builder(1)
            .column(ColumnId::new(0), "id", DataType::Int, 0, true)
            .column(ColumnId::new(1), "name", DataType::String, 1, false)
            .column(ColumnId::new(2), "age", DataType::Int, 2, false)
            .column(ColumnId::new(3), "email", DataType::String, 3, false)
            .pk([ColumnId::new(0)])
            .build()
            .unwrap()
    }

    pub(super) fn create_row(schema: &Schema) -> Row {
        Row::builder(schema)
            .value_named("id", 1)
            .value_named("name", "Mary")
            .value_named("age", 25)
            .value_named("email", "mary@mail.com")
            .build()
    }

    pub(super) fn entry_named<'a>(schema: &'a Schema, name: &str) -> &'a SchemaEntry {
        schema
            .columns()
            .values()
            .find(|entry| entry.name() == name)
            .unwrap()
    }
}
