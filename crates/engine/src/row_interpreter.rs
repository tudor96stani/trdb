//! This module contains the implementation for the row interpreter helper functions
//!
//! These functions will convert an in memory representation of a data row (a `Row` instance) to and from a byte array as defined by the binary representation of a row.
use shared::binary_helpers::bin_error::BinaryError;
use shared::binary_helpers::bitmap::Bitmap;
use shared::binary_helpers::conversions::{ConversionError, UsizeConversion};
use shared::schemas::ObjectId;
use shared::schemas::data_type::DataType;
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
    #[error("Mismatch between expected data type and found data type")]
    MismatchedTypes {
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
    #[error("string offsets must be nondecreasing and within the string data")]
    InvalidStringOffsets,
    /// Bytes remain after the final string end offset.
    #[error("row contains bytes beyond the string data")]
    TrailingStringData,
    /// A column marked as NULL contains a nonzero integer or a nonempty string.
    #[error("column {column_id} is marked as NULL but contains data")]
    InvalidNullColumn {
        /// ID of the column with an invalid NULL representation.
        column_id: ObjectId,
    },
    /// Header column counts differ from the supplied schema.
    #[error("header column counts must match the schema")]
    MismatchedColumnCounts,
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

    fill_in_row_header(&mut result_bytes, &metadata)?;

    // Populate fixed sized columns
    fill_in_fixed_sized_columns(&mut result_bytes, row, schema)?;

    // Populate null bitmap
    fill_in_null_bitmap(&mut result_bytes, &metadata, row, schema)?;

    // Populate var len offsets array
    fill_in_strings(&mut result_bytes, &metadata, row, schema)?;

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
            let string_bytes = bytes
                .get(cursor_for_already_read_strings..end)
                .ok_or(RowDeserializationError::InvalidStringOffsets)?;

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

//region Fill in bytes
fn fill_in_strings(
    vec: &mut [u8],
    metadata: &RowMetadata,
    row: &Row,
    schema: &Schema,
) -> Result<(), RowSerializationError> {
    let mut string_schema_entries: Vec<_> = schema
        .columns()
        .iter()
        .filter(|(_, entry)| !entry.is_fixed_size())
        .map(|(_, entry)| entry)
        .collect();

    string_schema_entries.sort_by_key(|e| e.position());

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
            RowCellValue::String(s) => s,
            RowCellValue::NULL => "",
            RowCellValue::Int(_) => {
                return Err(RowSerializationError::MismatchedTypes {
                    expected: DataType::String,
                    found: DataType::Int,
                });
            }
        };

        // First let's write the entry in the array
        let offset_starting_index = offsets_array_base_index + index * OFFSET_SIZE;
        // Since this string will be placed right after the previous one, we take the previous end offset we had and add this one's size (first run collector will be 0)
        let offset_to_write = (offsets_array_collector_index + string_value.len()).to_u16()?;
        vec[offset_starting_index..offset_starting_index + OFFSET_SIZE]
            .copy_from_slice(&offset_to_write.to_le_bytes());
        offsets_array_collector_index += string_value.len();

        // Now let's write the actual string
        vec[string_blob_collector..string_blob_collector + string_value.len()]
            .copy_from_slice(string_value.as_bytes());
        string_blob_collector += string_value.len()
    }

    Ok(())
}

/// Returns the end position of the fixed sized segment
fn fill_in_fixed_sized_columns(
    vec: &mut [u8],
    row: &Row,
    schema: &Schema,
) -> Result<usize, RowSerializationError> {
    let mut fixed_sized_schema_entries: Vec<_> = schema
        .columns()
        .iter()
        .filter(|(_, entry)| entry.is_fixed_size())
        .map(|(_, entry)| entry)
        .collect();

    fixed_sized_schema_entries.sort_by_key(|e| e.position());

    // The fixed sized columns start from this index
    let base_index: usize = HEADER_SIZE;
    let needed_size = base_index + fixed_sized_schema_entries.len() * FIXED_LEN_SIZE;

    if vec.len() < needed_size {
        return Err(RowSerializationError::BufferTooSmall {
            needed: needed_size,
            actual: vec.len(),
        });
    }

    for (index, entry) in fixed_sized_schema_entries.iter().enumerate() {
        let row_cell =
            row.get(&entry.id())
                .ok_or(RowSerializationError::MissingSchemaEntryForRowCell {
                    schema_entry_id: entry.id(),
                })?;
        let value = match row_cell.value() {
            RowCellValue::Int(v) => *v,
            RowCellValue::NULL => 0,
            RowCellValue::String(_) => {
                return Err(RowSerializationError::MismatchedTypes {
                    expected: DataType::Int,
                    found: DataType::String,
                });
            }
        };
        // Start writing this integer at base + all previous integers
        let start_offset = base_index + index * FIXED_LEN_SIZE;
        vec[start_offset..start_offset + FIXED_LEN_SIZE].copy_from_slice(&value.to_le_bytes())
    }

    Ok(needed_size)
}

fn fill_in_row_header(
    vec: &mut [u8],
    row_metadata: &RowMetadata,
) -> Result<(), RowSerializationError> {
    if vec.len() < HEADER_SIZE {
        return Err(RowSerializationError::BufferTooSmall {
            needed: HEADER_SIZE,
            actual: vec.len(),
        });
    }

    let values = [
        row_metadata.row_size_bytes.to_u16()?,
        row_metadata.nb_fixed.to_u16()?,
        row_metadata.nb_var.to_u16()?,
    ];

    for (index, value) in values.iter().enumerate() {
        vec[index * 2..index * 2 + 2].copy_from_slice(&value.to_le_bytes());
    }

    Ok(())
}

fn fill_in_null_bitmap(
    result_bytes: &mut [u8],
    row_metadata: &RowMetadata,
    row: &Row,
    schema: &Schema,
) -> Result<(), RowSerializationError> {
    let mut bitmap = Bitmap::new();
    for (index, entry) in schema.columns_sorted().enumerate() {
        let value =
            row.get(&entry.id())
                .ok_or(RowSerializationError::MissingRowCellForSchemaEntry {
                    schema_entry_id: entry.id(),
                })?;

        match value.value() {
            RowCellValue::NULL => {
                bitmap.set(index);
            }
            RowCellValue::Int(_) | RowCellValue::String(_) => {
                continue;
            }
        }
    }

    let bitmap_bytes = bitmap.to_bytes()?;

    let bitmap_start = get_null_bitmap_start(row_metadata);

    result_bytes[bitmap_start..bitmap_start + NULL_BITMAP_SIZE].copy_from_slice(&bitmap_bytes);

    Ok(())
}
//endregion

//region Metadata
/// Creates a `RowMetadata` value for the provided row.
///
/// ## Returns
/// - `RowMetadata' filled in if successful, error otherwise
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
/// Returns the position in the byte array of the NULL bitmap
fn get_null_bitmap_start(row_metadata: &RowMetadata) -> usize {
    HEADER_SIZE + FIXED_LEN_SIZE * row_metadata.nb_fixed
}

//endregion

//region validators
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
                    expected: DataType::Int,
                    found: DataType::String,
                });
            }
            (DataType::String, RowCellValue::Int(_)) => {
                return Err(RowSerializationError::MismatchedTypes {
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
        return Err(RowDeserializationError::MismatchedColumnCounts);
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

//endregion

#[cfg(test)]
mod api_tests {
    use super::test_helpers::*;
    use super::*;

    #[test]
    fn serialization_rejects_missing_cells_including_null_columns() {
        let schema = create_schema();
        for missing in ["id", "name", "age", "email"] {
            let mut builder = Row::builder(&schema);
            for entry in schema.columns_sorted() {
                if entry.name() != missing {
                    builder = builder.cell(entry, RowCellValue::NULL);
                }
            }
            assert!(matches!(
                to_binary(&builder.build(), &schema),
                Err(RowSerializationError::MissingRowCellForSchemaEntry { schema_entry_id })
                    if schema_entry_id == entry_named(&schema, missing).id()
            ));
        }
    }

    #[test]
    fn serialization_rejects_unknown_column_ids_and_owners() {
        let schema = Schema::builder(1)
            .column(7, "value", DataType::Int, 0, false)
            .build()
            .unwrap();
        for (owner, column) in [(1, 99), (2, 7)] {
            let other_schema = Schema::builder(owner)
                .column(column, "value", DataType::Int, 0, false)
                .build()
                .unwrap();
            let row = Row::builder(&other_schema).value(column, 1).build();
            assert!(matches!(
                to_binary(&row, &schema),
                Err(RowSerializationError::MissingSchemaEntryForRowCell { schema_entry_id })
                    if schema_entry_id == entry_named(&other_schema, "value").id()
            ));
        }
    }

    #[test]
    fn serialization_rejects_both_type_mismatches() {
        let ints = Schema::builder(1)
            .column(7, "value", DataType::Int, 0, false)
            .build()
            .unwrap();
        let strings = Schema::builder(1)
            .column(7, "value", DataType::String, 0, false)
            .build()
            .unwrap();
        let int_row = Row::builder(&ints).value(7, 1).build();
        let string_row = Row::builder(&strings).value(7, "one").build();
        assert!(matches!(
            to_binary(&int_row, &strings),
            Err(RowSerializationError::MismatchedTypes {
                expected: DataType::String,
                found: DataType::Int,
            })
        ));
        assert!(matches!(
            to_binary(&string_row, &ints),
            Err(RowSerializationError::MismatchedTypes {
                expected: DataType::Int,
                found: DataType::String,
            })
        ));
    }

    #[test]
    fn serialization_enforces_complete_row_size_using_utf8_bytes() {
        let schema = Schema::builder(1)
            .column(7, "number", DataType::Int, 0, false)
            .column(99, "text", DataType::String, 1, false)
            .column(42, "empty", DataType::String, 2, false)
            .build()
            .unwrap();
        // 22 bytes of header, integer, bitmap, and offsets; 3974 UTF-8 bytes.
        let text = "é".repeat(1_987);
        for extra in ["", "x"] {
            let row = Row::builder(&schema)
                .value(7, RowCellValue::NULL)
                .value(99, format!("{text}{extra}"))
                .value(42, RowCellValue::NULL)
                .build();
            let result = to_binary(&row, &schema);
            if extra.is_empty() {
                let bytes = result.unwrap();
                assert_eq!(bytes.len(), 3_996);
                assert_eq!(&bytes[..2], &3_996u16.to_le_bytes());
                assert_eq!(&bytes[22..], text.as_bytes());
            } else {
                assert!(matches!(result, Err(RowSerializationError::RowTooLarge)));
            }
        }
        let huge_row = Row::builder(&schema)
            .value(7, 0)
            .value(99, "x".repeat(65_536))
            .value(42, "")
            .build();
        assert!(matches!(
            to_binary(&huge_row, &schema),
            Err(RowSerializationError::RowTooLarge)
        ));
    }

    #[test]
    fn serialization_accepts_64_null_columns_including_both_primary_key_types() {
        let mut builder = Schema::builder(1);
        for position in 0..64 {
            builder = builder.column(
                position as u16,
                format!("column_{position}"),
                if position % 2 == 0 {
                    DataType::Int
                } else {
                    DataType::String
                },
                position,
                position < 2,
            );
        }
        let schema = builder.pk([0, 1]).build().unwrap();
        let mut builder = Row::builder(&schema);
        for entry in schema.columns_sorted() {
            builder = builder.cell(entry, RowCellValue::NULL);
        }
        let bytes = to_binary(&builder.build(), &schema).unwrap();
        assert_eq!(bytes.len(), 206);
        assert_eq!(&bytes[..6], &[206, 0, 32, 0, 32, 0]);
        assert!(bytes[6..134].iter().all(|byte| *byte == 0));
        assert_eq!(&bytes[134..142], &[0xFF; 8]);
        assert!(bytes[142..].iter().all(|byte| *byte == 0));
    }

    #[test]
    pub fn to_binary_returns_correct_byte_array() {
        // arrange
        let schema = create_schema();
        let row = create_row(&schema);

        // act
        let bytes = to_binary(&row, &schema).unwrap();

        // assert
        // Expected layout (byte ranges are relative to the start of the row):
        // | 0..6               | 6..14    | 14..22      | 22..26             | 26..43               |
        // | header             | integers | NULL bitmap | string end offsets | string blob          |
        // | size=43, I=2, S=2   | 1, 25    | 0           | 4, 17              | "Mary""mary@mail.com" |
        // Numeric fields are little-endian. String end offsets are relative to
        // the blob: "Mary" occupies 0..4 and "mary@mail.com" occupies 4..17.
        let expected = [
            0x2B, 0x00, 0x02, 0x00, 0x02, 0x00, // header: 43 bytes, 2 ints, 2 strings
            0x01, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00, 0x00, // integers: 1, 25
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // NULL bitmap
            0x04, 0x00, 0x11, 0x00, // string end offsets: 4, 17
            0x4D, 0x61, 0x72, 0x79, // "Mary"
            0x6D, 0x61, 0x72, 0x79, 0x40, 0x6D, 0x61, 0x69, 0x6C, 0x2E, 0x63, 0x6F,
            0x6D, // "mary@mail.com"
        ];
        assert_eq!(bytes.as_slice(), &expected);
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::row_interpreter::test_helpers::{
        create_row, create_schema, create_schema_with_interleaved_fixed_columns, entry_named,
    };
    use shared::schemas::schema::Schema;

    #[test]
    fn compute_row_size_in_bytes_returns_correct_size() {
        // arrange
        let schema = create_schema();
        let row = create_row(&schema);

        // act
        let row_size = compute_row_size_in_bytes(&row, &schema).unwrap();

        // assert
        assert_eq!(row_size, 43);
    }

    #[test]
    fn fill_in_row_header_writes_row_size_and_column_counts_as_little_endian_u16s() {
        // arrange
        let mut bytes = vec![0; HEADER_SIZE];
        let metadata = RowMetadata {
            row_size_bytes: 300,
            nb_fixed: 3,
            nb_var: 2,
        };
        // act
        fill_in_row_header(&mut bytes, &metadata).unwrap();

        // assert
        assert_eq!(bytes, vec![44, 1, 3, 0, 2, 0]);
    }

    #[test]
    fn fill_in_row_header_errors_when_buffer_is_too_small() {
        // arrange
        let mut bytes = vec![0; HEADER_SIZE - 1];
        let metadata = RowMetadata {
            row_size_bytes: 43,
            nb_fixed: 2,
            nb_var: 2,
        };
        // act
        let error = fill_in_row_header(&mut bytes, &metadata).unwrap_err();

        // assert
        assert!(matches!(
            error,
            RowSerializationError::BufferTooSmall {
                needed,
                actual
            } if actual == HEADER_SIZE - 1 && needed == HEADER_SIZE

        ));
    }

    #[test]
    fn fill_in_fixed_sized_columns_writes_ints_in_schema_position_order() {
        // arrange
        let schema = create_schema_with_interleaved_fixed_columns();
        let row = Row::builder(&schema)
            .value_named("score", -9)
            .value_named("name", "Mary")
            .value_named("id", 1)
            .value_named("email", "mary@mail.com")
            .value_named("age", 25)
            .build();
        let mut bytes = vec![0xAA; HEADER_SIZE + 3 * FIXED_LEN_SIZE + NULL_BITMAP_SIZE];

        // act
        fill_in_fixed_sized_columns(&mut bytes, &row, &schema).unwrap();

        // assert
        assert_eq!(&bytes[0..HEADER_SIZE], &[0xAA; HEADER_SIZE]);
        assert_eq!(&bytes[6..10], &1i32.to_le_bytes());
        assert_eq!(&bytes[10..14], &25i32.to_le_bytes());
        assert_eq!(&bytes[14..18], &(-9i32).to_le_bytes());
        assert_eq!(&bytes[18..], &[0xAA; NULL_BITMAP_SIZE]);
    }

    #[test]
    fn fill_in_fixed_sized_columns_writes_zero_for_null_fixed_values() {
        // arrange
        let schema = Schema::builder(3)
            .column(0, "id", DataType::Int, 0, true)
            .column(1, "name", DataType::String, 1, false)
            .pk([0])
            .build()
            .unwrap();
        let row = Row::builder(&schema)
            .value_named("id", RowCellValue::NULL)
            .value_named("name", "Mary")
            .build();
        let mut bytes = vec![0; HEADER_SIZE + FIXED_LEN_SIZE + NULL_BITMAP_SIZE];

        // act
        fill_in_fixed_sized_columns(&mut bytes, &row, &schema).unwrap();

        // assert
        assert_eq!(&bytes[6..10], &0i32.to_le_bytes());
    }

    #[test]
    fn fill_in_fixed_sized_columns_errors_when_row_is_missing_fixed_column() {
        // arrange
        let schema = create_schema_with_interleaved_fixed_columns();
        let row = Row::builder(&schema)
            .value_named("id", 1)
            .value_named("name", "Mary")
            .value_named("email", "mary@mail.com")
            .value_named("score", -9)
            .build();
        let mut bytes = vec![0; HEADER_SIZE + 3 * FIXED_LEN_SIZE + NULL_BITMAP_SIZE];

        // act
        let error = fill_in_fixed_sized_columns(&mut bytes, &row, &schema).unwrap_err();

        // assert
        assert!(matches!(
            error,
            RowSerializationError::MissingSchemaEntryForRowCell { schema_entry_id }
                if schema_entry_id == entry_named(&schema, "age").id()
        ));
    }

    #[test]
    fn fill_in_fixed_sized_columns_errors_when_buffer_is_too_small() {
        // arrange
        let schema = create_schema_with_interleaved_fixed_columns();
        let row = Row::builder(&schema)
            .value_named("id", 1)
            .value_named("name", "Mary")
            .value_named("age", 25)
            .value_named("email", "mary@mail.com")
            .value_named("score", -9)
            .build();
        let needed = HEADER_SIZE + 3 * FIXED_LEN_SIZE;
        let mut bytes = vec![0; needed - 1];

        // act
        let error = fill_in_fixed_sized_columns(&mut bytes, &row, &schema).unwrap_err();

        // assert
        assert!(matches!(
            error,
            RowSerializationError::BufferTooSmall { needed, actual }
                if needed == HEADER_SIZE + 3 * FIXED_LEN_SIZE && actual == HEADER_SIZE + 3 * FIXED_LEN_SIZE - 1
        ));
    }

    #[test]
    fn get_null_bitmap_start_returns_correct_value() {
        // arrange
        let metadata = RowMetadata {
            row_size_bytes: 100,
            nb_fixed: 6,
            nb_var: 2,
        };

        // act
        let result = get_null_bitmap_start(&metadata);

        // assert
        assert_eq!(result, 30)
        // this test is mostly to catch if the computation ever gets messed up or intentionally changed
    }
}

#[cfg(test)]
mod test_helpers {
    use super::*;
    use shared::schemas::schema::SchemaEntry;
    pub(super) fn create_schema() -> Schema {
        Schema::builder(1)
            .column(0, "id", DataType::Int, 0, true)
            .column(1, "name", DataType::String, 1, false)
            .column(2, "age", DataType::Int, 2, false)
            .column(3, "email", DataType::String, 3, false)
            .pk([0])
            .build()
            .unwrap()
    }

    pub(super) fn create_schema_with_interleaved_fixed_columns() -> Schema {
        Schema::builder(2)
            .column(0, "id", DataType::Int, 0, true)
            .column(1, "name", DataType::String, 1, false)
            .column(2, "age", DataType::Int, 2, false)
            .column(3, "email", DataType::String, 3, false)
            .column(4, "score", DataType::Int, 4, false)
            .pk([0])
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
