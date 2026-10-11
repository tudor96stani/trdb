//! This module contains the implementation for the row interpreter helper functions
//!
//! These functions will convert an in memory representation of a data row (a `Row` instance) to and from a byte array as defined by the binary representation of a row.
use shared::binary_helpers::bin_error::BinaryError;
use shared::binary_helpers::bitmap::Bitmap;
use shared::binary_helpers::conversions::{ConversionError, UsizeConversion};
use shared::schemas::data_type::DataType;
use shared::schemas::row::{FIXED_LEN_SIZE, Row, RowCellValue};
use shared::schemas::schema::{Schema, SchemaEntryId};
use thiserror::Error;

/// The size of the header of a row, in bytes
const HEADER_SIZE: usize = 6;

/// The size of the null bitmap, in bytes
const NULL_BITMAP_SIZE: usize = 8;

/// The size of an offset, used in the offsets array to store the end offset of each string from the row, in bytes
const OFFSET_SIZE: usize = 2;

/// Errors that can occur during encoding and decoding rows
#[derive(Debug, Error)]
pub enum RowInterpreterError {
    /// The row references a schema entry that is not present in the provided schema.
    #[error(
        "row cell references schema entry {schema_entry_id:?}, but that entry is not present in the provided schema"
    )]
    MissingSchemaEntryForRowCell {
        /// The schema entry id referenced by the row cell.
        schema_entry_id: SchemaEntryId,
    },
    /// The row does not contain a schema entry that is not present in the provided schema.
    #[error(
        "row cell does not contain schema entry {schema_entry_id:?}, but that entry is present in the provided schema"
    )]
    MissingRowCellForSchemaEntry {
        /// The schema entry id missing the row cell.
        schema_entry_id: SchemaEntryId,
    },
    /// Error during conversion between data types
    #[error("Error while converting between data types")]
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
    #[error("Error while performing binary operations")]
    Binary(#[from] BinaryError),
}

struct RowMetadata {
    row_size_bytes: usize,
    nb_fixed: usize,
    nb_var: usize,
}

/// Encode a row to binary
pub fn to_binary(row: &Row, schema: &Schema) -> Result<Vec<u8>, RowInterpreterError> {
    // TODO validate row matches schema before converting

    let metadata = compute_metadata(row, schema)?;
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

//region Fill in bytes
fn fill_in_strings(
    vec: &mut [u8],
    metadata: &RowMetadata,
    row: &Row,
    schema: &Schema,
) -> Result<(), RowInterpreterError> {
    let mut string_schema_entries: Vec<_> = schema
        .columns()
        .iter()
        .filter(|(_, entry)| !entry.is_fixed_size())
        .map(|(_, entry)| entry)
        .collect();

    string_schema_entries.sort_by_key(|e| e.position());

    // this is where the offsets array will start (used to know where to write the offset in the byte array. virtual/relative index, not actual position in the main byte array
    let offsets_array_base_index =
        HEADER_SIZE + metadata.nb_fixed * FIXED_LEN_SIZE + NULL_BITMAP_SIZE;
    // this index is used to track what is the end offset (relative to the start of the array of offsets) for the column we are currently tracking.
    let mut offsets_array_collector_index = 0;

    // this is where we will write the actual strings (initialized to the place where we will write the first string, updated by the loop as more string are added). concrete index, actual position in the main byte array
    let mut string_blob_collector = offsets_array_base_index + metadata.nb_var * OFFSET_SIZE;

    for (index, entry) in string_schema_entries.iter().enumerate() {
        let row_cell =
            row.get(&entry.id())
                .ok_or(RowInterpreterError::MissingSchemaEntryForRowCell {
                    schema_entry_id: entry.id(),
                })?;

        let string_value = match row_cell.value() {
            RowCellValue::String(s) => s,
            RowCellValue::NULL => "",
            RowCellValue::Int(_) => {
                return Err(RowInterpreterError::MismatchedTypes {
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
) -> Result<usize, RowInterpreterError> {
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
        return Err(RowInterpreterError::BufferTooSmall {
            needed: needed_size,
            actual: vec.len(),
        });
    }

    for (index, entry) in fixed_sized_schema_entries.iter().enumerate() {
        let row_cell =
            row.get(&entry.id())
                .ok_or(RowInterpreterError::MissingSchemaEntryForRowCell {
                    schema_entry_id: entry.id(),
                })?;
        let value = match row_cell.value() {
            RowCellValue::Int(v) => *v,
            RowCellValue::NULL => 0,
            RowCellValue::String(_) => {
                return Err(RowInterpreterError::MismatchedTypes {
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
) -> Result<(), RowInterpreterError> {
    if vec.len() < HEADER_SIZE {
        return Err(RowInterpreterError::BufferTooSmall {
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
) -> Result<(), RowInterpreterError> {
    let mut bitmap = Bitmap::new();
    for (index, entry) in schema.columns_sorted().enumerate() {
        let value =
            row.get(&entry.id())
                .ok_or(RowInterpreterError::MissingRowCellForSchemaEntry {
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
fn compute_metadata(row: &Row, schema: &Schema) -> Result<RowMetadata, RowInterpreterError> {
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

fn compute_row_size_in_bytes(row: &Row, schema: &Schema) -> Result<usize, RowInterpreterError> {
    // start with the minimum we know we will have - the header and the bitmap
    let mut total = HEADER_SIZE + NULL_BITMAP_SIZE;

    for cell in row.values() {
        let schema_entry = schema.columns().get(&cell.schema_entry_id()).ok_or(
            RowInterpreterError::MissingSchemaEntryForRowCell {
                schema_entry_id: cell.schema_entry_id(),
            },
        )?;

        match schema_entry.data_type() {
            // for integers, we only need to add 4 bytes each.
            DataType::Int => total += 4usize,
            DataType::String => match cell.value() {
                // we have to drill into the actual value to find its length
                RowCellValue::String(s) => {
                    // add both its actual length
                    total += s.len();
                    // and two bytes for the offsets array
                    total += 2
                }
                RowCellValue::NULL => {
                    // for a NULL string, only add 2 bytes for the offsets array
                    total += 2
                }
                RowCellValue::Int(_) => {}
            },
        }
    }

    Ok(total)
}

/// Returns the position in the byte array of the NULL bitmap
fn get_null_bitmap_start(row_metadata: &RowMetadata) -> usize {
    HEADER_SIZE + FIXED_LEN_SIZE * row_metadata.nb_fixed
}
//endregion

#[cfg(test)]
mod api_tests {
    use super::test_helpers::*;
    use crate::row_interpreter::to_binary;
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
            RowInterpreterError::BufferTooSmall {
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
            RowInterpreterError::MissingSchemaEntryForRowCell { schema_entry_id }
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
            RowInterpreterError::BufferTooSmall { needed, actual }
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
