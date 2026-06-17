use shared::binary_helpers::conversions::{ConversionError, UsizeConversion};
use shared::schemas::data_type::DataType;
use shared::schemas::row::{FIXED_LEN_SIZE, Row, RowCellValue};
use shared::schemas::schema::{Schema, SchemaEntryId};
use thiserror::Error;

const HEADER_SIZE: usize = 6;
const NULL_BITMAP_SIZE: usize = 8;

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
}

/// Encode a row to binary
pub fn to_binary(row: &Row, schema: &Schema) -> Result<Vec<u8>, RowInterpreterError> {
    // TODO validate row matches schema before converting

    let row_size = compute_row_size_in_bytes(row, schema)?;
    let row_size_as_u16 = row_size.to_u16()?;
    let mut vec: Vec<u8> = vec![0u8; row_size];

    fill_in_row_header(&mut vec, row_size_as_u16, schema)?;

    // Populate fixed sized columns
    fill_in_fixed_sized_columns(&mut vec, row, schema)?;

    // Populate null bitmap

    // Populate var len offsets array

    // Populate string blob

    Ok(vec)
}

fn fill_in_fixed_sized_columns(
    vec: &mut [u8],
    row: &Row,
    schema: &Schema,
) -> Result<(), RowInterpreterError> {
    let mut fixed_sized: Vec<_> = schema
        .columns()
        .iter()
        .filter(|(_, entry)| entry.is_fixed_size())
        .map(|(_, entry)| entry)
        .collect();

    fixed_sized.sort_by_key(|e| e.position());

    // The fixed sized columns start from this index
    let base_index: usize = HEADER_SIZE;
    let needed_size = base_index + fixed_sized.len() * FIXED_LEN_SIZE;

    if vec.len() < needed_size {
        return Err(RowInterpreterError::BufferTooSmall {
            needed: needed_size,
            actual: vec.len(),
        });
    }

    for (index, entry) in fixed_sized.iter().enumerate() {
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

    Ok(())
}

fn fill_in_row_header(
    vec: &mut [u8],
    row_size: u16,
    schema: &Schema,
) -> Result<(), RowInterpreterError> {
    if vec.len() < HEADER_SIZE {
        return Err(RowInterpreterError::BufferTooSmall {
            needed: HEADER_SIZE,
            actual: vec.len(),
        });
    }

    let nb_fixed_cols = schema
        .columns()
        .iter()
        .filter(|(_, entry)| entry.is_fixed_size())
        .count()
        .to_u16()?;

    let nb_var_len_cols = schema
        .columns()
        .iter()
        .filter(|(_, entry)| !entry.is_fixed_size())
        .count()
        .to_u16()?;

    let values = [row_size, nb_fixed_cols, nb_var_len_cols];

    for (index, value) in values.iter().enumerate() {
        vec[index * 2..index * 2 + 2].copy_from_slice(&value.to_le_bytes());
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use shared::schemas::schema::{Schema, SchemaEntry};

    fn create_schema() -> Schema {
        Schema::builder(1)
            .column("id", DataType::Int, 0, true)
            .column("name", DataType::String, 1, false)
            .column("age", DataType::Int, 2, false)
            .column("email", DataType::String, 3, false)
            .build()
    }

    fn create_schema_with_interleaved_fixed_columns() -> Schema {
        Schema::builder(2)
            .column("id", DataType::Int, 0, true)
            .column("name", DataType::String, 1, false)
            .column("age", DataType::Int, 2, false)
            .column("email", DataType::String, 3, false)
            .column("score", DataType::Int, 4, false)
            .build()
    }

    fn create_row(schema: &Schema) -> Row {
        Row::builder(schema)
            .value_named("id", 1)
            .value_named("name", "Mary")
            .value_named("age", 25)
            .value_named("email", "mary@mail.com")
            .build()
    }

    fn entry_named<'a>(schema: &'a Schema, name: &str) -> &'a SchemaEntry {
        schema
            .columns()
            .values()
            .find(|entry| entry.name() == name)
            .unwrap()
    }

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
        let schema = create_schema_with_interleaved_fixed_columns();
        let mut bytes = vec![0; HEADER_SIZE];

        // act
        fill_in_row_header(&mut bytes, 300, &schema).unwrap();

        // assert
        assert_eq!(bytes, vec![44, 1, 3, 0, 2, 0]);
    }

    #[test]
    fn fill_in_row_header_errors_when_buffer_is_too_small() {
        // arrange
        let schema = create_schema();
        let mut bytes = vec![0; HEADER_SIZE - 1];

        // act
        let error = fill_in_row_header(&mut bytes, 43, &schema).unwrap_err();

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
            .column("id", DataType::Int, 0, true)
            .column("name", DataType::String, 1, false)
            .build();
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
}
