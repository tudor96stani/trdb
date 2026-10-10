Schema metadata and in-memory row construction live in the shared crate:

- [Schemas and object identifiers](../shared/schemas/schemas.md)
- [Rows and row builder](../shared/schemas/rows.md)

The engine consumes these containers when producing the [binary row representation](binary_row_representation.md).
