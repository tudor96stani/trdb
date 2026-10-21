---
created: 2026-10-10
---

The binary helpers read and write small values in byte slices. Page views and row encoding use them to keep byte order and conversion checks in one place.

## Little-endian integers

Little-endian order puts the least significant byte first. This gives stored fields a defined byte order independent of the machine reading them. We use the same helpers for page metadata and row metadata.

`LittleEndianInteger` is implemented for `u16`, `u32` and `u64`. `from_le()` and `to_le()` work on a slice exactly as wide as the type. A wrong source width returns `BytesSliceSizeMismatch`; a wrong target width returns `WriteErrorSliceSizeMismatch`.

`read_le(bytes, offset)` and `write_le(bytes, offset, value)` select that width from a larger slice:

1. We take the range beginning at `offset`, relative to the supplied slice.
2. If the range is outside the slice, we return `BytesSliceSizeMismatch` with the offset and required width.
3. Otherwise, we read or write the value in little-endian order.

For example, writing `0x1234u16` at offset 1 in a four-byte buffer gives us:

```text
before: 00 00 00 00
after:  00 34 12 00
```

Reading a `u32` at offset 1 fails because it needs bytes `1..5`. Offsets are relative to the supplied slice, and callers provide the offset appropriate to their layout.

## Conversions

`UsizeConversion` provides `to_u16()` and `to_u32()` using checked conversions. A value that does not fit returns `ConversionError::Overflow` instead of being truncated.

For example, `65535usize.to_u16()` succeeds and `65536usize.to_u16()` fails. Callers use these checks when lengths and indices need to fit smaller stored fields.

## Bitmap

A bitmap packs independent true/false flags into individual bits. `Bitmap` holds a `u64`, so 64 flags occupy eight bytes. Row encoding uses this to mark NULL columns without storing a separate byte for each flag.

`new()` and `default()` start with no bits set; `from_u64()` uses the supplied bits.

- `set(index)` enables a bit.
- `unset(index)` disables it.
- `toggle(index)` flips it.
- `check(index)` returns whether it is set.

Indices are zero-based and must be in `0..64`. The bit operations use a mask formed by shifting `1` to the supplied index.

`to_bytes()` returns eight little-endian bytes in a vector. `from_bytes()` requires exactly eight bytes and maps slice conversion failure into `BinaryError`. Setting bits 0 and 9 produces `01 02 00 00 00 00 00 00`.

The caller decides what each bit means. The bitmap does not know about schema columns or NULL values.
