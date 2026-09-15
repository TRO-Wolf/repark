# map — repark-core/src/update_fields

## Purpose

Spark `UpdateFields` as a DataFusion `ScalarUDF` named `update_fields` for the
`Column.withField` / `Column.dropFields` facade surface.

## Contents

- [`udf.rs`](udf.rs) — the UDF: struct expression first, then an encoded edit list
  (`'with'` / `'drop'` literal tags, dotted field-path literals, value expressions for
  `with`). `return_field_from_args` applies the edits sequentially to the input struct
  type (case-insensitive match, every duplicate replaced, new spelling wins, `with`
  appends, `drop` is a no-op when absent, missing parent → `FIELD_NOT_FOUND`, empty
  top-level struct → `DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS`, non-struct →
  `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`). The kernel rebuilds the `StructArray`
  from input children and value arrays, masking every output child with the parent
  validity so a NULL struct (top or intermediate) stays NULL through field extraction;
  the mask ANDs the validity bitmaps onto shared value buffers (no `nullif`
  `Vec<bool>` roundtrip). pins: column-parity-1/C-008, C-009
- [`tests.rs`](tests.rs) — plan/exec pins: sequential replace-after-add, drop-after-add,
  add-after-drop order, nested chains, NULL-parent masking, duplicates, case folding,
  empty-name `col{index}`, all three error classes.
  pins: column-parity-1/C-008

## Pointers

- Up: [src map](../map.md)
- Facade: `python/repark/src/repark/spark/column_fields.py`
- Binding: `crates/repark-python/src/column/display.rs` (`PyColumnParts.update_fields`)
