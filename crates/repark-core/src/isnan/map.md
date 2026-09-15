# map — repark-core/src/isnan

## Purpose

Engine-side `isNaN` with Spark's type dispatch for the `Column.isNaN` facade surface:
floating and string inputs test the DOUBLE value, struct/array/map refuse at plan time,
every other type answers false.

## Contents

- [`udf.rs`](udf.rs) — the `repark_isnan` scalar UDF. Float16/32/64 cast to DOUBLE and
  test the NaN bit with NULL → false; `Utf8`/`LargeUtf8`/`Utf8View` cast with strict
  (`safe: false`) options so a malformed value errors like the engine's other casts;
  struct, array and map refuse in `return_field_from_args` with
  `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` naming `isnan(<arg>)` and the Spark type
  (containers rendered `STRUCT<>`/`ARRAY<>`/`MAP<>`; scalar leaves share the
  `update_fields` mapping); date, timestamp, boolean, int, decimal and every other
  type answer false for every row including NULL. pins: column-parity-1/C-008, C-009
- [`tests.rs`](tests.rs) — double mask, int false, date false-including-NULL, string
  `NaN`/`1.5`/NULL, malformed refusal, literal NaN, struct/array/map plan refusal.
  pins: column-parity-1/C-008, C-009

## Pointers

- Up: [src map](../map.md)
- Facade: `python/repark/src/repark/spark/column_fields.py`
- Binding: `crates/repark-python/src/column/display.rs` (`PyColumnParts.repark_isnan`)
