# map — repark-python/src/column/display

## Purpose

Child modules of [`display.rs`](../display.rs). The Group-1 typed constructors arrived
while `mod.rs` sat at its exact 1052-line baseline and `#[pymethods]` cannot split a
second impl block for `PyColumnParts` across files — so the `Expr` construction lives
in this child module and `display.rs` keeps the thin static-method wrappers that call it.

## Contents

- `construct.rs` — **FACADE-2 step 3 (2026-09-13):** the Group-1 typed constructors
  that replaced `_native.PyColumn.sql` call sites: `lit_timestamp`, `lit_date`,
  `lit_time`, `lit_array_cast`, `pi`, `uuid`. Each builds the `Expr` the analyzed SQL
  text produced and renders the display/SQL fragments in Rust. `lit_timestamp` mirrors
  the analyzer's fold decision — in-range text folds to
  `Literal(TimestampMicrosecond(µs, UTC))`; out-of-range or unparsable text keeps the
  `to_timestamp(__repark_decimal_cast_nullable__(Utf8))` call the SQL path stored
  (measured: Arrow's nanosecond cast yields a null element, never an error).
  pins: facade-2/C-014, C-015, C-016

## Pointers

- Up: [../map.md](../map.md)
