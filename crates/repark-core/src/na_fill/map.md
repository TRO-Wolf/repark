# map — repark-core/src/na_fill

## Purpose

Engine-side `na.fill` expression builder: Spark casts the fill literal to the
target column's own Arrow type, then `coalesce`s. The cast decision reads the
column type from the plan schema in Rust, so neither door can drift from it.

## Contents

- [`tests.rs`](tests.rs) — every signed numeric width casts to its own type,
  float-into-int and int-into-float casts, string/bool/decimal untouched,
  missing field uncast, engine-name miss with display fallback.
  pins: logical-width-1/C-012

## Pointers

- Up: [src map](../map.md)
- Facade: `python/repark/src/repark/spark/dataframe/actions_export.py`
- Binding: `crates/repark-python/src/dataframe_fill.rs` (free `fill_expr_for_column`)
