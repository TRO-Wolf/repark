# map — repark-functions/src/spark_sequence

## Purpose

The closed-form row kernels behind [`spark_sequence.rs`](../spark_sequence.rs): the
parent keeps the UDF plumbing, dispatch (scalar/column paths), shaping and the
`containsNull=false` list assembly, while this module builds each row's values.

## Files

- [`rows.rs`](rows.rs) — `int_row` / `date_row` / `timestamp_row` plus their stride,
  cardinality-ceiling (`push_over_ceiling`), calendar (`ymd_from_days`,
  `days_from_ymd`) and bound-formatting helpers. Month steps compute element `i`
  from the start (`start + months × i`); runtime expansion refuses with the
  literal ceiling's text. Extracted 2026-09-15 as a file-size split (move-only).
  pins: door-converge-2/C-007, C-009

## Contracts pinned

- Element `i` derives from the start, never by chaining (Jan 31 + 1 month = Feb 29).
- Closed-form counts reserve up front; open-ended walks check the ceiling per push.

## Pointers

- Up: [src map](../map.md)
- Parent: [`spark_sequence.rs`](../spark_sequence.rs)
