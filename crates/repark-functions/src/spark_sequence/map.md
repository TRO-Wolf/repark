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
  `timestamp_row` takes the element's `TimeUnit` (ICE-TSNS-TYPE-PLANNER-1 M1,
  2026-10-15): the stride turns into ticks of that unit, a month or day step shifts the
  wall read from those ticks (`datetime_from_ticks`, `ticks_from_datetime`), and a step
  finer than the unit refuses as a sub-microsecond step does.

## Contracts pinned

- Element `i` derives from the start, never by chaining (Jan 31 + 1 month = Feb 29).
- Closed-form counts reserve up front; open-ended walks check the ceiling per push.
- A timestamp row steps and stores in the element's unit; no unit is assumed.

## Pointers

- Up: [src map](../map.md)
- Parent: [`spark_sequence.rs`](../spark_sequence.rs)
