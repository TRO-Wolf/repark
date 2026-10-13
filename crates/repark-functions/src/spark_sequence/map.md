# map — repark-functions/src/spark_sequence

## Purpose

The closed-form row kernels behind [`spark_sequence.rs`](../spark_sequence.rs): the
parent keeps the UDF plumbing, dispatch (scalar/column paths), shaping and the
`containsNull=false` list assembly, while this module builds each row's values.

## Files

- [`rows.rs`](rows.rs) — `int_row` / `date_row` / `timestamp_ticks` plus their stride,
  cardinality-ceiling (`push_over_ceiling`), calendar (`ymd_from_days`,
  `days_from_ymd`) and bound-formatting helpers. Month steps compute element `i`
  from the start (`start + months × i`); runtime expansion refuses with the
  literal ceiling's text. Extracted 2026-09-15 as a file-size split (move-only).
  pins: door-converge-2/C-007, C-009
- [`timestamp_tests.rs`](timestamp_tests.rs) — the timestamp arm's unit and zone pins
  (ICE-TSNS-COERCION-1 slice 1 fold 2, 2026-10-13). pins: ice-tsns-coercion-1/C-005

## Contracts pinned

- Element `i` derives from the start, never by chaining (Jan 31 + 1 month = Feb 29).
- Closed-form counts reserve up front; open-ended walks check the ceiling per push.
- **The timestamp list has the unit and zone of its declared element** (fold 2,
  2026-10-13). The parent brings both bounds to the element's unit as raw ticks (`as_ticks`:
  a finer bound is floored, a coarser one is multiplied and refuses on overflow) and
  `rows::timestamp_ticks` picks the walk: microseconds run the unchanged kernel; nanoseconds
  split the start into whole microseconds plus its sub-microsecond rest, walk the kernel in
  microseconds up to the last element not past the stop, and add the rest back, so every
  element is `start + i × step` exactly and `sequence(t, t)` is `[t]`; milliseconds and
  seconds walk in microseconds and refuse a step that leaves an element between two ticks.
  Before this the arm always built `Timestamp(µs, UTC)` and the list assembly refused every
  other input (`ListArray expected data type …`), including plain `TIMESTAMP_NTZ`.
- The step stays microsecond-exact for every unit: a step with sub-microsecond nanos is
  refused, as before. The direction of a nanosecond walk comes from the raw bounds, so a gap
  under one microsecond still refuses a step of the wrong sign.
- Calendar steps (day, month) run on the UTC wall of the instant for every zone label, as
  before; this fold does not change that.
- Pins (`timestamp_tests.rs`): `microsecond_forms_keep_their_zone_and_match_spark` (five
  forms over `TIMESTAMP_NTZ` and `TIMESTAMP`, ticks recorded on live Spark 4.1.2 in zone UTC
  by `python/repark/tests/_record_ice_tsns_coercion_1_slice1_fold2_spark.py`),
  `nanosecond_forms_equal_the_microsecond_twin` (Spark has no nanosecond answer; the same
  forms at microsecond-exact instants equal the twin × 1000),
  `nanosecond_start_keeps_its_sub_microsecond_digits` (what is stored for
  `…05.123456789`: every element ends `789`, and a stop one nanosecond short of the third
  element yields two), `nanosecond_gap_under_a_microsecond_keeps_the_step_sign_rule`,
  `mixed_units_follow_the_first_argument`, `second_and_millisecond_inputs_keep_their_unit`,
  `scalar_nanosecond_bounds_build_a_nanosecond_list`,
  `null_bound_answers_a_null_list_in_the_input_unit`.

## Pointers

- Up: [src map](../map.md)
- Parent: [`spark_sequence.rs`](../spark_sequence.rs)
