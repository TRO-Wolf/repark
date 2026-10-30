# map — repark-functions/src/timestamp_ns_cast

## Purpose

ICE-TSNS-SQL-1 support. The parent `timestamp_ns_cast.rs` holds the two embedded Spark-door
casts to Iceberg `timestamp_ns` / `timestamptz_ns` and the `VALUES` timestamp-column conform the
`spark_ltz_timestamp_cast` analyzer rule calls.

**ICE-TSNS-MERGE-WALL-1 (2026-10-09):** `__repark_cast_timestamp_ns__` is the one conversion
into a nanosecond wall for every write door, not INSERT's only. `repark-iceberg`'s
`ntz_store::wall_cast_udf_name` emits its name for a `Timestamp(ns, None)` target from the
MERGE, UPDATE and overwrite sites; the ANSI door registers it beside the NTZ kernel. The
kernel itself did not change.

**ICE-TSNS-FLOOR-DOORS-1 (2026-10-14):** the parent's `narrow` became `store_micros(array,
target, zone)`, the one function that makes the tick when a timestamp goes into microseconds.
`tests.rs` calls it with the LTZ kernel's return type where it called `narrow`; what those
tests assert did not change.

**ICE-TSTZNS-WALL-1 (2026-10-10):** the zoned kernel's return field is nullable whenever the
source is not already a zoned nanosecond timestamp, the rule the naive kernel got in R-007's
fold 1 (`already_stored` decides for both). The shared MERGE, UPDATE and overwrite sites now
emit this kernel for a `timestamptz_ns` target, so a non-null literal past the range must be
allowed to answer NULL with ANSI off, as INSERT stores it. `tests.rs`
`the_return_field_is_nullable_where_an_overflow_answers_null` holds both kernels.

**ICE-TSNS-MERGE-WALL-1 fold 1 and the split (2026-10-09), what changed in that kernel.**

- **Nullability.** The wall kernel's return field is nullable whenever the source is not
  already the target type: an out-of-range value answers NULL when ANSI is off, and a field
  declared non-nullable for a non-null literal made `UPDATE … WHERE` and
  `MERGE … INSERT VALUES` raise Arrow's `declared as non-nullable but contains null values`
  where INSERT stores NULL. The **zoned** kernel keeps the rule it had (nullable for a
  nullable or string source only): fold 1 had widened both, and a required `timestamptz_ns`
  column then refused an overflow with a second sentence appended to main's text, six control
  cells of the re-verify.
- **No nested form.** Folds 1 and 2 gave the kernel a nested form with a pairing word, a
  layout `match`, a lineage guard and a named refusal. Three verifies found a wrong or a cut
  value in it each time, so the owner split the unit: that code is deleted, a nested
  `timestamp_ns` leaf is refused by `repark-iceberg/src/write/nested_ns_gate.rs`, and the
  design is card ICE-TSNS-NESTED-1 (`task/roadmap/mid-term/`). A `timestamptz_ns` leaf was
  never converted by the wall kernel (control; parity row ICE-TSNS-SQL-1-R-008).

- `tests.rs` — the module's `#[cfg(test)]` suite: nine fraction digits from strings, an explicit
  offset versus the session zone, malformed strings under ANSI on and off, exact microsecond
  widening, an instant into a naive target as the session wall, a wall into a zoned target
  localized, the overflow refusal, the planning-time refusal of a non-temporal source, and the
  `VALUES` retype / mixed-row cast. Round 2 (2026-09-18): narrowing floors instants and
  localizes walls, a microsecond input keeps its ticks, same-kind widening overflows as the row
  path does, and a `VALUES` node with no nanosecond column is left alone.
  `an_encoded_source_gets_the_verdict_and_the_value_of_its_value_type` (ruling 92,
  2026-10-15): a dictionary and a run-end encoded instant plan to the type and convert to
  the values of the plain instant, naive and zoned kernels; an encoded integer refuses.
- `values_evidence.rs` — **WO NTZ-STORE-DOORS-1 third re-verify fold (2026-09-29, RD4-1):**
  the syntactic evidence the parent's `VALUES` widening reads per cell. `values_cell_evidence`
  peels the `Alias` / non-null wrappers, at most one planner coercion `CAST` to a naive
  timestamp and any `± INTERVAL` literal arithmetic, then answers `Wall` (the embedded NTZ
  literal and casts, a naive microsecond literal, and `to_timestamp_ntz`,
  `make_timestamp_ntz` or `localtimestamp` when the call's own type is naive microseconds),
  `Timestamp` (a `TIMESTAMP` literal, the double
  `CAST` marker, a top-level `TRY_CAST` to `TIMESTAMP`, a cast under interval arithmetic),
  `NamedInstant` (`from_utc_timestamp`, `to_utc_timestamp`) or `None`. The parent pairs the
  `Timestamp` and `NamedInstant` answers with the rewritten type before it trusts them.
  **ICE-TSNS-TYPE-PLANNER-1 carry-over fix, mixed VALUES (ruling 100, 2026-10-15):** a written
  `TIMESTAMP` is recognized by the planner's microsecond instant type as well as DataFusion's
  naive nanosecond one (`is_written_timestamp`): with the planner `TIMESTAMP '…'` plans as
  `Timestamp(µs, "UTC")`, so an `INSERT … VALUES` mixing it with `TIMESTAMP_NTZ` cells into a
  `TIMESTAMP_NTZ` column lost its instant evidence and stored a New York gap wall as `02:30`
  where Spark stores `03:30` (`test_ntz_9_verify.py`, `test_ntz_store_doors_1.py` `ins_values`
  / `ins_sel`).

## Pointers

- Up: [../map.md](../map.md)
