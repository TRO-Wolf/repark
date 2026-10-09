# map — repark-functions/src/timestamp_ns_cast

## Purpose

ICE-TSNS-SQL-1 support. The parent `timestamp_ns_cast.rs` holds the two embedded Spark-door
casts to Iceberg `timestamp_ns` / `timestamptz_ns` and the `VALUES` timestamp-column conform the
`spark_ltz_timestamp_cast` analyzer rule calls.

**ICE-TSNS-MERGE-WALL-1 (2026-10-09):** `__repark_cast_timestamp_ns__` is the one conversion
into a nanosecond wall for every write door, not INSERT's only. `repark-iceberg`'s
`ntz_store::wall_cast_udf_name` emits its name for a `Timestamp(ns, None)` target from the
MERGE, UPDATE and overwrite sites; the ANSI door registers it beside the NTZ kernel. The
kernel itself did not change. pins: ice-tsns-merge-wall-1/C-008

**ICE-TSNS-MERGE-WALL-1 fold 1 (2026-10-09), two changes to that kernel:**

- **Its return field is nullable whenever the source is not already the target type.** An
  out-of-range value answers NULL when ANSI is off, so a field declared non-nullable for a
  non-null literal made `UPDATE … WHERE` and `MERGE … INSERT VALUES` of such a literal raise
  Arrow's `declared as non-nullable but contains null values` where INSERT stores NULL. Only a
  same-type source keeps its own nullability: nothing can overflow there.
- **A second argument conforms a nested value.** `__repark_cast_timestamp_ns__(value, shape)`
  takes a struct, list or map and a typed NULL of the target column's type, and answers the
  source value with every leaf whose paired target leaf is `Timestamp(ns, None)` converted by
  the one-argument conversion (`Conversion::convert`); every other leaf, the field names and
  the container nullability stay the source's, so the store cast that follows does what it did
  before on everything else. This is not a second kernel: the same registered function, the
  same leaf conversion, so a nested leaf overflows, reads a `DATE` and takes the session wall
  exactly as a top-level column does. `timestamp_ns_conform_expr` builds the call.
  **Why the shape is an argument:** a struct can mix a `timestamp_ns` field with a `TIMESTAMP`
  one, so the kernel must know which leaves are wall targets; the SQL-text store sites can pass
  it as `arrow_cast(NULL, '<type>')`.
  **Pairing** follows the store cast: a source field pairs with the target field of its name;
  when no name is shared at all the fields pair by position; a field with no pair is left
  alone. A converted leaf's field becomes nullable (the overflow answer), a map key's does not.
  A `timestamptz_ns` leaf is not converted by the wall kernel (control; parity row
  ICE-TSNS-SQL-1-R-008).

pins: ice-tsns-merge-wall-1/C-016, C-018

## Contents

- `nested.rs` — **ICE-TSNS-MERGE-WALL-1 fold 1 (2026-10-09):** the two-argument form's type
  rule (`conformed_type`) and array walk (`conform`) over struct, list, large list, fixed-size
  list and map, described above. pins: ice-tsns-merge-wall-1/C-016
- `nested_tests.rs` — its `#[cfg(test)]` suite: an instant under a struct, a list, a map, a
  map of lists and a list of maps takes the session wall; an overflow is NULL without ANSI and
  `[CAST_OVERFLOW]` with it; pairing by name, then by position, a mixed struct and a zoned
  target; the return field's nullability. pins: ice-tsns-merge-wall-1/C-016, C-018
- `tests.rs` — the module's `#[cfg(test)]` suite: nine fraction digits from strings, an explicit
  offset versus the session zone, malformed strings under ANSI on and off, exact microsecond
  widening, an instant into a naive target as the session wall, a wall into a zoned target
  localized, the overflow refusal, the planning-time refusal of a non-temporal source, and the
  `VALUES` retype / mixed-row cast. Round 2 (2026-09-18): narrowing floors instants and
  localizes walls, a microsecond input keeps its ticks, same-kind widening overflows as the row
  path does, and a `VALUES` node with no nanosecond column is left alone.
  pins: ice-tsns-sql-1/C-001, C-002, C-009
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
  pins: ntz-store-doors-1/C-009

## Pointers

- Up: [../map.md](../map.md)
