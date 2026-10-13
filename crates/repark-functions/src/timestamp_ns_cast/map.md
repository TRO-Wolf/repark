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

**ICE-TSTZNS-WALL-1 (2026-10-10):** the zoned kernel's return field is nullable whenever the
source is not already a zoned nanosecond timestamp, the rule the naive kernel got in R-007's
fold 1 (`already_stored` decides for both). The shared MERGE, UPDATE and overwrite sites now
emit this kernel for a `timestamptz_ns` target, so a non-null literal past the range must be
allowed to answer NULL with ANSI off, as INSERT stores it. `tests.rs`
`the_return_field_is_nullable_where_an_overflow_answers_null` holds both kernels.
pins: ice-tstzns-wall-1/C-005, C-008

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

pins: ice-tsns-merge-wall-1/C-018, C-031
**ICE-TSNS-CREATE-DOOR-1 (2026-10-13): the origin mark and the walk.**

- **The mark.** The parent's kernel holds `origin: CastOrigin`. A kernel that arrives through
  the parser is `Written`; a kernel built by `timestamp_ns_cast_expr` is `Inserted`. The
  parser cannot reach `Inserted` and engine code cannot reach `Written` without naming it in
  a diff.
- `asked.rs` — `asked_nanosecond_columns(plan)` returns one flag per output column of an
  analyzed plan: true when the column's type holds a nanosecond timestamp, it has at least
  one nanosecond origin, and every origin is a written kernel. A column whose type holds no
  nanosecond timestamp is `false` and is never walked. Origins, by type flow: an expression
  whose type holds no nanosecond is no origin and is not descended (so a `CASE` condition
  over nanoseconds does not count); a written kernel is an asked origin and the walk stops
  there; an inserted kernel is not an origin and the walk goes into its argument (a
  microsecond or NULL argument gives nothing); `Alias` is transparent; a `Column` is the same
  column of the input plan; any other nanosecond-typed expression is the union of its
  children's origins, and unasked when it has none (it made nanoseconds itself: a literal, an
  Arrow cast). Plan nodes followed: `Projection`, `Aggregate` (with a grouping set:
  unasked), `Window`, `SubqueryAlias`, `Filter`, `Sort`, `Limit`, `Distinct::All`,
  `Repartition`, `Union` (every branch), `Values` (every cell), `Join` (inner, left, right,
  full: the side that holds the index; any other join type is unasked). Every other node,
  `TableScan` first among them, is unasked; the plan `match` names every variant, so a new
  DataFusion node does not compile until it is placed. The walk stops and answers unasked
  below 1,024 levels, so a plan of any depth cannot overflow the stack. A wrong answer in
  the unasked direction is a refusal with a remedy, never a silently created column.
- `asked.rs`, fold 1 (2026-10-13, after a verify failed `53ced116`): **a view is followed
  into its plan.** A `TableScan` whose source carries a logical plan (DataFusion's
  `ViewTable`), or for which the seat's resolver answers one, is walked at the scan's
  projected column. The resolver (`ViewPlans`, a function from a scan to a plan) is how a
  door names its own view providers: the Spark door's SQL `CREATE TEMP VIEW` is a
  re-planning provider that `repark-functions` cannot name, and the Spark seat answers its
  definition plan. Any other scan is unasked as before, so a cached frame or a registered
  Arrow table still hides what was written before it. Each view followed costs 32 of the
  1,024 levels, and the scan arm is its own function, so a view that names itself through
  the resolver ends unasked without overflowing the stack. **Semi and anti joins** are followed
  on the side whose columns they keep, in the `Join` arm; `EXCEPT` and `INTERSECT` plan as
  such joins, so they are followed too. Mark joins are unasked. An expression whose type
  cannot be computed is unasked, not an error. `asked_nanosecond_columns_in(state, plan)`
  is what the SQL doors call: when no output column's type holds a nanosecond timestamp it
  answers all-false **without analyzing the plan** (the second analysis was 18 percent of a
  200-column create); otherwise it analyzes eagerly and walks. Not followed, by ruling
  (each needs its own case): grouping sets, a scalar subquery, a lambda body, and a window
  function's `ORDER BY` / `PARTITION BY` expressions, which count as origins.
  pins: ice-tsns-create-door-1/C-014, C-016
- `asked_tests.rs` — the walk's `#[cfg(test)]` suite, plans built with the DataFusion builders
  over an in-memory table holding `ns`, `tzns` and `us` columns: the fifteen cases the work
  order names, the nodes that keep their input columns, and the depth bound.
  pins: ice-tsns-create-door-1/C-003
- `tests.rs` (ICE-TSNS-CREATE-DOOR-1 additions) — the registered kernel is written; a kernel
  built by engine code is inserted; the two share a name and are not equal; a written cast
  stays written through two analyses; the plan text does not show the origin.
  pins: ice-tsns-create-door-1/C-002
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
