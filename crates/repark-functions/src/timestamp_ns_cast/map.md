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

**ICE-TSNS-MERGE-WALL-1 folds 1 and 2 (2026-10-09), what changed in that kernel.**

- **Nullability.** The wall kernel's return field is nullable whenever the source is not
  already the target type: an out-of-range value answers NULL when ANSI is off, and a field
  declared non-nullable for a non-null literal made `UPDATE … WHERE` and
  `MERGE … INSERT VALUES` raise Arrow's `declared as non-nullable but contains null values`
  where INSERT stores NULL. The **zoned** kernel keeps the rule it had (nullable for a
  nullable or string source only): fold 1 had widened both, and a required `timestamptz_ns`
  column then refused an overflow with a second sentence appended to main's text, six control
  cells of the re-verify.
- **The nested form.** `__repark_cast_timestamp_ns__(value, shape, pairing, column)` takes a
  nested value, a typed NULL of the target column's type, a pairing word and the column's
  name. It answers the source value with every leaf that pairs with a `Timestamp(ns, None)`
  target leaf converted by the one-argument conversion (`Conversion::convert`); every other
  leaf and the field names stay the source's, so the store cast that follows does what it did
  before on everything else. Not a second kernel: the same registered function and the same
  leaf conversion, so a nested leaf overflows, reads a `DATE` or a string and takes the
  session wall exactly as a top-level column does. `timestamp_ns_conform_expr` builds the
  call.
- **Why the pairing is an argument (fold 2).** Fold 1 paired struct fields by name as soon as
  one name matched. The doors do not all pair that way, so with one field renamed the conform
  converted one field and the door stored another: the leaf kept the UTC wall through
  `INSERT OVERWRITE`, `INSERT … BY NAME`, `overwritePartitions` and
  `insertInto(overwrite=True)` while MERGE stored the session wall. The conform cannot guess
  which cast follows it, so the site says:
  - `cast` — the door runs Arrow's `cast` (`arrow-cast` `cast_struct_to_struct`): fields in
    order when the names are the target's in order; by name when every target name is among
    the source's; else by position, which needs at least as many source fields as the target
    has.
  - `name` — the door runs DataFusion's `Expr::Cast` (`datafusion-common`
    `nested_struct::cast_struct_column`, which `arrow_cast(…)` simplifies to as well): by
    name, case-sensitive; a target field with no source field would be filled with NULL; no
    shared name at all, or a nullable source field for a required target field, is refused.
    Under a list the same rule continues only for `List` into `List`; a `LargeList` source
    and a map fall to Arrow's `cast` there, and the pairing word switches with them
    (`Pairing::under_list`, `under_map`).
  - `exact` — the site cannot know the cast that follows: the source field names must be the
    target's, in order, at every struct on the way to the leaf.
- **Layouts (fold 2).** One `match` (`layout`) names every Arrow type and has no arm that
  stores by default. `Struct`, `Map`, `List`, `LargeList` conform in place. `ListView` and
  `FixedSizeList` are cast to `List`, and `LargeListView` to `LargeList`, then conform:
  fold 1 left a list view on the plain cast (the UTC wall) and answered a fixed-size list
  with a type the sink refused by a raw Arrow error. `Dictionary` and `RunEndEncoded` are
  decoded to their values and conform, at any depth. `Timestamp`, `Date32`, `Date64` and the
  string types are what a leaf can be read from. `Null` passes. A `Union` and every other
  scalar where a `timestamp_ns` leaf is wanted are refused.
- **The one refusal (fold 2).** `nested_refusal` builds it and nothing else does:
  ``[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the
  table ``: Cannot safely cast `<column>`.`<leaf>` "<source type>" to "TIMESTAMP_NS". The
  nested source shape cannot be stored into this timestamp_ns leaf: <reason>. SQLSTATE:
  KD000``. The reasons are `Unstorable`'s: the layout cannot carry a timestamp; the source
  nests differently from the target; no source field pairs with the leaf under the door's
  pairing; the leaf is required and the value is NULL; the source narrows a nanosecond value
  to microseconds; the statement cannot store a nested value. It is raised when the call is
  typed, that is while the statement is planned, except the required-leaf reason, which needs
  the data. **Why refuse an unpaired leaf under `name`:** DataFusion would store NULL there
  and keep the row; a `timestamp_ns` leaf then holds neither the value nor a refusal.
- **A value under a NULL struct is not read** (`under`): a child's slot under a NULL parent
  can hold anything, and under ANSI an out-of-range one raised `[CAST_OVERFLOW]` for a row
  that stores NULL.
- A `timestamptz_ns` leaf is not converted by the wall kernel (control; parity row
  ICE-TSNS-SQL-1-R-008).

pins: ice-tsns-merge-wall-1/C-016, C-018, C-027, C-028, C-029, C-031

## Contents

- `nested.rs` — **ICE-TSNS-MERGE-WALL-1 folds 1 and 2 (2026-10-09):** the nested form's
  type rule and array walk (`Site::conformed_type`, `Site::conform`), `Pairing`, the `layout`
  match, `Unstorable` and `nested_refusal`, described above.
  pins: ice-tsns-merge-wall-1/C-016, C-027, C-028, C-029
- `lineage.rs` — **fold 2 (2026-10-09):** `NestedNanosecondGuard`, an optimizer rule
  (`repark_nested_timestamp_ns_guard`) that finds every nested call of the kernel in a plan
  and refuses by name when what feeds one of its `timestamp_ns` leaves was narrowed from
  nanoseconds: a call of `__repark_narrow_timestamp_ns__`, or a cast from a nanosecond
  timestamp to a coarser one, a typed NULL excepted. It reads the call's own argument
  (`narrows_into`: through `named_struct`, `struct`, `array`, `get_field`, `CASE`, casts and
  the non-null wrapper, so a microsecond field beside the leaf may be narrowed on purpose) and
  then follows the columns that argument reads down the plan (`narrows_nanoseconds`:
  projections, aliases, filters, sorts, limits, unions, joins by side, `VALUES`; any other
  node is searched whole, and whatever it cannot see through counts as narrowing).
  The Spark extension (`repark-spark/src/extension.rs`) adds the rule for the Spark door and
  the ANSI door's `on_session_built` for its own (not `register_all`: this crate's `lib.rs` is
  at its size ceiling), so every door that emits the nested call is guarded by the one rule and no door
  carries its own check. **Why here and not at the cause:** `array(ns, NULL)`,
  `coalesce(ns, NULL)` and `CASE … ELSE NULL END` are typed `Timestamp(µs, "UTC")` before any
  store sees them (a NULL beside a nanosecond value is typed as the SQL `TIMESTAMP`, and the
  nanosecond value is narrowed to match), on main too and for a top-level column too; the
  store cannot recover the digits, so for a nested leaf it refuses. The typing itself is
  parity row ICE-TSNS-SQL-1-R-017. pins: ice-tsns-merge-wall-1/C-030
- `narrow.rs` — `__repark_narrow_timestamp_ns__`, moved out of the parent unchanged (fold 2).
- `nested_tests.rs` — the nested form's `#[cfg(test)]` suite: every depth under each pairing;
  the five list layouts; a dictionary and a run-end-encoded child; a union, an integer and a
  misnested source refused by name; the pairing table (twenty source spellings against the
  three words); the pairing under a list and a map; a required field by name; overflow; a NULL
  in a required leaf; a value under a NULL parent; a string leaf; the return field of both
  kernels; the literal arguments. pins: ice-tsns-merge-wall-1/C-016, C-018, C-027, C-028,
  C-029, C-031, C-034
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
