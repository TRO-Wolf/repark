# Card ICE-TSNS-NESTED-1: a nested `timestamp_ns` leaf stores the session wall on every door

**Date:** 2026-10-09. **Filed by:** Claude (Opus 5.5), build lane of ICE-TSNS-MERGE-WALL-1, from
the owner-adopted amendment 3 of pull request #1018. **Source:** three Opus verifies of that pull
request, the last one failing the nested form for the third time.

**Status:** open. Since ICE-TSNS-MERGE-WALL-1 a write that supplies a value for a column whose
type holds a naive `timestamp_ns` leaf below the top level is refused by name before any file is
written (parity row ICE-TSNS-SQL-1-R-015, ledger
[ice-tsns-merge-wall-1-ledger.md](../../ledgers/staging/ice-tsns-merge-wall-1-ledger.md) §11).
This card is the work of lifting that refusal.

## Why

A top-level `timestamp_ns` column stores one session wall from every door. A nested leaf did
not: on main `ca5a062a` the same instant stored the session wall through one door and the UTC
wall through another, into one table. Two folds tried to conform the nested value in place. Each
time the verify found a route that stored a wrong or a cut value. The owner's ruling was to
ship the top-level fix and refuse every nested write by name until the nested form is designed
as its own unit.

## Starting evidence

All counts are the verifier's, values read from the Parquet files, five session zones in the
last round. The scripts and outputs are in the verifier's scratch directory of that day
(`reverify2/s`, `reverify2/s/out`); the lane's re-run on the split is in the ledger's §11.

### The three verifies

| Round | Head | Cells | Nested result |
|---|---|---|---|
| Verify | `1be18c26` | 16,383 | One S1: a nested leaf stored the UTC wall on the overwrite and MERGE doors and the session wall on the others. Two S2 out-of-range classes, top level. |
| Re-verify 1 (after fold 1) | `59462d6a` | 22,495 | Two S1 and one S2, all nested: struct fields paired by a rule the door did not use (a renamed field stored NULL or another field's value), Arrow layouts the conform did not walk, and a value cut to microseconds inside a nested leaf. |
| Re-verify 2 (after fold 2) | `c6d947a3` | 72,077 | One nested S1 and one nested S2 (below); two top-level S1, fixed in the split. |

### Re-verify 2, nested cells by outcome (39,125)

| Outcome | Cells |
|---|---|
| Stored the rule's wall at full precision | 20,220 |
| The fold's named refusal, nothing written | 6,570 |
| The statement's own `VALUES` list fails, as on main (parity row R-018) | 355 |
| Another refusal, identical to main | 11,980 |

The 11,980 are: 6,250 `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA]`, 750
`[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS]`, 4,250 `MERGE INSERT / UPDATE SET cannot
store-assign column` (every array-bearing column through MERGE), 700 planner errors with no
name, and 30 raw errors in the probe's own setup.

### The three leak routes (S1)

A nanosecond value cut to microseconds upstream reached a nested leaf at `c6d947a3` through
three routes the fold's lineage guard did not see (`leak.py`):

1. An array element: `named_struct('v', array(ns, NULL)[0], 'n', 1)`. Cut, and the wall moved
   by the zone offset; identical to main.
2. A SQL temporary view: `CREATE TEMPORARY VIEW sv AS SELECT id, coalesce(ns, NULL) AS x …`,
   then `named_struct('v', x, 'n', 1)` from the view. The right wall, cut.
3. A cached frame: `spark.sql("… named_struct('v', coalesce(ns, NULL), 'n', 1) …").cache()`
   then `writeTo(t).append()`. The right wall, cut.

A lineage guard reads the plan it is handed. A view and a cached frame reach the store as a
scan, so no walk of that plan finds the narrowing. The unit needs either correct typing
upstream (parity row R-017) or a store that cannot be handed a narrowed value.

### The 700 planner-error cells (S2)

A source struct with a missing or an extra field, inside an array or a map, through
`INSERT … SELECT`, `REPLACE WHERE`, `append`, `saveAsTable` and `UPDATE` with no `WHERE`
(`nx2.py`, shapes `ap_missing`, `ap_missingb`, `ap_extra`, `mp_missing`, `mp_missingb`,
`mp_extra`) fails with DataFusion's `Cannot automatically convert List(Struct(…)) to
List(Struct(…))`, and the `Map` form. Same class and text on main; nothing written.

### The pairing question (parity row R-016, ledger Q8)

For every leaf type, the INSERT family pairs struct fields by name where Spark pairs by
position, and `INSERT … BY NAME` and `overwritePartitions` pair by Arrow's rule where Spark
pairs by name (80 Spark 4.1.2 cells on `TIMESTAMP_NTZ` leaves, ledger §10.1). Fold 2 made the
conform follow each door's own rule and refuse where a leaf would be left NULL. The re-verify
measured that as wider than described: 1,125 cells where a renamed, recased or missing field
left the leaf NULL on main, and a further 525 cells (arrays and maps of structs through
`UPDATE … WHERE` and `MERGE INSERT`) where main stored a value, the rule's wall in 357 and the
UTC wall in 168. The unit cannot store a nested value before it decides which field a source
field pairs with on each door.

### The layouts

Every Arrow layout that can carry a timestamp inside a nested value, each of which the
conform must walk or refuse: `Struct`, `Map` (keys and values), `List`, `LargeList`,
`ListView`, `LargeListView`, `FixedSizeList`, a `Dictionary` child and a `RunEndEncoded`
child. `Dictionary<Struct>` could not be built through pandas in the verifier's probe (300
build errors, not door outcomes). On both builds a `FixedSizeList` into a nested `TIMESTAMP`
or `timestamptz_ns` column raises Arrow's `column types must match schema types`.

### Pins that carried no digit below the microsecond

The re-verify's list, each of which a new unit must pin with such digits and read from the
Parquet file: struct in struct, `array<struct>`, `struct<array>`, map values of both kinds,
map keys (no door pin at all), Arrow `List`, `LargeList`, `ListView`, `LargeListView`,
`FixedSizeList`, the `Dictionary` child, the `RunEndEncoded` child, every kernel unit test of
the nested form, and the carry pins outside `struct<a: timestamp_ns, b: TIMESTAMP>`. No lane
pin read a nested value from a Parquet file. Two guard mutants survived every Rust pin: a cast
from nanoseconds to microseconds read as not narrowing, and a `UNION` read as narrowing only
if every branch narrows.

## The ask

Design the nested store as one unit, then lift the refusal:

- one pairing rule per door, decided against Spark (R-016), before any conversion;
- one conversion that walks every layout above, or refuses it, with no arm that stores by
  default;
- no cut value: fix the typing of a nanosecond value beside an untyped NULL (R-017) first, or
  prove the store cannot receive one through a view, a cached frame or an array element;
- the gate `repark_iceberg::write::nested_ns_gate::refuse_nested_ns_supply` is the one place
  to relax, door by door.

## What the refusal leaves open today

- The DataFrame doors are refused when the frame has a column of the target's name, a
  `lit(None)` column included; only a SQL `NULL` literal for the whole column, or a statement
  that does not name the column, stores.
- `CREATE TABLE … AS SELECT`, `REPLACE TABLE … AS SELECT` and a column added by schema
  evolution are not refused: the leaf takes the source's own type and nothing is converted.
- `CALL system.add_files` is not refused either (ruled 2026-10-10): it registers a Parquet
  file and the table takes the file's own values, as CTAS takes its source's. Unchanged from
  main. One case for this unit: a file whose leaf column is a microsecond instant
  (`timestamp[us, UTC]`) is accepted for a `timestamp_ns` leaf and reads back as the UTC
  wall (`1767323045123456000` for `2026-01-02 03:04:05.123456Z`) in every session zone, where
  the rule would be the session wall or a refusal. A file whose leaf is `timestamp[ns]` reads
  back as written.
- `UPDATE t SET st = NULL` with no `WHERE` raises the raw Arrow
  `arguments need to have the same data type`, as on main; with a `WHERE` it stores NULL. The
  unfiltered `UPDATE` cannot carry or assign any nested column (parity row R-014).
- The streaming sink and the Rust `repark_iceberg::write::append` API are below the SQL router
  and are not gated. The ANSI door cannot create such a column.
- A write through a branch reference (`t.branch_x`, `branch_main` included) or under
  `spark.wap.id` / `spark.wap.branch` is refused like any other: the gate runs before the
  router rewrites the target. The first split left it after that rewrite and ten routes
  stored the UTC wall, as main does.

## Siblings

- **R-011** (ledger Q4): a nested microsecond `TIMESTAMP_NTZ` field stores the session wall
  through field assignment and the UTC wall through every other door, on main and today. Same
  shape of fault, a microsecond control, 134 measured cells. Not refused, not changed.
- **R-016**, **R-017**, **R-018**, **R-014**: the pairing rule, the typing beside an untyped
  NULL, the `VALUES` spellings that fail before any store, and the raw nested refusals.

## Gates

- every shape and every layout above, every door, with digits below the microsecond, values
  read from the Parquet files, five zones;
- the three leak routes store every digit or cannot be written;
- no control moved: `timestamptz_ns` and microsecond leaves byte-identical to main;
- an Opus verifier on the product pull request.
