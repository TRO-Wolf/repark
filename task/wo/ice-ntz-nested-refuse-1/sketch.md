# ICE-NTZ-NESTED-REFUSE-1 (U-8, R-011, ruling T-3) — step 0: the sketch

**Date:** 2026-10-20. **Model:** Claude Opus 5.5 (`claude-opus-5-5`). **Base:** main `c516c54a`.
**Order:** [order.md](order.md) (plan row U-8 of
[the 2026-10-20 re-prioritisation](../../../docs/artifacts/iceberg-closeout-priorities-2026-10-20.html),
owner-adopted the same day). **Parity row:** ICE-TSNS-SQL-1-R-011 in
[docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md). Written before any
product code. This file closes when the unit's pull request merges.

**Verdict of step 0: the lean holds, with two corrections (§3, §5).** The session zone is
readable where the router decides; every door in the census reaches the router gate before it
writes (R-015 proves the path); the type of the value that reaches the leaf is readable by
planning one `SELECT` through the same router, without executing it. Main was measured with the
door matrix of PR #1037 (`probe.py`, §6) plus two sources: the hint's own spelling
`CAST(<instant> AS TIMESTAMP_NTZ)` from a literal and from a column. That measurement found a
silent wrong wall the row does not name (§3), and the rule is widened to it.

## 1. The rule

A write is refused, before any file is written, when ALL of these hold:

1. **Zone.** The session zone is not UTC. A zone is UTC when its canonical id
   (`repark_functions::session_time_zone::session_time_zone_from_options`, which already folds
   `Z`, `UT`, `GMT`, `UTC` and the `GMT+0` spellings) is `UTC`, `+00:00` or `-00:00`. Any other
   id refuses, `Etc/UTC` included (fail closed; the hint names `UTC`). A statement carried by
   `PREPARE` is decided as if the zone were not UTC, because `EXECUTE` can run it after a
   `SET spark.sql.session.timeZone`.
2. **Leaf.** The target column's type holds a naive microsecond timestamp below the top level
   (struct field, any list layout's element, map key or value, any depth): R-015's walk with
   the unit as a parameter, `Timestamp(Microsecond, None)`.
3. **Supply.** The statement supplies the column: R-015's `Supply` reading, unchanged (a listed
   or positional column, a by-name source carrying it, an assignment to it or into it, a MERGE
   clause that sets or inserts it, a star, a statement the router cannot parse). A bare `NULL`
   for the whole column and an assignment into a sibling field that holds no such leaf
   (`SET st.n = 1`) do not supply it.
4. **Value.** At least one value that reaches the column is not provably naive:
   - its planned type holds a zoned timestamp (`Timestamp(_, Some(_))`) anywhere, or
   - the router cannot plan it (fail closed), or
   - **(correction 1, §3)** it is a plain `UPDATE` assignment whose path stops above the leaf
     (`SET st = …`, `SET st.inner = …`) and any sub-expression of the planned value has a zoned
     type (`named_struct('v', CAST(c AS TIMESTAMP_NTZ), …)` with `c` a `TIMESTAMP`).

A naive value whose type the router reads is not refused: a `TIMESTAMP_NTZ` literal or column,
`CAST(… AS TIMESTAMP_NTZ)` on every door but the one in correction 1, a string, a date. A UTC
session refuses nothing. The nanosecond leaf keeps R-015 exactly (decided first, its own text).
A `timestamptz_ns` or zoned microsecond leaf is a control and is not matched (R-008).

**Where a zoned leaf sits in the value is not read.** A value holding a zoned leaf anywhere is
refused even when that leaf pairs with a zoned sibling of the target (`struct<v: TIMESTAMP_NTZ,
w: TIMESTAMP>` written from `named_struct('v', ntz, 'w', ltz)`). Pairing a source leaf with a
target leaf is R-016's rule and differs by door; reading it here would be a second copy of it.
This over-refusal is stated in the registry row.

## 2. Decision point and seam

- **Where:** `crates/repark-spark/src/router/nested_ns.rs` `refuse_at`, after R-015's call, on
  the same loaded table, before the metadata-table, changes, branch, WAP and time-travel
  rewrites (R-015's placement, so a branch reference or a WAP setting does not change the answer).
- **One walk, one supply model, one refusal** in `crates/repark-iceberg/src/write/nested_ns_gate.rs`:
  - `NaiveLeaf::{Nanos, Micros}` parameterises the walk (`nested_naive_leaf`) and the text
    (`nested_naive_refusal`). R-015's text is byte-identical.
  - The supply functions (`insert_supply`, `assigned`, `inserted`, `merged`) return the supply
    and, for a `Value`, the **feeds**: where each value comes from. `Feed::Expr` (an assignment
    or a MERGE `VALUES` cell, with the field path below the column), `Feed::SourceColumn` (an
    INSERT source column by position and/or name), `Feed::SourceStar` (a MERGE star),
    `Feed::Opaque` (a tuple assignment, `INSERT ROW`, an unparsed statement). R-015 reads only
    the supply, as today. No second copy of the model.
  - `nested_supplies(table, write, leaf)` answers the supplied columns with their leaf path and
    feeds; `refuse_nested_ns_supply` is R-015 over it; `holds_zoned_timestamp(type)` is the zone
    test. Pure functions, unit-tested at the function.
- **The type read (router):** one `SELECT` planned through `router::execute_calibrated`, the
  Spark door's own planner, and never executed:
  - INSERT (plain, `OVERWRITE`, `BY NAME`, `REPLACE WHERE`, the DataFrame writers' lowering):
    the statement's source query. The column's value is the source column at its position, the
    one of its name for a by-name write, or, when neither resolves (a static `PARTITION`
    clause, an arity mismatch), every source column.
  - UPDATE: `SELECT <values> FROM <table> [AS <alias>]`; MERGE:
    `SELECT <values> FROM <table> [AS <alias>], <source>`; a MERGE star:
    `SELECT * FROM <source>` and the column of the target column's name, else every column.
  - The table in `FROM` is the loaded table's qualified name, so `t.branch_x` reads as `t`.
  - A plan error is a refusal (fail closed). The read costs one extra planning of the source
    and runs only for a table with such a leaf, in a non-UTC session, on a supplying statement.

## 3. Correction 1: the plain `UPDATE` door shifts a `TIMESTAMP_NTZ` cast inside a struct

*Corrected 2026-10-20 (fix round 1, verifier finding S2-1).* This section first said the door
"converts inside a struct with UTC". The measured mechanism: the door adds the session offset
to every `TIMESTAMP_NTZ` cast inside the assigned struct or array, whatever the cast's input.
The cast of an instant first yields the session wall, and the offset lands it on the UTC wall
(the table below). The cast of a string yields the wall, and the offset shifts it:
`named_struct('v', CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP_NTZ), 'n', 1)` stores
`1767341045123456` in New York and `1767303245123456` in Kolkata, where Spark stores
`1767323045123456`. A `TIMESTAMP_NTZ` literal, `to_timestamp_ntz`, `make_timestamp_ntz`, a
column, the field assignment and the MERGE door store the wall. Fix round 1 refuses every
`TIMESTAMP_NTZ` cast in such a value outside UTC (ledger C-017).

Measured on main, `struct<v: TIMESTAMP_NTZ, n: INT>`, America/New_York:

| Statement | stored `st.v` | Spark's rule |
|---|---|---|
| `UPDATE t SET st = named_struct('v', CAST(TIMESTAMP '2026-01-02 03:04:05.123456+00:00' AS TIMESTAMP_NTZ), 'n', 1) WHERE id >= 0` | 1767323045123456 (UTC wall) | 1767305045123456 |
| the same from a `TIMESTAMP` column `c` | 1767323045123456 (UTC wall) | 1767305045123456 |
| `UPDATE t SET st.v = CAST(<the literal> AS TIMESTAMP_NTZ) WHERE id >= 0` | 1767305045123456 | 1767305045123456 |
| `MERGE … UPDATE SET t.st = named_struct('v', CAST(<the literal> AS TIMESTAMP_NTZ), 'n', 1)` | 1767305045123456 | 1767305045123456 |

Every other door stores the session wall for the `CAST` spelling (132 of 140 answering cells,
§5). The value's own type is naive, so the brief's type rule passes it and the hint would lead a
user into a silent UTC wall (lessons: a hint must not store a wrong value). The rule
therefore also reads the planned value's sub-expressions on that one door. The hint names the
field assignment for UPDATE, which stores the session wall.

## 4. Text

``[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table
`<catalog>`.`<namespace>`.`<table>`: Cannot safely cast `<column>`.`<leaf>` to "TIMESTAMP_NTZ".
A TIMESTAMP value is not written into a nested TIMESTAMP_NTZ field while the session time zone
is not UTC: write the field from CAST(<value> AS TIMESTAMP_NTZ) to store the session wall time
(in an UPDATE, assign the field itself), or set spark.sql.session.timeZone to UTC for the
statement. SQLSTATE: KD000``

R-015's class, first sentence and SQLSTATE; the second sentence is RePark's. It names no
dropping, truncating, overwriting or recreating action (ruling 76(a)). Both ways it offers store
a value Spark stores: the `CAST` way stores Spark's session wall on every door that still answers
(§5); the UTC way stores what Spark stores in a UTC session.

## 5. Cells (measured on main `c516c54a`, Rust door through the facade, format v2)

`probe.py` with five sources: `ts_literal`, `ts_column` (instants), `ntz_literal` (a wall),
`ntz_cast`, `ntz_cast_column` (the hint's spelling). 28 doors, the 16 row-level ones in both
modes, three zones, three schemas: 1980 cells. For `struct<v: TIMESTAMP_NTZ, n: INT>`:

| Group | Main | Predicted after |
|---|---|---|
| instant, New York and Kolkata | 108 UTC wall, 32 session wall (field assignment), 36 raw errors | all 176 refuse with the text |
| `CAST` spelling, New York and Kolkata | 132 session wall, 8 UTC wall (`update_whole_where`), 36 raw errors | 132 unchanged; the 8 and the 8 `update_whole_nowhere` raw errors refuse; 28 raw errors unchanged |
| wall literal, New York and Kolkata | 72 wall, 16 raw errors | unchanged |
| UTC session, every source | 176 stored, 44 raw errors | unchanged |
| `struct<v: TIMESTAMP>` and top-level `TIMESTAMP_NTZ` controls | 1320 cells | unchanged |

Per door (New York and Kolkata, instant sources): `insert_values` 2 UTC wall + 2 raw,
`insert_overwrite_values` 2 + 2 raw; `insert_select`, `insert_by_name`, `insert_overwrite`,
`insert_replace_where` and the six DataFrame doors 4 UTC wall each; `merge_insert`,
`merge_insert_literal`, `merge_insert_star`, `merge_update_whole`, `merge_update_whole_literal`,
`merge_update_star`, `update_col_where`, `update_whole_where` 8 UTC wall each;
`merge_update_field`, `merge_update_field_literal`, `update_field_where`,
`update_field_col_where` 8 session wall each; the four `*_nowhere` doors 8 raw errors each. The
row's 106 and 28 were measured on another matrix (format v3, §9.2 of the merge-wall ledger);
this unit restates its own counts and does not reuse them. **The 32 field-assignment cells
refuse too:** one rule, one text; their value was right, and the row says so.

## 6. Doors in scope

Every SQL door of the fable-q3 census (§2 of
[../ice-ntz-nested-wall-1/fable-q3/answer.md](../ice-ntz-nested-wall-1/fable-q3/answer.md)): the
UPDATE and MERGE folds, the MERGE star, the predicate-DML UPDATE projection, INSERT OVERWRITE and
BY NAME, the INSERT hook; the DataFrame writers (`append`, `overwrite`, `overwritePartitions`,
`insertInto`, `saveAsTable` into an existing table); `EXPLAIN ANALYZE` of a DML; `PREPARE … AS` a
write; `CREATE TABLE … AS INSERT`; each with and without a branch reference. Not refused:
`CALL system.add_files`, `CREATE TABLE … AS SELECT`, compaction and the other maintenance calls,
reads and time travel, `DELETE`, schema evolution, the native `repark.sql()` door and the Rust
`append` API (below the router; R-015 states the same boundary).

## 7. Pins

- Rust door: `crates/repark-spark/tests/timestamp_ns_wall_doors.rs`
  `a_nested_microsecond_ntz_field_keeps_the_split_main_has` is rewritten (dated) into
  `a_nested_microsecond_ntz_field_refuses_an_instant_outside_utc`: its six doors and the
  `CAST` spellings in three zones, the text, no file written, and in UTC the walls it stored
  before. A new `crates/repark-spark/tests/ntz_nested_refuse.rs`: every door of §6 refuses, the
  allowed statements run, the controls, the branch and the carriers, the hint's two ways store
  Spark's value.
- R-015's `a_nested_zoned_or_microsecond_leaf_is_not_guarded` (`timestamp_ns_nested_shapes.rs`)
  writes an instant into a nested `TIMESTAMP_NTZ` in New York; its `TIMESTAMP_NTZ` row goes red
  by design and is re-pinned to the refusal with a dated note.
- Gate unit tests: the walk per unit, the feeds of each statement form, the zone test.
- Facade: the DataFrame writers from an Arrow frame with a zoned nested leaf and a naive one.
- Live Spark: #1037's measured shapes re-run on Spark 4.1.2 and RePark: Spark stores the session
  wall, RePark refuses; the hint's `CAST` spelling stores Spark's value on both.
- Ledger `task/ledgers/staging/ice-ntz-nested-refuse-1-ledger.md`; each pin cites
  `ice-ntz-nested-refuse-1/C-NNN`.

## 8. What a verifier should attack

The type read's coverage: a value whose planned type is naive but whose door converts it
(correction 1 is the one found; the matrix covers 28 doors and five sources only). The `FROM`
synthesis for aliases, branch targets and MERGE sources. The by-name and positional pick. The
zone spellings. `PREPARE` across a zone change.
