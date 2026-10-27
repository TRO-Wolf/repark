# v1.5.6 release notes (draft)

This draft collects one line per stored-value change as each unit of the 1.5.6 cycle lands, for
the release PR to edit into the notes.

Reconciled 2026-10-22: the `v1.5.5` tag was re-pointed on 2026-10-20 from the 2026-10-13 release
commit `293896ce` to `7fb5f6b2`, so every unit merged between those two commits shipped in v1.5.5
(ICE-TSNS-FLOOR-DOORS-1, ICE-TSNS-TYPE-PLANNER-1, ICE-STRUCT-PAIRING-1 and the SET walk,
REPARK-SQL-UPDATE-SUBQUERY-WHERE-1, PA-LOCAL-FS-POLICY-1, PA-DBT-MISMATCH-REDACT-1,
WRITE-BODY-CARRIERS-1, the fork repins RP-58 and RP-59). Their lines moved to
[the v1.5.5 notes](v1-5-5-release-notes-2026-10-13.md) under "Added by the re-pointed tag". What
stays here merged after `7fb5f6b2`.

WRITE-SPINE-1 step (3b) (PR #1185, awaiting verification) is test-only: it changes no stored value, so it adds no bullet below.

## Stored values that change

- **`map(<key>, <non-literal value>)` stores and answers (MAP-COLUMN-PANIC-1).** `SELECT map('a', id)` and `INSERT INTO t SELECT id, map('a', id)` over a column, a function or a cast value panicked `list array` (one source row) or raised `map requires key and value lists to have the same length` (several rows); they now answer Spark's rows and type on every door (`spark.sql`, `selectExpr`, `expr`, a string `filter`, INSERT, UPDATE, MERGE). A repeated key raises Spark's `[DUPLICATED_MAP_KEY]` message and a NULL key `[NULL_MAP_KEY]`, where DataFusion's `map key must be unique` and `map key cannot be null` came out; an odd argument count raises `WRONG_NUM_ARGS.WITHOUT_SUGGESTION` with the count on `spark.sql`. A map whose keys and values are all literals is unchanged.
- **`repark.sql()` and the Rust ANSI door answer `map(<key>, <column>)` too (MAP-COLUMN-PANIC-NATIVE-DOOR-1).** The native door and `repark_sql::AnsiDialect` panicked `list array` (one source row) or raised `map requires key and value lists to have the same length` (several rows) on a map over a column, a function or a cast value; it now answers the rows `spark.sql` answers, through the same per-row kernel. A repeated key raises Spark's `[DUPLICATED_MAP_KEY]` message and a NULL key `[NULL_MAP_KEY]` on this door too, where DataFusion's `map key must be unique` and `map key cannot be null` came out; an odd argument count and a map whose keys and values are all literals are unchanged.
- **A caught Rust panic fails the facade test suite (PANIC-FATAL-SUITE-1).** `REPARK_PANIC_FATAL=1` is now read once when the Python module initialises, not when a panic is caught, and the facade suite sets it; nothing a user stores or reads changes, and without the variable a caught panic still raises `repark internal error` with the same message.
- **The `DataFrame.replace` memory pin measures the operation, not the wheel (REPLACE-LINEAR-PIN-1).** The depth-40 pin's child now runs one depth-1 call of the mode it measures before it reads the peak, so first-touch code pages of the native module no longer count toward the delta; the bound, the 8 MiB floor and the depth are unchanged, and nothing a user stores or reads changes.
- **The small-fair-pool spill probes run in an 80 MiB pool, not 64 MiB (SPILL-REACH-FAIR-POOL-FLAKE-1).** `test_sort_merge_join_spills_under_small_fair_pool` refused `ExternalSorterMerge` intermittently on CI; the session helper of `test_t2_spill_reach.py` now defaults to 80M, every assertion and the two 16M pins are unchanged, and nothing a user stores or reads changes.
- **`DESCRIBE TABLE` and `DESCRIBE TABLE EXTENDED` name nanosecond columns by the Iceberg spec (R-001, NS-TRUTHUP-1).** A `timestamp_ns` column shows `timestamp_ns` and a `timestamptz_ns` column shows `timestamptz_ns`; they showed `timestamp_ntz` and `timestamp`. The microsecond columns keep their names, and `DESCRIBE <table> <column>`, `SHOW CREATE TABLE`, `df.schema`, `dtypes` and `printSchema` are unchanged. No stored value changes.
- **`UPDATE` and `DELETE` on the nanosecond cast spellings are pinned (R-010, NS-TRUTHUP-1).** `CAST(… AS timestamp_ns)` and `CAST(… AS timestamptz_ns)` in `SET`, `WHERE` and `DELETE … WHERE` store and match on UTC and America/New_York. The store landed with ICE-TSNS-TYPE-PLANNER-1, which the reconciliation above moves to the v1.5.5 notes; this release adds the pins and no new value change.

Tables already written keep the values they hold; this release ships no migration.

- **Container casts, `TRY_CAST`, `to_json`, `from_json` and `to_utc_timestamp` read timestamps in
  the session zone (ZONE-SEAM-1).** A `CAST` or `TRY_CAST` of a struct, array or map holding a
  `TIMESTAMP` now answers what the leaf cast answers: the session wall for `TIMESTAMP_NTZ`,
  Spark's session text for `STRING`, the session date for `DATE`, epoch seconds for `BIGINT`; a
  wall, a zoneless string or a date cast to `TIMESTAMP` inside a container is read in the session
  zone; a wall to `BIGINT` and a `BIGINT` to `TIMESTAMP_NTZ` inside a container refuse as Spark
  does. A bare `TRY_CAST` of those pairs answers what `CAST` answers. `to_json` renders a
  `TIMESTAMP_NTZ` field as its wall with no zone; `from_json` reads a `TIMESTAMP_NTZ` field as its
  wall and answers NULL for a text with a zone; `to_utc_timestamp` and `from_utc_timestamp` of a
  wall, a date or NULL return `TIMESTAMP`. Writes store what the `SELECT` yields, so the six
  table-creating writers and every door holding such a cast store Spark's value; a container cast
  of an instant into a nested `TIMESTAMP_NTZ` field now stores the session wall where it refused.
  ZONE-ORACLE-1: 1,540 of 1,640 cells EQUAL (was 1,199).
- **Nested literal and constant timestamps store Spark's session-zone value outside UTC (NESTED-LITERAL-SEAM-1).** A date literal under the row-merge CASE of a `MERGE` whole-struct or `SET *` update, a constant struct holding a date literal on an `UPDATE` (partitioned or not), a date literal or column used as a `MAP<TIMESTAMP, …>` key on an `INSERT`, a `MERGE` update or an `UPDATE`, and a `TRY_CAST` of a date or wall into a nested `TIMESTAMP` on an `UPDATE` now store the session-zone reading (the session midnight for a date) where they stored the UTC reading. `DATE '2026-01-02'` into a nested `TIMESTAMP` stores 1767330000000000 microseconds in New York and 1767292200000000 in Kolkata, where it stored 1767312000000000 in both. The change moves 52 census cells of the write matrix, all on the merge, update and insert doors, and no other cell; a constant array or map holding a date already stored Spark's value on main and does not move; a value Spark refuses stays refused. The `CAST` and `TRY_CAST` forms under the `MERGE` CASE still refuse, as do a wall literal under that CASE and a wall literal in a constant on an `UPDATE` (ZS-9), as before.
- **A `CAST` of a map with a timestamp key reads the key in the session zone (NESTED-LITERAL-SEAM-1).** This is a read value, not a stored one: `CAST(<map> AS MAP<TIMESTAMP, STRING>)` and `CAST(<map> AS MAP<TIMESTAMP_NTZ, STRING>)` of a map whose key is a date, a wall or an instant now answer Spark's session-zone key (a date key the session midnight) where they answered the UTC reading. No stored value changes; a table already written keeps the values it holds, and the change shows only in the read.

## Behaviour changes

- **A nested `TIMESTAMP_NTZ` value into a nested `TIMESTAMP` field refuses outside UTC on the
  overwriting, by-name, MERGE and whole-struct `UPDATE` doors (ZONE-SEAM-1, ruling ZS-9).** Those
  doors store the wall as if it were UTC; until WRITE-NTZ-TO-LTZ-NESTED-1 they refuse with a hint
  (set the session zone to UTC, or cast the leaf to `TIMESTAMP` before nesting it). Positional
  `INSERT`, `REPLACE WHERE`, the appending DataFrame writers and a field assignment store Spark's
  value.

- **An instant written into a nested microsecond `TIMESTAMP_NTZ` field refuses outside a UTC
  session (ICE-NTZ-NESTED-REFUSE-1, R-011).** Every door now refuses it with
  `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]` and a hint, where before field assignment
  stored the session wall and every other door the UTC wall; the leaf cast
  `CAST(<value> AS TIMESTAMP_NTZ)` still stores, and a UTC session still stores for every
  statement but `PREPARE` (a `PREPARE` of an instant or of a parameter that supplies the
  column refuses in every zone, UTC included), while a `CAST` of a whole
  struct, array or map holding an instant, a parameterised `PREPARE` that supplies the column,
  and a `TIMESTAMP_NTZ` cast inside a struct assigned by a plain `UPDATE` refuse too. Nested `TIMESTAMP_NTZ` values that `INSERT` wrote before this
  release in a session whose zone was not UTC hold the UTC wall. Not gated (CONTAINER-CAST-ZONE-1,
  owner's ruling pending): a container cast or `from_json(to_json(…))` whose zoned leaf passes
  through `STRING`, `DATE` or `BIGINT` still stores the UTC wall outside UTC, and the six
  table-creating writers (CTAS, `CREATE OR REPLACE`/`REPLACE TABLE … AS SELECT`,
  `saveAsTable` in mode `overwrite`, `writeTo().create()`/`.replace()`/`.createOrReplace()`)
  carry the same value. Refused by design, measured (R-011 lists each class): 16 correct cells
  (8 shapes; the count covers the shapes measured and the class is open-ended) where a cast in an
  aggregate, a grouping key, a `CASE`/`IF` condition or a sibling field never
  reaches the stored value (accepted as cost until GATE-RETIRE-1, owner ruling 2026-10-21); 17
  `PREPARE` cells; and every instant write in a geographic zone at offset zero
  (`Africa/Abidjan`, `Atlantic/Reykjavik`, `Europe/London` in winter: 126 correct cells per zone
  on the 28-door matrix), whose tzdb history holds a local-mean-time or summer offset. A pure
  UTC alias (`UTC`, `UCT`, `Universal`, `Zulu`, `Z`, `GMT`, `GMT0`, `GMT+0`, `GMT-0`,
  `Greenwich`, their `Etc/` forms) counts as UTC (owner ruling 2026-10-21). An `IN`, `EXISTS` or quantified subquery in the value is not read, and a scalar
  subquery is read through its output column (owner ruling 2026-10-21), so a cast that only
  feeds such a subquery's predicate stores.
  An `INSERT` of a `named_struct` holding a `TIMESTAMP_NTZ` value in a non-UTC session zone
  carries about 3 to 4 ms of planning per statement (the type read) until GATE-RETIRE-1 (owner
  ruling 2026-10-21).

- **`EXPLAIN ANALYZE` and `PREPARE` of a write refuse and write nothing (EXPLAIN-DML-WRITE-1,
  #1137 at `b7a20167`).** On every door `EXPLAIN ANALYZE` of a `DELETE`, `UPDATE`, `INSERT` or
  `MERGE`, and `PREPARE` of one, refuse and write nothing, as Spark refuses both; `EXECUTE` then
  finds no prepared statement. A plain `EXPLAIN`, `PREPARE` of a query and `EXPLAIN ANALYZE` of a
  query still answer. On `repark.sql()`, which plans first, every wrapped write that DataFusion
  cannot plan keeps DataFusion's own error and writes nothing: a `MERGE`, a CTE-carried `DELETE`,
  `UPDATE` or `INSERT`, a DML-in-CTE query, a missing table or column, `EXPLAIN ANALYZE … FORMAT x`,
  `DESCRIBE ANALYZE`. In v1.5.5 `EXPLAIN ANALYZE` of a `DELETE` or `UPDATE`, and `PREPARE` then
  `EXECUTE` of one, performed the write, and with a subquery in the `WHERE` changed every row of
  the table (the v1.5.5 notes list it as a known issue).

- **Ordered writes, merge-on-read MERGE and changelog reads refuse above a stated memory bound (ICE-RESOURCE-BOUND-1, U-11).** Each path has a key, read at build: `repark.write.ordered.max-input-fraction` (0.10), `repark.merge.mor.max-updated-fraction` (0.05) and `repark.changelog.max-source-fraction` (0.0625), times the live memory pool. Above it the statement refuses with the pool's `ResourcesExhausted` class (`PySparkException`) before any file is staged. The value 0 means no bound. Default pool detection is closed for cgroup v2 (the leaf-and-ancestors minimum of `memory.max`, capped by `MemTotal`); cgroup v1, macOS and Windows need an explicit pool budget.

- **Format-v2 Iceberg reads and writes carry the PRODUCTION label as of this release
  (ICE-V2-LABEL-1).** Three carve-outs stay named on the label: ENC-1 items 1 and 5 (ruling 121 (b))
  and ENC-CTAS-REPLACE-1 (ruling 98 (a)); the text-leaf row NESTED-TEXT-LEAF-STORE-1 is carried by
  owner ruling 2026-10-26 and fixed in 1.5.7. The label itself changes no stored value; the stored-
  value changes of this release are the bullets above.

## Integrity under failure (no stored value or behaviour changes)

- **A writer killed between its data files and its commit is pinned (ICE-INTEGRITY-PINS-1, pin
  K).** On the local catalog, a process killed inside the commit of an `INSERT`, `INSERT
  OVERWRITE`, `MERGE` (copy-on-write and merge-on-read) or `DELETE`, on format v2 and v3, before
  the metadata file is written, leaves the table exactly as it was when reopened through `CALL
  … system.register_table`. The files it wrote stay on disk, never more than the crashed commit
  wrote, and `remove_orphan_files` lists them once past its 24-hour floor. A process killed
  inside the same commit call after the metadata file is written reads back as committed. A `CREATE TABLE
  … AS SELECT` killed at its publish reads back as created, because the local catalog's commit is
  its newest metadata file (registry row ICE-LOCAL-COMMIT-POINT-1). Glue and S3 Tables are
  drilled by the live legs once the workflow is wired (#1070).
- **An unknown commit outcome and a cross-release read are pinned (ICE-INTEGRITY-PINS-1, pins U
  and X).** Pin U pins the unknown commit outcome locally on three write doors, with live legs on
  Glue and S3 Tables. Pin X shows that 1.5.5 and the head (the pre-bump build, which still reports 1.5.5) read each other's tables equal on the local
  catalog. Both legs hold live (2026-10-23, run 40025222296): leg U 96 of 96 cells and leg X 40 of 40 on Glue and S3 Tables, v2 and v3. The cross-read module of U-10 remains open (ledger C-060): run 4 timed out in Glue RePark row collection and S3 Tables PyArrow metadata-file I/O. Native diagnostics are the next measurement.

- **An unfiltered `UPDATE` of an out-of-range `TIMESTAMP` literal reports INSERT's overflow (ICE-TSNS-UPDATE-RANGE-CLASS-1, R-012).** Under ANSI the statement refuses with `[CAST_OVERFLOW]` and INSERT's text, as ICE-TSNS-TYPE-PLANNER-1 left it in v1.5.5; this release pins the 16 cells and changes no stored value or refusal.

- **The Spark SQL door's replace statements are pinned for a writer killed at the publish
  (ICE-REPLACE-KILL-DOORS-1, test only).** `CREATE OR REPLACE TABLE … AS SELECT`, with or without a
  session write key, and `REPLACE TABLE … AS SELECT` are pinned on format v2 and v3 on the local
  catalog: the table reads back as replaced, with no orphan. The native door's replace is not yet
  pinned. No stored value changes.

## Known issues

- **REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 (known issue, updated 2026-10-17):** `repark.sql()` refuses
  every `UPDATE` and `DELETE` whose `WHERE` holds a subquery, including the `IN`, `NOT IN` and
  `EXISTS` forms the Spark SQL session answers with the matching rows, bare, under `PREPARE`
  and under `EXPLAIN ANALYZE`; a plain `EXPLAIN` answers its plan. Run those statements through
  the Spark SQL session until `repark.sql()` is routed through the ANSI door. On the Spark SQL
  session the bare forms keep the matching rows, and `PREPARE` of such a statement, alone or
  around `EXPLAIN ANALYZE`, now refuses with the same G3-E8 text instead of changing every row at
  `EXECUTE`; `EXPLAIN ANALYZE` of the statement itself still writes there (EXPLAIN-DML-WRITE-1).
  *(VF-2, measured wording, 106 ledger §13.6: on main the prepared subquery forms emptied the table,
  rewrote every row, or wrote the wrong rows (`IN … AND v > 25` deleted ids 3 and 4), then
  `EXECUTE`. From EXPLAIN-DML-WRITE-1 (merged #1137, `b7a20167`), every wrapped write on every door,
  these forms included, refuses with the wrapper's text instead of the G3-E8 text. On
  `repark.sql()` a wrapped write that DataFusion cannot plan keeps DataFusion's own error instead,
  and writes nothing either way.)*

GATE-RETIRE-1 (2026-10-25, PR #1187): R-011's container-cast gate arm is retained, not retired. A verifier round found it load-bearing for whole-container casts whose fields do not line up with the target (an arity mismatch, or fields at different positions), which the zone seam does not rewrite and the arm refuses in New York. The micros hint now says such a cast is refused unless its fields line up with the target's. No cell moved in the step-0 census (0 of 12192 cells).
