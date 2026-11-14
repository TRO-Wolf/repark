# v1.5.6 release notes (2026-11-08)

Every change on `origin/main` after the `v1.5.5` tag (`7fb5f6b2`), one line per user-visible change.
The units merged between `293896ce` (the original v1.5.5 commit) and `7fb5f6b2` shipped in v1.5.5;
their lines are in [the v1.5.5 notes](v1-5-5-release-notes-2026-10-13.md) under "Added by the re-pointed tag".

## Data-safety fix

A DELETE or UPDATE whose condition the optimizer folds to "matches nothing" (for example `WHERE false`, `WHERE NULL`, a comparison with a NULL struct, and on `repark.sql()` also `WHERE 1 = 0`, `WHERE col = NULL`, `WHERE required_col IS NULL`) deleted every row, or rewrote every file and on format v3 marked every row updated. Measured on 1.5.5; earlier releases were not measured. Fixed in 1.5.6: such statements now change no rows and report zero rows; one route, a DELETE on `required_col IS NULL` through the metadata-delete path, still records an empty snapshot (see Known issues) (EMPTY-DML-SHARED-GUARD-1, PR #1277).

Declared divergences from Spark (ruling D, 2026-11-08): on a DELETE or UPDATE whose `WHERE` is NULL, Spark refuses (`FILTER_NOT_BOOLEAN` on DELETE and on UPDATE for format-v2 merge-on-read, `INTERNAL_ERROR` on UPDATE for format-v2 copy-on-write and format v3), on an empty no-op Spark commits an empty snapshot, which RePark does not, and Spark's errors raised from the `SET` expression of a no-op UPDATE are declared the same way (the owner's 2026-11-08 ruling "All recommendations" extends ruling D to them; the cell is `UPDATE t SET prim_col = 1 / 0 WHERE false` on format-v2 merge-on-read, where Spark refuses with `CAST_OVERFLOW_IN_TABLE_INSERT` and RePark writes nothing).

The v2 production label is not part of this release. Its open label items (AUD-022, AUD-023 and CTAS) move to a later release, as ruling A of 2026-11-08 sets, and the format-v3 label moves to a later release (owner ruling 2026-11-02).

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

- **Plain `EXPLAIN` of an empty `DELETE` or `UPDATE` shows the rewritten plan (EMPTY-DML-SHARED-GUARD-1).** `EXPLAIN DELETE FROM t WHERE false` shows a one-row `Values` plan and the source that answers it, where v1.5.5 showed the `Dml` node and the Iceberg delete operator. The plan text is DataFusion's, as on every plain `EXPLAIN`; what runs is the rewritten plan.

- **A table identifier follows Spark 4.1.2's grammar on every statement head and on both doors (IDENT-DIALECT-1, IDENT-DIALECT-1B).** The statements are `DROP`, `TRUNCATE`, `DELETE`, `SELECT`, `INSERT`, `UPDATE`, `ALTER` and `MERGE`, including their end-of-input and second-target spellings.

- **`spark.sql.ansi.doubleQuotedIdentifiers=true` refuses instead of being ignored (DQ-IDENTIFIERS-CONFIG-REFUSE-1, AUD-021).** The runtime setter and the Spark-door session build raise `UnsupportedOperationException` naming the config; `false` is accepted as a no-op, and a non-boolean value answers `INVALID_CONF_VALUE.TYPE_MISMATCH`. v1.5.5 accepted the true value and ignored it.

- **A field access after a subscript, a call or a parenthesis answers on the Spark door (SUBSCRIPT-DOT-ACCESS-1).** `m['k'].f`, `f(x).y`, `(e).y` and `map_keys(mk)[0].a` answer the field's value, where DataFusion refused them with `Dot access not supported for non-string expr`. A leading column path keeps its identifier resolution.

- **An `ALTER` of a field inside a map key refuses as Spark does (MAP-KEY-ALTER-1).** A nested `DROP`, `RENAME` or `ADD` under a map key refuses with `Unsupported table change: Cannot alter map keys: <map>` (an add under a struct key: `Cannot add fields to map keys: <map>`), and the table metadata is unchanged. Moving a key-struct child succeeds and leaves the key unchanged.

- **A temporary view's name never resolves to the same-named table on a write (SPARK-DOOR-TEMP-VIEW-TARGET-1, AUD-006).** A write that names a temporary view refuses as Spark does and leaves the table unchanged, and `writeTo` refuses a view name.

- **On `type=hadoop` tables, metadata discovery follows Iceberg Java's rule (HADOOP-DISCOVERY-RULE-1).** The version hint is read as Java reads it, and a hint that cannot be read falls to the listing. A negative, past or removed hinted version refuses with Java's missing-version text; a zero hint with no `v0` is a missing table; and the listing keeps the highest `v` file whose canonical name is listed, so a catalog-style name never beats a `v` name.

- **The hadoop path read follows Java's gzip metadata names (ICE-HADOOP-GZIP-METADATA-1).** The hint walk and the listing see gzip-named metadata files, as Java's reader does.

- **A rejected CTAS, RTAS or `CREATE TABLE` removes the files it staged (STAGED-PUBLISH-CLEANUP-1).** When the catalog rejects the publish, the staged metadata file, the new snapshot's manifest list and manifests, and the data files the statement wrote are deleted, so a reader no longer takes the rejected change for a commit. An unknown commit outcome keeps every file.

- **RePark reads the top-level columns of an id-less Parquet file on a route with no name mapping as Spark does (RP-65, fork F-IDLESS-TOPLEVEL-POSITIONAL-1 `#381`).** A string column read as binary keeps its bytes, a binary column read as string answers Spark's predicate results on the pinned cells, an overflowing narrowed decimal is loud on read, and a reordered file refuses by position without the mapping. With the mapping the pair reads by name. The fork pin moves to `66901f02`.

- **Lock-only security bumps.** tokio 1.54.1 (RUSTSEC-2026-0386), ctutils 0.4.5 (RUSTSEC-2026-0385), pulldown-cmark 0.13.6 (RUSTSEC-2026-0414 and RUSTSEC-2026-0433), the zstd advisory exception (DEPS-1) and four advisory ignores retired (#1239).

- **A bulk merge-on-read `DELETE` that panicked past one position-delete batch now completes (RP-60, fork F-POSDEL-BATCH-1 `#374`).**
  A `DELETE` on a format-v2 merge-on-read table whose matched rows held more than 2 GiB of data-file path text in one partition
  panicked with `offset overflow`, which surfaced as an internal error; the fork now writes the partition's position deletes in
  bounded batches and the statement completes. The new table property `write.delete.position-delete-batch-rows` sets the row bound:
  default 1,048,576, and a value below one refuses the `DELETE` and names the property.

- **A statement whose change committed no longer fails when the session's name refresh fails afterwards (ICE-POST-PUBLISH-OUTCOME-1, AUD-002).** On the Spark SQL door, `CREATE TABLE … AS SELECT`, `CREATE OR REPLACE TABLE … AS SELECT` and the other DDL and writes return success when the change is committed and only the session's table-name refresh fails; the next statement that names the namespace refreshes it. Only when the session cannot record the name at all does the statement fail, and its error begins `committed:` so the caller knows the change is in the catalog. The native door (`repark.sql()`) is unchanged in this release.

- **Merge-on-read `DELETE`, `UPDATE` and `MERGE` on a format-v2 Avro or ORC table write their position-delete files in the table's delete format, and the next read applies them (ICE-DELETE-FORMAT-GUARD-1, AUD-003; RP-61).** Before this release, a merge-on-read `DELETE` or `UPDATE` on such a table could write Avro or ORC position-delete files that RePark could not read, and the statement reported success while the table became unreadable. Now the delete format is `write.delete.format.default` when set (Avro and ORC honoured) and otherwise the data format, as Spark and Java write them, and the fork reads those files back. The native door (`repark.sql()`) is not covered in this release (NATIVE-DOOR-DELETE-FORMAT-1).

- **RePark reads Avro and ORC position-delete files through the fork's own readers (RP-61, fork F-POSDEL-FORMAT-READ-1 `#375`).** The fork's position-delete and equality-delete loaders open Avro and ORC files with their own readers, and an equality key that the current schema no longer has refuses, naming the file and the field id. The fork pin moves to `56aa8913`.

- **RePark reads a dropped equality-delete key on Parquet data files (RP-62, fork F-EQDEL-DROPPED-KEY-1 `#377`).** An equality delete whose key column a later `ALTER TABLE … DROP COLUMN` removed used to fail the read with `field not found`; on Parquet data files the surviving rows now read back through the Spark door and the native door, including after the column is re-added. Avro and ORC data files still refuse the read with an error that names the data file (F-EQDEL-DROPPED-KEY-2). The fork pin moves to `935cc55c`.

- **RePark reads a struct that a column drop emptied, with no panic (RP-63, fork F-STRUCT-EMPTY-CHILD-PANIC-1 `#378`).** A struct whose every field an `ALTER TABLE … DROP COLUMN` removed used to fail the read with an internal error (AUD-008). Now the Spark door answers it, and the emptied struct's validity is read per row: a non-null row reads a non-null empty struct, and a NULL row reads NULL. Spark's NULL collapse covers every data file that one write produced when that write held a NULL struct row, and Spark reads NULL on those rows; RePark keeps the per-row answer, a recorded divergence by owner ruling of 2026-10-31. Three id-less Parquet file shapes that this bullet left open (F-IDLESS-STRUCT-CHILD-REMAP-1, grade A) are closed by RP-64, the next bullet. The fork pin moves to `14a93602`.

- **RePark reads the struct children of an id-less Parquet file by id, and keeps the name mapping through an ALTER (RP-64, fork F-IDLESS-STRUCT-CHILD-REMAP-1 `#379` and F-NAME-MAPPING-SCHEMA-UPDATE-1 `#380`).** An id-less Parquet file admitted by `add_files` (the output of RePark's own frame writer) used to read a dropped struct child's value under a new child, or refuse the read. Now a child absent from the file reads NULL per row, and a rename, a file with its children in another order and an extra file child read as Spark reads them. A file admitted after a rename, an added column or a drop and re-add by the same name reads its values as Spark does; before the second pin the schema update did not keep the name mapping, so those values read NULL silently. A NULL struct row on a write that holds one stays NULL per row where Spark collapses that write's struct to NULL: the owner's ruling of 2026-10-31, a recorded divergence. Two residues stay open as cards: an incompatible nested type on a file with embedded field ids (F-NESTED-TYPE-MISMATCH-EMBEDDED-IDS-1); and a promotable top-level or nested type that `add_files` refuses with an internal message (ADD-FILES-IDLESS-PROMOTION-1, grade C, P3). The top-level residue (F-IDLESS-TOPLEVEL-POSITIONAL-1) is closed by RP-65 below. The fork pin moves to `ee592dcf`, then to `56ff5545`, then to `66901f02`.

- **A struct-key map subscript answers Spark's value, with no internal error (MAP-STRUCT-KEY-SUBSCRIPT-1, AUD-016, PR #1232).** `m[named_struct(...)]` on a map whose key is a struct used to fail with an internal error (a sliced-buffer panic inside DataFusion's `get_field`). Now it answers Spark's value: the key is matched by position at every depth, each leaf is cast as the Spark door's CAST, the session zone included, floating leaves compare as Spark compares them (`-0.0` equals `0.0`, NaN equals NaN), the array, large-array and fixed-size-array key kinds match alike, and a `NULL` leaf for a required key field matches nothing. The DOUBLE-to-DECIMAL rounding and `F.expr` subscripts in an aggregate or a scalar-key filter are not yet fixed. A key whose field count or leaf pair Spark refuses refuses with Spark's `DATATYPE_MISMATCH` class, and `F.expr` map subscripts in a select answer. With `spark.sql.ansi.enabled=false` a key of another type refuses as Spark does, and struct field names match case-insensitively unless `spark.sql.caseSensitive` is true; an array element of the key keeps RePark's ANSI-on cast under ANSI off, a carded divergence (ANSI-OFF-LEGACY-CAST-1).

- **The hadoop path read answers the newest committed metadata file past a stale
  `version-hint.text` (ICE-HADOOP-HINT-WINDOW-1, PR #1205).** It answered the hinted version, so a
  row a committed `DELETE` had removed could read back; a walk past 10,000 metadata files refuses
  with an error that names the limit.
- **A merge-on-read `DELETE`, `UPDATE`, `MERGE` or identity-IN statement that RePark writes itself now honours the position-delete batch bound (RP-POSDEL-BATCH-2, PR #1203).** The plain `DELETE` with a comparison, `AND` or `OR` predicate (`>= AND <`, `d = DATE ...`), the merge-on-read `MERGE`, and the `IN (subquery)` forms of `DELETE` and `UPDATE` now write their position deletes in bounded batches, as the fork's `DELETE` does: a batch closes at `write.delete.position-delete-batch-rows` (default 1,048,576) or at 256 MiB of path text. A statement past about 2 GiB of data-file path text in one partition completes instead of panicking with `offset overflow`; for the plain range-`WHERE` `UPDATE` below, that panic is inferred, not measured. A value below one now refuses on these routes too; before, the property was ignored there. That includes copy-on-write identity `DELETE` and `UPDATE`. The plain merge-on-read `UPDATE` with a range `WHERE` (a top-level `BinaryOp`) now honours the property and shares the batched writer: measured refused at `0` on the head and succeeding on main; its pre-fix overflow is inferred from that shared writer, not measured. On a merge-on-read `UPDATE` or a `MERGE` with an `UPDATE` or `INSERT` arm, a statement refused at `0` has already written its data files, which stay uncommitted; rows and snapshot are unchanged.
- **`ALTER TABLE` through a temporary view's name refuses, and `RENAME TO` renames the view (SPARK-DOOR-TEMP-VIEW-DDL-1, AUD-015).** With a temporary view in scope under the name of an Iceberg table, an `ALTER TABLE <name>` on the Spark SQL door changed the same-named table (dropping or replacing columns, changing properties, renaming the table); now every spelling through the view's name refuses and leaves the table unchanged, a leading semicolon included (an `IDENTIFIER('t')` target, and a view named `only` referenced unquoted, refuse as parse errors), with Spark 4.1.2's `EXPECT_TABLE_NOT_VIEW` text for Spark's forms and a RePark-only prefix such as `ALTER TABLE ONLY <name>` refused too; `ALTER TABLE <view> RENAME TO <name>` renames the view, and `DROP TABLE <view>` drops the view, as Spark does. The Iceberg extension forms still reach the table when spelled as Spark spells them. Correction, 2026-11-02 (verifier round 3, F10 and F11): the first statement held only for the spellings the verifiers had reached; a digit-leading view name (`1t`) skipped the check until fix round 3 (`c45fc2b2`), and `DROP TABLE "t"` and `TRUNCATE TABLE "t"` through a view's name refuse as Spark does.

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
  Glue and S3 Tables. Pin X shows that 1.5.5 and this release read each other's tables equal on the local
  catalog. Both legs hold live (2026-10-23, run 40025222296): leg U 96 of 96 cells and leg X 40 of 40 on Glue and S3 Tables, v2 and v3. The cross-read module of U-10 remains open (ledger C-060): run 4 timed out in Glue RePark row collection and S3 Tables PyArrow metadata-file I/O. Native diagnostics are the next measurement.

- **An unfiltered `UPDATE` of an out-of-range `TIMESTAMP` literal reports INSERT's overflow (ICE-TSNS-UPDATE-RANGE-CLASS-1, R-012).** Under ANSI the statement refuses with `[CAST_OVERFLOW]` and INSERT's text, as ICE-TSNS-TYPE-PLANNER-1 left it in v1.5.5; this release pins the 16 cells and changes no stored value or refusal.

- **The Spark SQL door's replace statements are pinned for a writer killed at the publish
  (ICE-REPLACE-KILL-DOORS-1, test only).** `CREATE OR REPLACE TABLE … AS SELECT`, with or without a
  session write key, and `REPLACE TABLE … AS SELECT` are pinned on format v2 and v3 on the local
  catalog: the table reads back as replaced, with no orphan. The native door's replace is not yet
  pinned. No stored value changes.

## Known issues

- **Maintenance on an Avro or ORC merge-on-read table leaves delete files that Spark removes (AUD-013, card [F-MAINT-AVRO-ORC-DELETE-RESIDUE-1](f-maint-avro-orc-delete-residue-1-card-2026-10-29.md)).** The residue is a follow-up unit. The Avro and ORC delete write side is fixed in this release (ICE-DELETE-FORMAT-GUARD-1).

- **A facade `DELETE` whose predicate is `id IS NULL` on a required column still commits an empty snapshot (EMPTY-DML-SHARED-GUARD-1 residue).** The metadata-delete door runs before planning, so the guard does not reach it. Rows and data files are unchanged; Spark commits no snapshot for that statement.

- **`UPDATE t SET prim_col = 1 / 0 WHERE false` on a format-v2 merge-on-read table answers zero rows.** Spark 4.1.2 refuses it with `CAST_OVERFLOW_IN_TABLE_INSERT`. This is a declared divergence under ruling D, as the owner extended it on 2026-11-08 (see Data-safety fix).

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

- **Struct values whose fields are declared in a different order compare by name, where Spark compares by position (STRUCT-COMPARE-BY-POSITION-1).** A comparison between a struct column and a struct literal or column whose fields are declared in a different order pairs the fields by name where Spark pairs them by position. A `WHERE` built from such a comparison selects the wrong rows for `SELECT`, `UPDATE`, `DELETE` and `MERGE`. The defect is pre-existing and was measured on 1.5.6. Until the fix ships, write struct literals in the column's declared field order, or compare field by field. The fix is unit `STRUCT-COMPARE-BY-POSITION-1`, target v1.5.7.

- **A `repark.sql(…)` DataFrame write reports success and stores nothing, and a path overwrite deletes the rows already there (NATIVE-DATAFRAME-WRITE-SILENT-1).** On the native door, a frame from `repark.sql(…)` returns without an error and stores nothing when it is written to an existing Iceberg table (`saveAsTable` append, `format("iceberg").save(name)` append or overwrite, `insertInto` in either form, `writeTo(…).append()` or `writeTo(…).overwritePartitions()`), to an in-memory table (`insertInto`), or to a Parquet, CSV or JSON path in any mode. A Parquet, CSV or JSON `mode("overwrite")` onto an existing path also deletes the rows already there and leaves one file with no rows. The defect is pre-existing. It was measured on main `1d9d2808` across 1568 native-door cells, and a sample run on the published 1.5.6 wheel showed the same silent and destructive forms. Until the fix ships, do not write a `repark.sql(…)` frame with the DataFrame writer. For a table, register the frame as a temporary view `v` and run the write as a statement through `repark.sql(…)`, collecting the result. `saveAsTable`, `format("iceberg").save(name)` and `writeTo(…)` pair the frame's columns with the table's by name, so name them: `INSERT INTO <table> (<frame columns>) SELECT <frame columns> FROM v` to append, and `INSERT OVERWRITE <table> (<frame columns>) SELECT <frame columns> FROM v` to replace an Iceberg table's rows. Without the column list the statement pairs columns by position, and a frame whose columns are in another order stores its values in the wrong columns. `insertInto` pairs by position: `INSERT INTO <table> SELECT * FROM v`, or `INSERT OVERWRITE <table> SELECT * FROM v` for its overwrite. An in-memory table has no `INSERT OVERWRITE`, so its overwrite is `CREATE OR REPLACE TABLE <table> AS SELECT <frame column> AS <table column>, … FROM v`, pairing the columns by position; the table takes the query's column types and drops `NOT NULL`, so a NULL row is stored where the Spark door refuses. A native view cannot be written: `insertInto` on one also returns and stores nothing. For a path, convert the frame on a `ReparkSession` `spark` and write the result: `spark.createDataFrame(frame.collect(), frame.schema)`, which holds every row in memory and fails loudly, with nothing written, for a decimal value with 11 or more integer digits at any declared precision, and for ±Infinity in a DOUBLE or FLOAT column. `spark.sql(…)` cannot read a table made in the native session, and `spark.createDataFrame(frame.toPandas())` without the schema widens integer columns, so an append onto the frame's own files refuses, and it fails on an empty frame. Each route was measured on the published 1.5.6 wheel. The fix is unit `NATIVE-DATAFRAME-WRITE-SILENT-1`, target v1.5.7 (owner ruling 2026-11-13): each such write refuses before any effect and names the working route, or says that a view or a database-source table cannot be written.
