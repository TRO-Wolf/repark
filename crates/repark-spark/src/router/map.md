# map — repark-spark/src/router

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

File-backed tests for the statement router (`../router.rs`): the TRUNCATE missing-table
class pin (`TABLE_OR_VIEW_NOT_FOUND`; DML-C), passthrough sanity, the BUG-010 ordering pin,
and the P11 read-only threading pin.
Location translation follows byte equality across owned and borrowed buffers.
The lib-root battery lives in `../tests/`
(`crate::tests`; see [../tests/map.md](../tests/map.md)).
**IPI-26/27 round 2 (2026-09-20):** the directory also holds pre-parse intercept
modules, which live here because `lib.rs` is at its re-export ceiling.

## Contents

- `door_tag.rs` — **WRITE-SPINE-1 step 3a round 2 (2026-10-24, orchestrator ruling on Q2, Q4 and
  Q5):** the runtime door record. `WriteDoorRecord` (a `Mutex<Vec<WriteDoor>>`, a poisoned lock
  is read through) is a `SessionConfig` extension `SparkExtension` installs per session
  (`with_write_door_record`). `clear` runs at the start of `execute_in_session`, the one entry
  of every Spark statement; `record` adds a door once, in the order the statement takes it;
  `enter` also emits the `tracing` tag; `last_write_doors` (re-exported as
  `repark_spark::write_door::last_write_doors`) reads the set. A session without the
  extension records nothing. The branch points: D5 and D3 at the `REPLACE WHERE` and `BY NAME`
  intercepts, D6 at the CTAS arm, and in `execute_insert_routed` D3 on the schema-evolution
  `BY NAME` route, D4 before `execute_insert_overwrite`, D2 on the owned append, on a statement
  with write options and on the session-write-conf append, D1 on the plain route; a Postgres
  target returns before any record. D10 is recorded in `spark_ast.rs` just before
  `execute_predicate_dml` runs an `UPDATE`, and (round 3, ruling Q9) when the planned
  statement is a `Dml` `Update` the fork provider runs (`record_planned_update`, called by
  `spark_ast.rs` after planning; an `UPDATE` with no `WHERE` takes it); D7, D8 and D9 in `../merge/nested_assign.rs`. A
  `CALL` procedure that runs SQL through `router::execute` clears and records as its own
  statement. Pins: `../../tests/write_door_record.rs`. pins: write-spine-1/C-011

- `delete_update.rs` — **ENC-1 round 2 (2026-10-09):** the DELETE/UPDATE doors moved out
  of `../router.rs` (pure move; it sat at its 1000-line ceiling) so the encrypted-write
  refusal fits: each door runs the house valves first, then refuses a target carrying
  `encryption.key-id`, before the meta-delete door, predicate DML or provider delegation.
  ObjectName-only target extraction (aliases would under-refuse BUG-001).
  **C-4 merge (2026-10-10):** `execute_update` also carries the Postgres upsert check
  from the deleted `update.rs` after the P11 refusal, so an unpopulated read-only set
  names the `CONNECT-DECL-pg-write-upsert` row.
  pins: enc-1/C-005, c-4/C-013
- `insert_positional.rs` — **U8 WRITE-SQL PR1 (2026-09-24):** `prepare_positional_insert`
  runs after U6's by-name routing check in `execute_insert_routed`. It aliases a later
  projection item whose DataFusion name repeats an earlier one (`__repark_col_<n>`; a cast
  keeps its input's name) in every top-level `SELECT` of the source, because a positional
  write never reads the names, then hands the statement to the PARTITION rewrite.
  `PreparedInsert.owned_append` tells the router to commit through the owned append. Round 3
  (2026-09-25): it hands the raw source to the PARTITION rewrite for arity naming. Round 4
  (2026-09-25, critic r3 V-003): beside a `*` it also aliases every column-named item (a
  column, a cast of one, or an alias), because the expansion's names are unknown before
  planning; a `SELECT *, CAST(id AS STRING)` no longer leaks DataFusion's `Projections require
  unique expression names`. Its two children are in [insert_positional/](insert_positional/map.md).
  pins: u8-write-sql/C-011, C-023
  **C-4 fold 3 item 3 (2026-10-11):** `alias_source_positionally`, beside the shared
  rewrite and used only by the Postgres route: it aliases every UNNAMED projection
  item by position (two different expressions can share one display name, which no
  expression-text key can see), keeping the collision rule for named items. The shared
  Iceberg path is untouched.
  **WO STORE-TS-TO-NUMERIC-1 (2026-09-28):** after that rewrite (`rewrite_positional_insert`)
  it runs `void_type::refuse_insert_source_types` on the prepared statement, so every
  positional INSERT door — append, owned append, OVERWRITE and VALUES — refuses a `-NULL`
  into a DATE, BOOLEAN or timestamp column and a STRING into FLOAT/DOUBLE with Spark's text.
  `router.rs` stays at its 1000-line ceiling. pins: store-ts-to-numeric-1/C-002, C-003
  **Fold 2026-09-29 (re-verify RT-1..RT-3, narrowing):** the gate judges `-NULL` only; STRING sources store
  as on base. pins: store-ts-to-numeric-1/C-006

- `tests.rs` — `#[cfg(test)] mod tests;` in `../router.rs`.
  **MW-6:** the CALL dispatch covers all supported procedures, including `register_table`.
- `comment_on_table.rs` — **IPI-26/27 round 2 (2026-09-20):** the
  `COMMENT ON TABLE t IS 'lit'` / `IS NULL` intercept. The parser is a
  `DatabricksDialect` reader for `COMMENT ON TABLE <3-part> IS <literal|NULL>`;
  `COMMENT ON COLUMN` is not the shape and passes through. Execute sets or
  unsets the `comment` property with no reregister, like `SET TBLPROPERTIES`
  (cell `D-COMMENT-ON`). Unit pins are inline; door pins are
  [test_ice_ddl_clauses_1.py](../../../../python/repark/tests/iceberg/test_ice_ddl_clauses_1.py).
- `hive_change_column.rs` — **IPI-26/27 round 2 (2026-09-20); rename refusal
  (2026-09-21):** the two-name Hive `ALTER TABLE t CHANGE [COLUMN] old new TYPE
  [COMMENT 'lit']` intercept. The type parses with `SparkSqlDialect` and maps at
  execute time, so non-primitive targets refuse with the session timestamp type
  applied. Parse refuses the rename form (`old` != `new`) right after the new
  name with `[PARSE_SYNTAX_ERROR]` / SQLSTATE 42601, as Spark does; execute then
  applies only `UpdateColumnType` plus an optional `UpdateColumnDoc` against the
  old name (cell `D-X-CHANGE-COLUMN-TYPE`). Unit pins are inline; door pins are
  [test_ice_change_column_rename_1.py](../../../../python/repark/tests/iceberg/test_ice_change_column_rename_1.py)
  and
  [test_ice_ddl_clauses_1.py](../../../../python/repark/tests/iceberg/test_ice_ddl_clauses_1.py)
  beside the `COMMENT ON` pins.
- `table_props_ddl.rs` — **IPI-26/27 round 4 (2026-09-21, cell `D-SET-LOCATION`):**
  the `ALTER TABLE <cat.ns.tbl> SET LOCATION '<literal>'` intercept (the statement does
  not survive sqlparser). The parser is a `DatabricksDialect` reader that claims only
  after `SET LOCATION` — keywords case-insensitive, the quoted path verbatim — so
  `SET/UNSET TBLPROPERTIES`, branch/tag shapes and every other ALTER form pass through;
  a claimed-but-malformed tail (non-three-part name, missing or non-string path,
  trailing tokens) refuses loud naming the clause. Execute moves the metadata location
  through `repark_iceberg::write::set_location::set_table_location` behind the
  `parsed_ddl("ALTER TABLE")` write-options gate, then defensively invalidates the
  touched namespace as the schema-changing intercepts do — sibling convention, not a
  tested leg: the move changes no names, so no pin can observe it. Unit pins are
  inline; door pins are
  [../tests/ice_ddl_clauses_1.rs](../tests/ice_ddl_clauses_1.rs) and
  [test_ice_ddl_clauses_1.py](../../../../python/repark/tests/iceberg/test_ice_ddl_clauses_1.py)
  (first linked from the `comment_on_table.rs` row above).
- `pg_insert.rs` — **C-4 step 2 (2026-10-09):** Postgres-target INSERT routing on this door
  (a router child because `../router.rs` is at its line ceiling and `../lib.rs` at its
  re-export ceiling). Appends drive the shared `repark_core::write_postgres` sink; overwrite
  refuses with the `CONNECT-DECL-pg-write-modes` row, REPLACE and UPDATE with the
  `CONNECT-DECL-pg-write-upsert` row. The `write.path` writer option is read off
  `StatementWriteOptions::raw` and parsed by the shared core parser (`bulk` default, `row`
  forces the INSERT path, anything else refuses as `Configuration`/IllegalArgumentException).
  The write future is heap-boxed at the module boundary (clippy large-futures). Each write
  records its report on the session carrier. Door pins are
  [../tests/pg_insert.rs](../tests/pg_insert.rs).
  **C-4 merge (2026-10-10):** the route runs before the ENC-1 guard in
  `execute_insert_routed`; the guard still sees every Iceberg target.
  **C-4 fold 3 item 3 (2026-10-11):** the route calls `alias_source_positionally`,
  so `SELECT 1, '1'` and its siblings plan instead of colliding on one display
  name. pins: c-4/C-013
  **C-4 fold 2 item 5 (2026-10-10):** the source passes through the Iceberg route's
  `deduplicate_source_names` before planning, so a positional INSERT never depends on
  the SELECT's output names.
  pins: c-4/C-013, C-014
- `nested_ns.rs` — **ICE-TSNS-MERGE-WALL-1, the split (2026-10-09):**
  `refuse_nested_supply` is the one call that hands an `INSERT` (plain, `OVERWRITE`,
  `BY NAME`, `REPLACE WHERE`), `UPDATE` or `MERGE` statement and its Iceberg target to
  `repark_iceberg::write::nested_ns_gate::refuse_nested_ns_supply`. It costs one catalog load
  of the target. `insert_positional/replace_where.rs` gained the two accessors it reads.
  **Where the call sits (split fold 1, 2026-10-10):** in `router::execute_calibrated`, on the
  statement as the caller wrote it, before the metadata-table, changes, write-to-branch, WAP
  and time-travel rewrites. The first split called it at the top of `execute_inner`, which
  runs after `write_to_branch::apply_write_to_branch`; that function points a branch or WAP
  append at a temporary provider (`datafusion.public.<name>`), so the gate saw a target that
  was not an Iceberg table and ten routes stored the UTC wall. **Why no routed write can go
  around it:** every public entry of this module ends in `execute_in_session`, which either
  answers a temporary-view statement or calls `execute_calibrated`; `execute_inner` has two
  callers, the `CREATE VIEW` arm of `execute_calibrated` and `execute_time_travelled`, which
  only `execute_calibrated` calls after the gate. What re-enters with a statement of its own
  (the maintenance and partitioning `CALL`s) re-enters by the public `execute`, so it passes
  the gate again; a DataFrame writer enters by the public entry with the real name.
  **The target:** the written name, and also the name with a `branch_<x>` reference removed
  (`split_write_ref_parts`, the function the branch rewrite uses), so `t.branch_x` and
  `t.branch_main` decide on `t`; a `tag_<x>` reference is left to the door's own refusal.
  A session WAP setting does not change the written name.
  **A statement that carries a write (split fold 2, 2026-10-10):** the parsed statement is
  not matched against a list of write kinds with an arm that returns Ok for the rest; that
  arm let `EXPLAIN ANALYZE INSERT`, `PREPARE … AS INSERT` (run by `EXECUTE`) and
  `CREATE TABLE … AS INSERT …` execute a write past the gate. `executed_writes` answers every
  write the statement runs: an `EXPLAIN` is looked into when its analyze flag is set and
  answers nothing when it is not (a plain `EXPLAIN` executes nothing); a `PREPARE` is looked
  into; for any other statement the parser's `visit_statements` hands over each statement
  nested anywhere in it (a query body, a CTE, a subquery, a `CREATE TABLE AS` source, a
  procedural block), and each INSERT, UPDATE and MERGE found is decided as if written alone.
  `EXECUTE` carries a name only; its statement was decided at `PREPARE`.
  **A statement it cannot parse:** the function applies the two rewrites `execute_inner`
  applies before parsing (map casts and system functions, `WITH SCHEMA EVOLUTION`). If the
  statement still does not parse: one `EXPLAIN` layer is peeled by its tokens
  (`explained_statement`; the parser rejects a nested EXPLAIN, and the engine runs a doubled
  `EXPLAIN ANALYZE`), a peeled layer without ANALYZE answers Ok, one with it is decided on
  what remains; otherwise every `INSERT`, `UPDATE` or `MERGE` keyword in the statement gives
  a written target (`write_to_branch::written_targets`), and a table with such a leaf
  refuses (`NestedWrite::Unreadable`). The cost: a malformed write aimed at such a table
  reports the nested refusal, not the parser's error.
  A target that is not an Iceberg table passes through to the door, which answers as before.
  pins: ice-tsns-merge-wall-1/C-040, C-050, C-053
  **REPARK-SQL-UPDATE-SUBQUERY-WHERE-1, owner ruling 135 (a) (2026-10-17):** the walk over the
  parsed statement, `executed_writes`, moved unchanged to
  `repark_iceberg::write::statement_writes`, which `repark.sql()` also calls; this file calls it
  from there. The moved walk also answers a `DELETE`, which this file's match ignores, and the
  statement itself still comes first, so the `BY NAME` index and every answer stay as they were.
  pins: repark-sql-update-subquery-where-1/C-021
  **ICE-NTZ-NESTED-REFUSE-1 (2026-10-20, parity row R-011, ruling T-3):** after R-015's call on
  the same loaded table, `refuse_zoned_micros` refuses a value for a column with a nested
  microsecond `TIMESTAMP_NTZ` leaf when the session zone is not UTC (canonical id `UTC`,
  `+00:00` or `-00:00`; `Etc/UTC` refuses; *owner ruling 3, 2026-10-21, ledger C-042:* the one
  set `UTC_ALIASES` and `names_utc` in this file hold every pure UTC alias, `UTC`, `UCT`,
  `Universal`, `Zulu`, `Z`, `GMT`, `GMT0`, `GMT+0`, `GMT-0`, `Greenwich`, their `Etc/` forms and
  the zero offsets, matched as written and as the session canonicalises them, and no
  geographic zone: `Africa/Abidjan`, `Atlantic/Reykjavik` and `Europe/London` still refuse,
  their tzdb history holding a local-mean-time or summer offset; the unit test
  `only_a_pure_utc_alias_counts_as_utc` holds both lists, each written out in the test so a
  name dropped from the set reds it, ledger C-043) or the statement is a `PREPARE` (`EXECUTE` can run it
  after a `SET` of the zone). For each supplied column it reads the type of each feed by
  planning one `SELECT` through `router::execute_calibrated`, never executed: an INSERT's source
  (with the outer `WITH` of `WITH … INSERT`), the column at the source position, of the column's
  name, or every column when neither resolves; `SELECT <values> FROM <table> [AS <alias>]` for
  UPDATE and `… FROM <table> [AS <alias>], <source>` for MERGE, the table being the loaded
  table's name, so `t.branch_x` reads as `t`; `SELECT * FROM <source>` for a MERGE star. It
  refuses when a type holds a zoned timestamp, when the plan fails (fail closed), and, on a plain
  `UPDATE` whose path stops above the leaf, when any planned sub-expression calls the
  `TIMESTAMP_NTZ` cast (`shifts_in_an_update`). GATE-RETIRE-1 (2026-10-25, PR #1187) retired the arm
  that refused a `CAST` or `TRY_CAST` from a zoned type to a naive one: the zone seam had made it
  unreachable. *(Fix round 2, 2026-10-20: that door no longer refuses a zoned
  sub-expression as such. `to_timestamp_ntz(<instant>)`, a zoned column feeding only an `INT`
  field or a condition, store Spark's value there; the cast arm and the planned type cover every
  wrong wall measured on that door; ledger C-022.)* **Where the walk reads (fix round 2,
  2026-10-20, ledger C-023):** both tests run over the lineage of the supplied column only
  (`lineage_has`), not the whole statement: from the output column of the typing plan it reads
  the expression that produces it and follows each column it references through projections,
  aliases, derived tables, filters, sorts, limits, `DISTINCT`, joins (the side the column comes
  from), unions (every branch), windows, values rows and a `ReplanningTempView`'s definition
  plan (a DataFusion or catalog view is inlined by the plan builder before the walk). A
  predicate, a join condition and a sibling column are not read. *(Owner ruling 1, 2026-10-21,
  ledger C-041: an `IN`, `EXISTS` or quantified subquery inside a read expression is not read,
  its left operand still is; a scalar subquery is walked through its one output column as a
  scope entry (`Next::Lateral` with the node's input as the scope), so an outer reference in
  its output is followed back out and one in its `WHERE` is not. Round 2 read every subquery
  expression whole and followed its outer references as columns.)* Read whole, fail closed:
  every group and aggregate expression of an aggregate (owner ruling 2, 2026-10-21: kept), and
  any other node (`plan_has`, the round-1 walk). A column that resolves in no input reads every input whole.
  **A `LATERAL` subquery (fix round 3, 2026-10-21, ledger C-033):** DataFusion 54 plans a
  lateral factor as a join whose right side is a `Subquery` node, and the lateral's projection
  names the left side's column as an outer reference (`Expr::OuterReferenceColumn`). Each entry
  of the walk carries the scopes it sits in: entering a join's right side pushes the join's left
  input as a scope, and an outer reference is followed into the innermost scope that resolves
  it, as a column, so `LATERAL (SELECT a.c AS s)` reads the expression that produces `a.c` and
  a sibling column of `a` is not read. Fail closed: an outer reference that resolves in no scope
  refuses, and a node read whole inside a scope that holds an outer reference reads every
  enclosing scope whole as well. Round 2 followed the column into the subquery and dropped the
  reference, so a container cast of an instant through `LATERAL (SELECT a.c AS s)` stored the
  UTC wall (verifier round 2, S1-1).
  Neither walk descends a scan source's own logical plan (round 1's frame-view branch, removed
  in fix round 2, ledger C-024): DataFusion 54's `LogicalPlanBuilder::scan` replaces a scan
  whose source carries a plan (a catalog view's `ViewTable`, a frame's `into_view()`) with that
  plan before the router sees it, and every planner path builds scans through it. The file's
  `#[cfg(test)]` module pins the fail-closed read of a column that resolves in no input on a
  synthetic plan (no planner builds one), and, from fix round 3, the two fail-closed arms of
  the lateral walk on synthetic plans: an outer reference no enclosing join resolves, and an
  unmodelled node (`DISTINCT ON`) inside the right side of a join that holds an outer reference
  (no measured lateral that RePark can execute reaches either: a scalar subquery of the outer
  column inside the lateral reaches the whole-read arm, and it plans, then fails in the
  physical planner on main and the build; a derived table, `DISTINCT` or `UNION ALL` inside the
  lateral plans and fails in `repark_lateral_projection_hoist`, and the walk follows its outer
  reference). Fix round 3 also pins the walk's reach on SQL (ledger C-034): a top-level
  `ORDER BY`, a repartition and a `DISTINCT ON` in a frame, and the nearest scope when lateral
  aliases shadow.
  A `PREPARE` that holds a parameter is read as `Scope::Unreadable`, so every supply of the
  column refuses. **Why the container cast:** the planner's cast of a whole
  struct, array or map converts a zoned leaf with UTC even in a bare `SELECT`
  (`CAST(array(<instant>) AS ARRAY<TIMESTAMP_NTZ>)[0]` is `1767323045123456` in New York; Spark
  `1767305045123456`), and its planned type is naive; the leaf `CAST(<value> AS TIMESTAMP_NTZ)`
  is lowered to the session-aware cast function, never to a planner cast, so it still passes.
  **Why the plain `UPDATE` rule (corrected 2026-10-20, fix round 1):** that door adds the session
  offset to every `TIMESTAMP_NTZ` cast inside an assigned struct or array, whatever the input:
  `CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP_NTZ)` stores `1767341045123456` in New York
  (Spark `1767323045123456`), a date cast stores midnight plus the offset, and the cast of an
  instant lands on its UTC wall; a literal, `to_timestamp_ntz`, a column, the field assignment
  and MERGE store the wall. The round-0 text said the door "converts with UTC"; the measured
  mechanism is the offset. **Why a parameter refuses:** `EXECUTE` binds the argument after the
  decision and casts an instant to the declared type through its UTC wall (`STRING`,
  `TIMESTAMP_NTZ`), so no declared type proves a wall. **Why plan and not read the AST:** a
  column, a view, a by-name frame and a scalar subquery have no type in the AST; the router's own
  planner gives the type the write sees.
  **Fix round 4 (2026-10-21, ledger C-047):** the `///` lines the unit had put on this file's
  four unit tests (`a_column_that_resolves_in_no_input_reads_every_input_whole`,
  `an_outer_reference_no_enclosing_join_resolves_fails_closed`,
  `an_unmodelled_node_that_refers_out_reads_the_other_side_whole`,
  `only_a_pure_utc_alias_counts_as_utc`) are removed; each test's reason and its `pins:` citation
  are in the paragraphs above (the fail-closed read of an unresolved column, the two fail-closed
  arms of the lateral walk, the alias set).
  pins: ice-ntz-nested-refuse-1/C-001, C-002, C-004, C-005, C-015, C-016, C-017, C-018, C-022, C-023, C-024, C-033, C-042, C-047
  **ZONE-SEAM-1 step 3c (2026-10-22, ruling ZS-9, an over-refusal by design until
  WRITE-NTZ-TO-LTZ-NESTED-1):** outside UTC (the same zone flag as R-011, `PREPARE` included),
  `refuse_naive_into_instant` refuses a write whose value carries a nested `TIMESTAMP_NTZ` leaf
  into a nested `TIMESTAMP` leaf of the target column (`naive_into_instant`, struct fields by
  name then position, list elements, map entries), on the doors whose store conversion reads the
  wall as UTC: an `INSERT OVERWRITE` or `INSERT … BY NAME` (the sink's `conform` cast; the
  DataFrame `insertInto(overwrite=True)` and `overwritePartitions` reach it as `INSERT
  OVERWRITE`), a MERGE `INSERT`, and a MERGE or plain `UPDATE` assignment whose path stops above
  the leaf (the `arrow_cast` text of `update_cast.rs`). A positional `INSERT`, `REPLACE WHERE`,
  the appending DataFrame writers and an assignment to the leaf itself read the wall in the
  session zone and store as before; a plain `UPDATE` value whose lineage holds a
  `TIMESTAMP_NTZ` cast (`shifts_in_an_update`) stores as before too. The value's type is planned
  as R-011 plans it (`column_type` for an `INSERT` source or a MERGE `*`, one projection for
  assignments); a feed the router cannot type is not refused. pins: zone-seam-1/C-015
  **ZONE-SEAM-1 fix round 1 (2026-10-22, verifier round 1, S1-1):** `refuse_insert` hands the
  INSERT's `PARTITION` clause itself to the gate (`InsertSupply.partition`), not a flag, so the
  gate resolves the static columns against the loaded table and names the source position; the
  type read then picks that column on `INSERT OVERWRITE … PARTITION (…)` in every spelling (a
  `SELECT`, `VALUES`, a column list, the `TABLE` keyword, a dynamic `PARTITION (k)`), and the
  refusal fires there as on the unpartitioned doors. `REPLACE WHERE` passes no clause.
  pins: zone-seam-1/C-016
  *Fix round 1, S3-1:* on the U-8 matrix the refusal holds 134 `ltz_nested` cells per zone (New
  York, Kolkata), on main 37 Spark-equal, 79 DIFFERENT and 18 engine errors, as R-011's line now
  counts them. pins: zone-seam-1/C-019
  **ZONE-SEAM-1 fix round 3 (2026-10-23, item A, found by the calibration verifier):** the plain
  `UPDATE` exemption from the ZS-9 refusal was read from the whole value's lineage (any
  `TIMESTAMP_NTZ` cast anywhere), while the plain-UPDATE door shifts only that cast's own leaf; a
  second naive leaf beside it (an extract of a container cast, a `TIMESTAMP_NTZ` literal) or a
  cast that sat only inside a non-timestamp field's expression let the struct store the session
  wall read as UTC, unrefused. `only_shifted_walls` now reads the planned value leaf by leaf
  (`shifted_leaves`): through an alias, the non-null marker, `named_struct` values and `struct`
  arguments, every sub-value whose type holds a naive timestamp must be the `TIMESTAMP_NTZ` cast
  call itself; anything else holding one (an extract, a literal, a column, a `CASE`, an array)
  makes the statement refuse, as the MERGE twin already does. A struct whose only naive leaf is
  the cast still stores Spark's instant; UTC is untouched. pins: zone-seam-1/C-024
  The file's four unit tests moved to [nested_ns/tests.rs](nested_ns/map.md) to keep
  the file under the 1000-line ceiling. pins: zone-seam-1/C-024
  **GATE-RETIRE-1 (2026-10-25, PR #1187):** the container-cast arm is removed from `proven_naive`
  and from `naive_columns`, with its two `lineage_has` uses; the other arms are unchanged. A
  whole-struct `CAST` of an instant outside UTC stores the session wall (pinned by
  `ntz_nested_refuse`), and the census replay on the head moves no cell (0 of 12192).
  pins: gate-retire-1/C-001
  After both fix-round-3 items the corpus, the U-8 matrix and the partitioned census answer as
  before them and the seven timed statements stay within 1.05x of main. pins: zone-seam-1/C-025
  *Fix round 1, cost:* the refusal's extra plan of a MERGE into a nested `TIMESTAMP` target in New
  York times at 0.922x main, the six U-8 statements within 1.05x. pins: zone-seam-1/C-020
  *Fix round 2 (2026-10-23), the records:* R-011 counts the partitioned over-refusal of
  Spark-equal-on-main cells beside the matrix's 37 (21, 68 of 354, 24 of 231 per zone, one per
  named census, the ZS-9 class until GATE-RETIRE-1); R-015's line states the measured change on a
  nanosecond leaf (13 bare-`NULL` cells per zone) and that it awaits the owner's ruling. The
  partition-column-first pins are in `tests/ntz_into_ltz_nested.rs`. pins: zone-seam-1/C-022
  *Fold 4 (2026-10-23, verifier round 3, owner ruling "count it"):* R-011 lists the plain-`UPDATE`
  shapes Spark-equal on main that refuse on the head (the Muse twelve and the Opus ten per zone,
  four in both, and the two calibration cells) and the London-winter `ltz_nested` split (134
  refused, 116 Spark-equal on main, 18 engine errors) as ZS-9 over-refusal until GATE-RETIRE-1.
  pins: zone-seam-1/C-026, C-027
  *Fold 4 item 3 (2026-10-23, verifier round 3 S2-1):* the offset-text `UPDATE` wall is carded as
  NTZ-CAST-OFFSET-TEXT-UPDATE-1 (a `TIMESTAMP_NTZ` cast of `+05:00` text stores `1767305045123456`
  and of `Z` text `1767323045123456` on main, `05e6b32a` and the head, in UTC too; `INSERT` and
  `SELECT` agree with Spark; not a seam regression; no fix proposed), and C-024's cast-leaf claim
  holds for text without an offset only. pins: zone-seam-1/C-029

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A routing regression on an intercepted form | The lib-root battery (`cargo test -p repark-spark tests::`) pins every arm end to end |
| A `COLLATE` spelling on CREATE TABLE was a generic column-option refuse | G15: `refuse_collation_in_statement` runs on the router parse before the CREATE arm |
| `CAST(x AS STRING COLLATE name)` was a generic parse error | G15 type-position scan on the executing-parse text in `spark_ast` |
| `RESET spark.sql.collation.*` skipped the valve | G15 `DfStatement::Reset` arm in `spark_ast` |

First checks: `cargo test -p repark-spark router::`. Escalate to: [../map.md#debug](../map.md).
