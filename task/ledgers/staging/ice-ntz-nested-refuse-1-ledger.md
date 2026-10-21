# Unit ledger — ICE-NTZ-NESTED-REFUSE-1 · a nested microsecond TIMESTAMP_NTZ store refuses an instant outside UTC

**Date:** 2026-10-20 · **Branch:** `feat/ice-ntz-nested-refuse-1` · **Base:** `c516c54a` ·
**Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: the next release (v1.5.6 notes draft).

**Order:** U-8 of the 2026-10-20 Iceberg re-prioritisation, ruling T-3 (adopted 2026-10-14),
parity row [ICE-TSNS-SQL-1-R-011](../../../docs/spark-sql-iceberg-parity.md). The order of
record and the step-0 sketch:
[../../wo/ice-ntz-nested-refuse-1/order.md](../../wo/ice-ntz-nested-refuse-1/order.md),
[../../wo/ice-ntz-nested-refuse-1/sketch.md](../../wo/ice-ntz-nested-refuse-1/sketch.md).
**This ledger closes when the unit's pull request merges.**

**Fix round 1 (2026-10-20).** The scoped verifier returned FAIL on PR #1124 at `8f56bcf5`: two
S1 findings (a container `CAST` of an instant and a parameterised `PREPARE` stored the UTC wall
outside UTC), one S2 (the plain `UPDATE` door's mechanism was misnamed and shifts a string-built
wall) and four S3 notes. Each became a clause below: S1-1 C-015, S1-2 C-016, S2-1 C-005 and
C-017, S3-1 C-006, S3-2 C-018, S3-3 C-010. The round's cells are C-019, its mutants C-020, its
gates C-021.

**Fix round 2 (2026-10-20).** The verifier's round-1 verdict on `c20c3e25` was FAIL: S1-1 (a
cast through `STRING`, `DATE` or `BIGINT`, or `from_json(to_json(…))`, stores the UTC wall),
S1-2 (the plain `UPDATE`'s zoned-sub-expression arm refuses four EQUAL cells), S1-3 (the cast
walk reads the whole statement, not the column's value), S1-4 (no pin holds the subquery
descent), and S2-1 to S2-4. Each became a dated clause from C-022 on; earlier rows stand as
written and the later row supersedes where they differ. S1-1 and S2-1 are one mechanism that
is not this unit's (carded CONTAINER-CAST-ZONE-1, owner's ruling pending) and are stated as a
residue, not gated. The round's cells are C-030, its mutants C-031, its gates C-032. C-014 stays
**OPEN**: the verifier's round 1 (`c20c3e25`) returned FAIL, answered here; round 2 is pending.

**Fix round 3 (2026-10-21).** The verifier's round-2 verdict on `75475b71` was FAIL: S1-1 (a
container cast of an instant that reaches the column through `LATERAL (SELECT a.c …)` stores
the UTC wall; 17 cells), S1-2 (18 correct cells the lineage walk refuses are not listed with a
count), S1-3 (four `lineage_has` arms no pin holds: `DISTINCT`, a limit, a window's input
columns, a values row past the first), S2-1 (the zero-offset-zone cost is 126 matrix cells per
zone, not 3) and S3-1 to S3-4. Main `d6ee0411` is merged in first (merge `4bb1f82f`). Each
finding became a dated clause from C-033 on; earlier rows stand as written. No rule changes:
the walk is not narrowed and the zone rule is not changed; the over-refusal list is the
owner's to rule on.

**Owner rulings folded into fix round 3 (2026-10-21, about 09:30 UTC).** Ruling 1, yes: an
`IN`, `EXISTS` or quantified subquery in the column's expression is not read, and a scalar
subquery is read through its one output column (C-041). Ruling 2, no: aggregates, conditions and
sibling fields stay read whole; the 14 cells that remain are accepted as cost until
GATE-RETIRE-1. Ruling 3, yes for the pure UTC aliases only: a zone id with no transition and no
local-mean-time offset in its tzdb history counts as UTC; `Africa/Abidjan`, `Atlantic/Reykjavik`
and `Europe/London` stay refused (C-042).

**Why.** In a session whose zone is not UTC, an instant written into a nested microsecond
`TIMESTAMP_NTZ` leaf stored the session wall through field assignment and the UTC wall through
every other door, so one table held two walls for one input. Two folds that stored the right
wall are parked (#1037, #1038); the lift is a 1.5.7 unit. Ruling T-3: refuse with a hint now,
as R-015 does for a nested `timestamp_ns` leaf.

## PROPOSITION LEDGER — ICE-NTZ-NESTED-REFUSE-1 — 2026-10-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | One walk, one supply model and one refusal serve both leaf units: `NaiveLeaf::{Nanos, Micros}` parameterises the walk and the text; the supply functions answer the supply and the feeds; R-015's decisions and text are unchanged. The zone condition and the type read sit in the router: UTC is the canonical id `UTC`, `+00:00` or `-00:00`; a `PREPARE` is decided as if the zone were not UTC, and one that holds a parameter refuses every supply of the column (C-016); an `INSERT` source is typed with its outer `WITH`; a value the router cannot plan refuses. | The gate's unit tests; R-015's three pin files green unchanged but for one dated row (C-008); the zone and type-read pins. | **PROVEN** | §1. `nested_ns_gate.rs` (10 unit tests), `timestamp_ns_nested_shapes.rs`, `nested_ns_routes.rs`, `only_a_utc_zone_lets_an_instant_through`, `a_prepared_write_of_an_instant_refuses_in_a_utc_session_too`, `the_type_read_follows_a_cte_skips_a_zoned_sibling_and_fails_closed`. |
| C-002 | Outside UTC every door refuses a value whose type holds a zoned timestamp for a column with a nested microsecond `TIMESTAMP_NTZ` leaf, before any file is written, field assignment included. | Five shapes × eighteen doors × two zones at the Rust door, no file, no snapshot; the five field doors; the six doors of the old pin in three zones; a frame written by name. | **PROVEN** | §2. `every_door_refuses_an_instant_outside_utc_and_writes_nothing`, `a_field_assignment_of_an_instant_refuses_too`, `a_nested_microsecond_ntz_field_refuses_an_instant_outside_utc`, `a_frame_written_by_name_is_decided_by_the_column_of_that_name`. |
| C-003 | A UTC session refuses nothing and stores what main stored. | The old pin's six doors in UTC; the zone spellings. | **PROVEN** | §2. The UTC walls equal main's (`1767323045123456` on all six doors). |
| C-004 | A naive value still stores outside UTC: a wall literal or column stores itself, and `CAST(<instant> AS TIMESTAMP_NTZ)` stores the session wall Spark stores, on every door but the one of C-005. Branch and WAP routes type their value. | The wall and cast cells at the Rust door; the branch and WAP routes with a wall; the matrix (C-011). | **PROVEN** | §2, §3. `a_wall_or_the_hints_cast_stores_outside_utc_and_the_plain_update_refuses_the_cast`, `a_branch_or_wap_write_of_a_wall_passes_the_gate`. |
| C-005 | Correction 1 of the sketch: the plain `UPDATE` that assigns a struct above the leaf stores the UTC wall for `CAST(<instant> AS TIMESTAMP_NTZ)` on main, so there a value with a zoned sub-expression refuses; the field assignment of the same cast stores the session wall. *(Mechanism corrected 2026-10-20, fix round 1, S2-1: the door does not convert with UTC; it adds the session offset to every `TIMESTAMP_NTZ` cast in the assigned value, so the cast of an instant lands on its UTC wall and the cast of a string on the wall plus the offset. C-017 extends the refusal.)* | The cast cells on that door, literal and column, both modes, two zones. | **PROVEN** | §3: 8 matrix cells and 8 raw errors move to the refusal; the field assignment pin stores `1767305045123456` in New York. |
| C-006 | One text: R-015's class, first sentence and SQLSTATE, naming the table, the column and the leaf path, with one hint that names no dropping, truncating, overwriting, recreating, deleting or replacing action (ruling 76(a)). *(Re-pinned 2026-10-20, fix round 1, S1-1 and S3-1: the hint names the leaf cast, says a cast of the whole struct, array or map is refused, and names no source type, since an unplannable statement takes the same text.)* | The whole-text pin. | **PROVEN** | §4. `the_refusal_names_the_table_the_column_and_the_leaf`. |
| C-007 | The carriers and routes R-015 lists are decided as the write they run (`EXPLAIN ANALYZE`, `PREPARE`, `CREATE TABLE … AS INSERT`, `WITH … INSERT`, an unparsed statement, a branch, `branch_main`, WAP id and branch); a plain `EXPLAIN` runs; a statement that does not supply the column runs and keeps the stored wall; maintenance calls run. | The internal route pins; the unsupplied-statement pin. | **PROVEN** | §2. `a_branch_or_wap_write_of_an_instant_refuses_and_writes_nothing`, `a_statement_that_wraps_a_write_of_an_instant_is_decided_as_that_write`, `a_statement_that_does_not_supply_the_column_runs_and_carries_its_rows`. |
| C-008 | Controls do not move: a nested `TIMESTAMP` or `timestamptz_ns` leaf, a top-level `TIMESTAMP_NTZ` column, and a nested `timestamp_ns` leaf (R-015's text first). R-015's control row for a nested `TIMESTAMP_NTZ` leaf is re-pinned to R-011's refusal with a dated note. | The control pin; the matrix's 1320 control cells. | **PROVEN** | §5. `a_zoned_leaf_and_a_top_level_ntz_column_are_not_guarded`, `a_nested_zoned_or_microsecond_leaf_is_not_guarded` (re-pinned). |
| C-009 | Every DataFrame writer (append, `overwrite(condition)`, `overwritePartitions`, `insertInto` both modes, `saveAsTable` append, a cached frame, a temporary view) refuses an Arrow frame whose nested leaf is zoned (struct, dictionary child, run-end-encoded child, list, map value) outside UTC and writes no file; a naive leaf stores its wall; a UTC session stores the UTC wall. | The facade module. | **PROVEN** | §6: 56 facade tests. |
| C-010 | Spark 4.1.2 answers 174 recorded cells on twelve of #1037's doors (12 doors × 5 sources × 3 zones, less the two column sources on `insert_values`), recorded and re-derived live: the session wall of the instant in each of the 138 instant and cast cells, the literal's own wall (`1767323045123456`) in the 36 `TIMESTAMP_NTZ` literal cells *(wording made precise 2026-10-20, fix round 1, S3-3)*; RePark refuses each instant cell outside UTC and answers Spark's value in every other cell but C-005's. | The recorded fixture, the always-run comparison, the live re-derivation alone and co-collected after `tests/parity`. | **PROVEN** | §6. `ice_ntz_nested_refuse_1_spark.json`; `test_repark_refuses_where_spark_stores_an_instant_and_answers_spark_elsewhere`, `test_live_spark_stores_the_recorded_session_wall`. |
| C-011 | Against main the door matrix (28 doors, both row-level modes, five sources, three zones, three schemas: 1980 cells) moves 192 cells, every one to the refusal: 176 instant cells (108 UTC wall, 32 session wall, 36 raw errors), 8 `CAST` cells on the plain `UPDATE` door that stored the UTC wall, 8 raw errors on the same door with no `WHERE`. No other cell moves. | `probe.py` on main and on head; the cell diff. | **PROVEN** | §5. Main: `task/wo/ice-ntz-nested-refuse-1/evidence/repark-main-c516c54a.jsonl`; head: `repark-head.jsonl` beside it. |
| C-012 | Eight hand mutants are each killed by a named pin. | §7. | **PROVEN** | §7. |
| C-013 | The unit's gates are green with real exit codes. | §8. | **PROVEN** | §8. |
| C-014 | A scoped Opus verifier on a fresh agent finds no S0 or S1 defect. | The verifier's report. | **OPEN** | Round 0 (2026-10-20, `8f56bcf5`): FAIL, S1-1, S1-2, S2-1 and four S3 notes, each answered by fix round 1 (C-015 to C-018). Round 1: pending. |
| C-015 | Outside UTC a value whose plan holds a `CAST` or `TRY_CAST` from a type holding a zoned timestamp to one holding a naive one refuses on every door: in the source, a subquery, a temporary view (`ReplanningTempView`), a catalog view, a DataFrame's frame view; a struct, an array; `INSERT … SELECT`, `VALUES`, a cast of a zoned struct column, MERGE insert values, MERGE update set and `UPDATE SET *`, the plain `UPDATE`, a static `PARTITION`, a branch, `REPLACE WHERE` and every DataFrame writer but a cached frame. The leaf cast, which is lowered to the session-aware cast function and not to a planner cast, and a container cast of a wall or a string still store. In UTC the container cast stores the UTC wall as on main. | The Rust door in New York, Kolkata and UTC; the facade writers; the matrix and the attack probe (C-019). | **PROVEN** | §9. `a_container_cast_of_an_instant_refuses_on_every_door_outside_utc`, `a_container_cast_of_an_instant_stores_the_wall_in_a_utc_session`, `the_leaf_cast_and_a_container_cast_of_a_wall_still_store_outside_utc`, `test_a_frame_holding_a_container_cast_of_an_instant_refuses_outside_utc`. |
| C-016 | A `PREPARE` that holds a parameter and supplies the column refuses in every zone, typed (`TIMESTAMP`, `TIMESTAMP_NTZ`, `STRING`, `INT`) or untyped, in a `SELECT` or a `VALUES` source, wherever the parameter sits; one with no parameter is decided as the write it runs, and one that does not supply the column prepares; both execute. `EXECUTE` therefore runs only a statement already decided. | The Rust door in UTC and New York; the attack probe. | **PROVEN** | §9. `a_prepared_write_with_a_parameter_that_supplies_the_column_refuses_in_every_zone`. Measured on the round's first build: `EXECUTE` casts an instant argument to a declared `STRING` or `TIMESTAMP_NTZ` parameter through its UTC wall, so no declared type proves a wall. |
| C-017 | Outside UTC the plain `UPDATE` that assigns a whole struct or array above the leaf refuses a value that calls the `TIMESTAMP_NTZ` cast, whatever its input (a string, `TRY_CAST`, a wall literal, an array element); a literal, `to_timestamp_ntz`, the field assignment and MERGE store the wall; UTC stores the wall and a date cast midnight. No EQUAL cell moves: every such cell stored the wall plus the offset on main. | The Rust door in three zones; the matrix's `ntz_text_cast` source. | **PROVEN** | §9. `the_plain_update_refuses_a_timestamp_ntz_cast_in_a_whole_struct_outside_utc`. |
| C-018 | The type read plans its `SELECT` and never runs it: a counting function in the source is called as often in New York as in UTC (where the gate does not run) across an `INSERT`, an `UPDATE` and a `MERGE`; a refused write never calls it, adds no snapshot, writes no file and leaves no view. | The Rust door. | **PROVEN** | §9. `the_type_read_never_runs_the_source`. |
| C-019 | Against main `c516c54a`, the extended matrix (3168 cells: the round-0 1980 plus `ntz_text_cast`, `ts_struct_cast`, `ts_struct_cast_column`) moves 360 cells, every one to the refusal: the round-0 192 and 168 new ones (100 container-cast UTC walls, 4 string-cast walls shifted by the offset on the plain `UPDATE`, 64 raw errors). No other cell moves: 2112 control cells, every UTC cell, the leaf cast's 132 session-wall cells. The round-0 cells answer as in round 0 but for the new text. The attack probe (28 shapes × 3 zones) refuses every shape that stored a wrong wall. | `probe.py run` and `probe.py attack` on main and head; the cell diff. | **PROVEN** | §9. `task/wo/ice-ntz-nested-refuse-1/evidence/`. |
| C-020 | Each round-1 refusal has a pin that a one-line revert kills (seven hand mutants). | §10. | **PROVEN** | §10. |
| C-021 | The round's gates are green with real exit codes. | §11. | **PROVEN** | §11. |
| C-022 | Outside UTC the plain `UPDATE` that assigns a whole struct or array above the leaf no longer refuses a value for holding a zoned sub-expression; it still refuses a `TIMESTAMP_NTZ` cast in the value (C-017), a planned type holding a zoned timestamp and a container cast of an instant. `to_timestamp_ntz(<instant>)` and `to_timestamp_ntz(coalesce(z, <instant>))` store the session wall, a wall literal beside `IF(z IS NULL, 1, 2)` and `named_struct('v', st.v, 'n', CAST(year(coalesce(z, <instant>)) AS INT))` keep the wall, as Spark 4.1.2 and main store them. No wrong wall re-opens. *(Supersedes the zoned-sub-expression arm of C-001, C-005 and §1, fix round 2, verifier S1-2.)* | The four cells in New York and Kolkata at the Rust door; the 3168-cell matrix and the 84-cell probe on the build against `c20c3e25`. | **PROVEN** | §12. `the_plain_update_stores_a_wall_beside_a_zoned_sub_expression`. |
| C-023 | Outside UTC the container-cast test (C-015) and the plain `UPDATE`'s cast test (C-017) read only the lineage of the supplied column: the expression that produces it in the typing plan, followed column by column through projections, aliases, derived tables, filters, sorts, limits, `DISTINCT`, the side of a join it comes from, every branch of a union, windows, values rows and a `ReplanningTempView`'s definition plan. A predicate, an `EXISTS` in a `WHERE`, a join condition and a sibling column are not read, so a container cast of an instant there no longer refuses and the wall stores. Read whole, fail closed: a subquery inside the column's expression (its outer references followed), an aggregate's group and aggregate expressions, any other node, a column that resolves in no input. A cast that reaches the column through a derived table, a union branch, a join side, a window, a grouping, a temporary view or a MERGE source still refuses. *(Supersedes the whole-statement walk of C-015, fix round 2, verifier S1-3.)* | The Rust door in New York and Kolkata, three shapes off the lineage, seven on it, with the walk disabled as the control; the verifier's cells; the 3168-cell matrix and the 84-cell probe against `c20c3e25`. | **PROVEN** | §12. `the_cast_walk_reads_only_what_reaches_the_column`. |
| C-024 | A subquery inside the expression that produces the column is read whole: a scalar subquery returning a container cast of an instant (directly, through a derived table, as a MERGE update value) refuses outside UTC, and so does an `IN` or `EXISTS` subquery holding one in the column's own expression; a scalar subquery of a wall or of the leaf cast stores. The walk does not descend a scan source's own logical plan: DataFusion 54.1.0 (`datafusion-expr` `logical_plan/builder.rs`, `scan_with_filters_inner`) inlines every scan whose source carries a plan and has no filters, and the SQL planner, `read_table` and the session's table reads build scans with no filters, so a catalog view or a frame view never reaches the walk as such a scan; the branch that read it (verifier M9) is removed as dead. *(Fix round 2, verifier S1-4 and S3-2.)* | The Rust door in New York and Kolkata with the subquery read removed as the control; the premise pin over the three view kinds; the verifier's M9 and M10 and their round-2 equivalents (§12). | **PROVEN** | §12. `a_subquery_in_the_columns_expression_is_read_whole`, `a_view_the_write_reads_is_inlined_before_the_walk`. |
| C-025 | A UTC session refuses no write but a `PREPARE`: every `PREPARE` of a write that supplies such a column with an instant, a cast of one or any parameter refuses in every zone, UTC included. Measured cost in correct cells: 17, 12 by the parameter rule (the named-shape probe's two parameterised controls in three zones, 6; the verifier's parameter only in `WHERE`, only in `LIMIT`, only in a sibling column, in UTC and New York, 6) and 5 by the zone rule (UTC cells whose UTC wall is the session wall: four instant parameters in the probe, one parameterless instant in the verifier's cells). The named-shape probe's 48 moves are 38 wrong walls (24 container-cast, 2 of them on a branch, 8 `PREPARE`, 6 plain-`UPDATE` cast) and 10 correct cells (6 parameterised controls, 4 UTC `PREPARE` cells); the probe reads the branch write from the branch. *(Supersedes C-003's "a UTC session refuses nothing" and the breakdown of §9 and C-019, fix round 2, verifier S2-3 and S2-4.)* | The committed probe re-run on main and the build; the verifier's `PREPARE` cells. | **PROVEN** | §12. `a_prepared_write_with_a_parameter_that_supplies_the_column_refuses_in_every_zone`, `a_prepared_write_of_an_instant_refuses_in_a_utc_session_too`. |
| C-026 | The `list array` panic beside this unit is DataFusion's `map()` constructor over a column value (`MapFunc::invoke_with_args` → `list_to_arrays` → `arrow-array` 58.4.0 `cast.rs:929`), for any value type, in a plain `SELECT` as in a write, on main and on the build, before any store; a `map()` of literals stores, and a container cast of a map literal does not panic but returns the UTC wall. It is not this unit's and is carded as MAP-COLUMN-PANIC-1 (`task/roadmap/mid-term/map-column-panic-1-card-2026-10-20.md`). *(Supersedes §9's "the container cast of a map panics in the engine", fix round 2, verifier S2-2.)* | The verifier's residue and map probes re-run on the build and on main. | **PROVEN** | §12. Residue, not gated; the map residue line in `crates/repark-spark/tests/map.md`. |
| C-027 | Not gated, stated (owner's ruling pending): outside UTC, a container cast or `from_json(to_json(…))` whose zoned leaf passes through `STRING`, `DATE` or `BIGINT` stores the UTC wall (the UTC date, the UTC wall to the millisecond) on main and on the build, on `INSERT … SELECT` and a MERGE update, and the six table-creating writers (CTAS, `CREATE OR REPLACE TABLE … AS SELECT`, `REPLACE TABLE … AS SELECT`, `saveAsTable` in mode `overwrite`, `writeTo().create()`, `.replace()`, `.createOrReplace()`) store the UTC wall of a container cast of an instant; Spark 4.1.2 stores the session wall (`STRING`, the writers), the session date (`DATE`), refuses `BIGINT` and stores NULL through JSON. One mechanism, the container cast and `to_json` rendering a zoned leaf in UTC for every target type, carded as CONTAINER-CAST-ZONE-1 (`task/roadmap/mid-term/container-cast-zone-1-card-2026-10-20.md`); the owner rules whether this unit grows to gate it. *(Supersedes C-015's "a container cast of a wall or a string still store[s]" as a description of what passes; fix round 2, verifier S1-1 and S2-1.)* | The verifier's cells re-run on the build against main and Spark. | **PROVEN** | §12. Residue, not gated; the residue line in `crates/repark-spark/tests/map.md`. |
| C-028 | After fix round 2 the plain `UPDATE` that assigns a whole struct or array above the leaf refuses, outside UTC, a value whose lineage calls the `TIMESTAMP_NTZ` cast, and no other value for that reason. Every cell that rule moves in the 3168-cell matrix (8 `CAST` cells that stored the UTC wall, 8 raw errors, 4 string casts that stored the wall plus the offset) and in the named-shape probe (6 string, date and array-element casts that stored the wall or midnight plus the offset) held a wrong value or raised on main, in New York and Kolkata; the four EQUAL cells round 1's zoned arm refused store again (C-022). A zone whose offset is zero at the value is still a zone that is not UTC: in `Europe/London` on 2026-01-02 the string cast, the instant cast and a container cast of an instant refuse though Spark and main store the same wall (3 cells measured), by design, as `Etc/UTC` does. *(Supersedes C-017's "No EQUAL cell moves", fix round 2, verifier S1-2.)* | The matrix and the probes on the build; the London cells on Spark, main and the build. | **PROVEN** | §12. `the_plain_update_refuses_a_timestamp_ntz_cast_in_a_whole_struct_outside_utc`, `the_plain_update_stores_a_wall_beside_a_zoned_sub_expression`. |
| C-029 | The hint names the DataFrame remedy: a leaf cast written inside `F.expr(…)` is not lowered to the session-aware cast function, so on main `F.expr("named_struct('v', CAST(x AS TIMESTAMP_NTZ), 'n', 1)")` stores the UTC wall in New York (Spark 4.1.2 the session wall) and the build refuses it; the hint now reads "… or array(CAST(<value> AS TIMESTAMP_NTZ)) in SQL, or col.cast(\"timestamp_ntz\") on the leaf column of a DataFrame (a CAST written inside expr() is refused), …", and `col.cast("timestamp_ntz")` stores the session wall on that door. The text still names no dropping, truncating, overwriting, recreating, deleting or replacing action. The lowering of `expr()` is outside this unit's files: a residue line in R-011, not a change. *(Supersedes C-006's text, fix round 2, verifier S3-1.)* | The whole-text pin; the `F.expr` and `col.cast` cells on Spark, main and the build. | **PROVEN** | §12. `the_refusal_names_the_table_the_column_and_the_leaf`. |
| C-030 | On the round-2 build, against main `c516c54a`: the 3168-cell door matrix moves the same 360 cells, every one to the refusal, and answers every cell as `c20c3e25` does (refusal for refusal, value for value; only the text is the round-2 hint); the 84-cell named-shape probe moves 48 (38 wrong walls, 10 correct cells by design) and answers as `c20c3e25` with the branch now read; the verifier's named shapes answer as `c20c3e25` but for the cells of C-022 to C-024, each of which now stores the value Spark and main store or refuses a wrong wall. | `probe.py run` and `probe.py attack` on main and the build; the verifier's case files re-run. | **PROVEN** | §12. `task/wo/ice-ntz-nested-refuse-1/evidence/`. |
| C-031 | Every hand mutant of both verifier rounds that still applies, and fifteen of this round's, is killed by a named pin (35 run; the frame-view branch of M9 is gone). | §12. | **PROVEN** | §12. |
| C-032 | The round's gates are green with real exit codes, and the round costs nothing measurable (every 10-run median ratio against main within the 1.05x gate). | §12. | **PROVEN** | §12. |
| C-033 | Outside UTC the lineage walk follows a `LATERAL` subquery's outer reference into the join's other side: a container cast of an instant that reaches the column through `LATERAL (SELECT a.c AS s)` (comma, `CROSS JOIN LATERAL`, `JOIN LATERAL … ON true`, `LEFT JOIN LATERAL`, an array element, an `ARRAY` column, a temporary view, `BY NAME`, a MERGE insert and a MERGE update, a lateral of a lateral) refuses and writes nothing, and a lateral that returns only a wall sibling of that cast stores the wall. Fail closed: an outer reference no enclosing join resolves refuses, and a node read whole inside the join's right side that holds an outer reference reads the left side whole too. *(Supersedes C-023's reading of a `Subquery` node as a pass-through, fix round 3, verifier round-2 S1-1.)* | The verifier's 17 cells and control re-run on the build; this round's lateral cells on Spark, main and the build; the two Rust lateral pins with mutant L16 as the control; two synthetic-plan pins for the fail-closed arms. | **PROVEN** | §13.1. `the_cast_walk_reads_only_what_reaches_the_column`, `router::nested_ns::tests::an_outer_reference_no_enclosing_join_resolves_fails_closed`, `router::nested_ns::tests::an_unmodelled_node_that_refers_out_reads_the_other_side_whole`. |
| C-034 | Every lineage arm that opens a hole when it stops has a pin that a one-line mutant of the arm reds: `DISTINCT` (L7), a limit inside a derived table (L9), a window's input columns (L13), a values row past the first (L14), a top-level sort (L8), a repartition (L18), a node the walk does not model read whole (L5), the `LATERAL` outer reference (L16, R1, R2, R7), the nearest scope that holds it (R5) and the scopes outside the one it resolves in (R6). Every hand mutant of the three verifier rounds that still applies, and this round's, is killed by a named pin but two, each equivalent on every plan the type read builds: L11 (no scan the type read reaches carries a projection) and L17 (the step bound only refuses past 65,536 steps). *(Supersedes C-031's mutant set, fix round 3, verifier round-2 S1-3.)* | 61 mutants on the round's head, each alone over the four integration files, `--lib nested` and repark-iceberg `--lib nested_ns_gate`; the survivors re-run after the pins. | **PROVEN** | §13.2. `the_cast_walk_reads_only_what_reaches_the_column`, `a_frame_through_a_repartition_or_an_unmodelled_node_refuses`. |
| C-035 | The refusal's measured cost in correct cells, listed by class, no rule changed: (1) 18 cells the lineage walk reads whole without following (9 shapes in New York and Kolkata, each writing the wall: a scalar subquery read whole, 2 shapes, 4 cells; aggregate expressions read whole, 3 shapes, 6 cells; a grouping key, 1 shape, 2 cells; a `CASE`/`IF` condition or a sibling field of the same `named_struct`, 3 shapes, 6 cells); (2) the `IN`/`EXISTS` cell, 1, with the 4 scalar-subquery cells of (1) belonging with it; (3) `PREPARE`, 17 (C-025); (4) zero-offset zones whose id the session does not read as UTC (`Etc/UTC`, `Etc/GMT`, `Etc/Universal`, `Etc/Zulu`, `Africa/Abidjan`, `Atlantic/Reykjavik` all year, `Europe/London` in winter): 180 matrix cells move per zone, 126 of them correct cells and 54 raw errors on main. The owner rules on narrowing the walk and on fixed zero-offset zones. *(Supersedes C-028's "3 cells measured" and C-024's silence on the cost, fix round 3, verifier round-2 S1-2 and S2-1.)* | The verifier's `cases_o.py` and `cases_z.py` and a 28-door matrix in two zero-offset zones, re-run on main and the build. | **PROVEN** | §13.3. R-011 "Over-refusal, by design"; the v1.5.6 notes line. |
| C-036 | C-014, verifier round 0 (2026-10-20, head `8f56bcf5`): **FAIL**, with S1-1 (a container `CAST` of an instant stored the UTC wall outside UTC), S1-2 (a parameterised `PREPARE` stored the UTC wall), S2-1 (the plain `UPDATE` door's mechanism misnamed) and four S3 notes; fix round 1 answered each (C-015 to C-021). *(A dated row for C-014's round 0, fix round 3, verifier round-2 S3-4; C-014 is not edited.)* | The round-0 verifier's hand-back. | **PROVEN** | The ledger's fix-round-1 preamble; PR #1124. |
| C-037 | C-014, verifier round 1 (2026-10-20, head `c20c3e25`): **FAIL**, with S1-1 (a cast through `STRING`, `DATE` or `BIGINT`, or `from_json(to_json(…))`, stored the UTC wall), S1-2 (the plain `UPDATE`'s zoned-sub-expression arm refused four EQUAL cells), S1-3 (the cast walk read the whole statement), S1-4 (no pin held the subquery descent) and S2-1 to S2-4; fix round 2 answered each (C-022 to C-032; S1-1 and S2-1 as the CONTAINER-CAST-ZONE-1 residue). *(A dated row for C-014's round 1, fix round 3, verifier round-2 S3-4.)* | The round-1 verifier's hand-back. | **PROVEN** | The ledger's fix-round-2 preamble; PR #1124. |
| C-038 | C-014, verifier round 2 (2026-10-21, head `75475b71`): **FAIL**, with S1-1 (a container cast of an instant through `LATERAL (SELECT a.c …)` stored the UTC wall, 17 cells), S1-2 (18 correct cells refused and not listed with a count), S1-3 (four lineage arms no pin held), S2-1 (the zero-offset-zone cost understated) and S3-1 to S3-4; fix round 3 answers each (C-033 to C-042; S3-2 as the LATERAL-CORRELATED-MEMORY-1 card). *(A dated row for C-014's round 2, fix round 3, verifier round-2 S3-4.)* | The round-2 verifier's hand-back. | **PROVEN** | The ledger's fix-round-3 preamble; PR #1124. |
| C-039 | C-014, verifier round 3 on fix round 3's head: pending. C-014 stays OPEN until a verifier round on a fresh agent finds no S0 or S1 defect. *(A dated row, fix round 3, verifier round-2 S3-4.)* | The round-3 verifier's hand-back. | **OPEN** | Round 3 has not run. |
| C-040 | Two of the verifier's round-2 notes, stated: S3-1, `crates/repark-spark/src/router/nested_ns.rs` measured 729 lines (`git show 75475b71:…` piped to `wc -l`) at `75475b71`, not the 726 that the fix-round-2 brief and hand-back gave, and 796 after fix round 3 (ceiling 1000); S3-3, the C-024 premise pin plans its three `SELECT`s through `ReparkSession::sql`, and that is the plan the type read's `planned()` builds: `ReparkSession::sql` → `SparkDialect::execute` → `router::execute_in_session`, which canonicalises verbatim literals and routes temporary-view DDL, then calls `router::execute_calibrated`, the function `planned()` calls; the pin's statements hold no literal and no DDL, so the same SQL text reaches the same planner, and neither path runs the optimizer (no router path calls `into_optimized_plan`). *(Fix round 3, verifier round-2 S3-1 and S3-3.)* | `wc -l` on both heads; the call chain in `crates/repark-core/src/session.rs`, `crates/repark-spark/src/dialect.rs` and `crates/repark-spark/src/router.rs`. | **PROVEN** | §13.4. `a_view_the_write_reads_is_inlined_before_the_walk`. |
| C-041 | Owner ruling 1 (2026-10-21): outside UTC the lineage walk does not read an `IN`, `EXISTS` or quantified subquery in the column's expression (its left operand still is), and reads a scalar subquery only through its one output column, the node's input pushed as the scope its outer references resolve in. A scalar subquery that returns a container cast of an instant (directly, through a derived table, as a MERGE update value, over a temporary view, nested, or as an outer column followed back out) still refuses; one whose cast feeds only its own `WHERE` or a column it does not return, a correlated one whose outer reference feeds only its `WHERE`, and an `EXISTS` in the value store Spark's wall. Cells moved, on main `c516c54a`, Spark 4.1.2 and the build: the verifier's 4 scalar-subquery cells (New York, Kolkata) and the 2 `EXISTS` cells store the wall as main and Spark do; the `IN` shape raises the engine's own error, as on main (round 2 refused it). The `LATERAL` outer-reference walk (C-033) is a plan node under a join and is unchanged. *(Supersedes C-024's whole read of a subquery in the column's expression and C-035's classes (1) and (2) for these 6 cells; the lineage over-refusal is 14 cells, 7 shapes.)* | The Rust door in New York and Kolkata, each storing cell red on the round-2 walk; the verifier's `cases_o.py` and this round's subquery cells on Spark, main and the build. | **PROVEN** | §13.6. `a_scalar_subquery_is_read_by_its_output_and_a_boolean_subquery_is_skipped`. |
| C-042 | Owner ruling 3 (2026-10-21): a session zone counts as UTC when its id, as written or as the session canonicalises it, is a pure UTC alias (`UTC`, `UCT`, `Universal`, `Zulu`, `Z`, `GMT`, `GMT0`, `GMT+0`, `GMT-0`, `Greenwich`, their `Etc/` forms, `+00:00`, `-00:00`), held in one set in `crates/repark-spark/src/router/nested_ns.rs`; `Africa/Abidjan`, `Atlantic/Reykjavik` and `Europe/London` stay refused (a pre-1912 local-mean-time offset in tzdb for the first two, a summer offset for London) and stay listed with 126 correct cells per zone. Cells moved: the verifier's five instant shapes store Spark's wall in `Etc/UTC`, `Etc/GMT`, `Etc/Universal` and `Etc/Zulu` (20 cells, refused before) and still refuse in the three geographic zones; the 28-door matrix in `Etc/GMT` moves 0 cells against main (180 before the ruling), in `Europe/London` 180 as before. *(Supersedes C-003's "`Etc/UTC` refuses" and C-035's class (4) for the pure aliases.)* | The unit test over both lists; the zone pin; the verifier's `cases_z.py` and the matrix in two zones on the build against main and the verifier's Spark cells. | **PROVEN** | §13.7. `router::nested_ns::tests::only_a_pure_utc_alias_counts_as_utc`, `only_a_utc_zone_lets_an_instant_through`. |
| C-043 | After owner rulings 1 and 3, on the round's final head: the 3168-cell door matrix moves the same 360 cells against main `c516c54a`, every one to the refusal, and the 84-cell named-shape probe the same 48 (38 wrong walls, 10 correct cells); neither probe holds a subquery shape or an `Etc/` zone, so the rulings move none of their cells, and both answer cell for cell as the committed `repark-head.jsonl` and `attack-head.jsonl`. Every mutant whose code or killing pin the rulings touched, and eight new ones on the rulings (Q1 to Q4, Z1 to Z4), was re-run alone: each is killed by a named pin but L11 and L17 (equivalent, C-034), M10 and Q2, each equivalent on stored values: the only plans that reach them (a scalar subquery inside a frame's `DISTINCT ON`; a correlated scalar subquery returning an outer column) raise `Physical plan does not support logical expression ScalarSubquery` on main and the build in every zone, so the mutant only trades that error for the refusal or the refusal for it. M10b, N5 and V2 mutated code the rulings removed; Q1 and Q3, R2 and Q2, Z3 and Z4 take their place. *(Fix round 3, after the owner's rulings of 2026-10-21.)* | `probe.py run` and `probe.py attack` on the final build; the targeted mutant pass. | **PROVEN** | §13.8. `a_frame_through_a_repartition_or_an_unmodelled_node_refuses`, `router::nested_ns::tests::only_a_pure_utc_alias_counts_as_utc`. |
| C-044 | Fix round 3's gates are green on the final code head `f795a0ff` with real exit codes but one carded flake, and the round costs nothing measurable: `make verify` 7746 passed; `make rust-panic-ban` exit 0; `make ci` on every commit; the facade suite 20227 passed and 1 failed, `test_t2_spill_reach.py::test_sort_merge_join_spills_under_small_fair_pool` (SPILL-REACH-FAIR-POOL-FLAKE-1, carded 2026-10-14; alone it failed 1 of 8 runs on main `c516c54a` and 1 of 10 on this head); the live leg alone 67 passed and co-collected 5924 passed, the live cell passing in both; every interleaved 50-run median ratio against main within the 1.05x gate. | §13.9. | **PROVEN** | §13.9. |
| C-045 | C-014, verifier round 3 (2026-10-21, head `d9943218`, Opus 5.5): **FAIL**, with S1-1 (mutant M10 is not equivalent: a scalar subquery that returns a container cast, under an `Unnest` node, stored the UTC wall in New York), S1-2 (the unit adds 153 code comment lines against `d6ee0411`), S2-1 (a sibling-field shape of ruling 2 has no count), S2-2 (one armed facade run hung at 99 % with no test name), S2-3 (the New York nested `INSERT` reads 1.090 and 1.050) and S3-1 to S3-4; fix round 4 answers S1-1 (C-046), S1-2 (C-047), S2-1 (C-048) and S2-2 and S2-3 (C-049, stated, not changed). The code held on every cell the verifier measured: the lateral walk, rulings 1 and 3, every count, 39 mutants killed. *(Supersedes C-039's `pending`; C-014 stays OPEN until a verifier finds no S0 or S1 defect. Fix round 4.)* | The round-3 verifier's hand-back. | **PROVEN** | §14.1; PR #1124. |
| C-046 | Mutant M10 (`plan_has` reads a whole-read node with `apply` in place of `apply_with_subqueries`) is not equivalent on stored values, and a pin now holds it. A DataFrame `explode` puts an `Unnest` node, which the walk reads whole, above a projection whose scalar subquery returns the container cast of an instant; it runs on main and stored the UTC wall in New York where Spark 4.1.2 stores the New York wall (the verifier's three doors and a temporary view: `writeTo(…).append()`, `withColumn` and `append`, `write.insertInto`, `INSERT … SELECT … FROM <view>`). `a_scalar_subquery_returning_the_container_cast_under_an_unnest_refuses` plans that frame with `DataFrame::unnest_columns`, registers it as a temporary view and writes it: it refuses in New York and Kolkata with the hint and adds no snapshot, and the same frame whose subquery returns the wall stores the UTC wall. Under M10 the pin fails on both zones (cast stored, one snapshot), and it is the one test that fails across the `ntz_nested_refuse`, `ntz_nested_refuse_casts` and `ntz_nested_refuse_routes` binaries. No rule in `nested_ns.rs` changed. *(Supersedes C-043's equivalence claim for M10 and §13.8's M10 row; Q2 stays equivalent on stored values. Fix round 4.)* | The pin on the build and under M10; the full binaries under M10. | **PROVEN** | §14.2. `task/wo/ice-ntz-nested-refuse-1/evidence/m10-kill-round-4.txt`; `a_scalar_subquery_returning_the_container_cast_under_an_unnest_refuses`. |

## §1 The rule and the seam

The rule, the decision point and the type read are §1 and §2 of the sketch. As built:
`crates/repark-iceberg/src/write/nested_ns_gate.rs` holds the walk (`nested_naive_leaf`), the
text (`nested_naive_refusal`), the supply model with feeds (`nested_supplies`), the zone test
(`holds_zoned_timestamp`) and the path tests (`reaches_leaf`, `lands_on_naive_leaf`);
`refuse_nested_ns_supply` is R-015 over `nested_supplies`. `crates/repark-spark/src/router/nested_ns.rs`
`refuse_at` runs R-015 and then, outside UTC or under `PREPARE`, `refuse_zoned_micros`, which
plans one `SELECT` per supplied column through `router::execute_calibrated` and refuses when a
value's type holds a zoned timestamp, the plan fails, or (plain `UPDATE`, path above the leaf)
any planned sub-expression is zoned.

## §2 The doors

The Rust door pins carry eighteen doors × five shapes, five field doors, the six old doors, the
internal routes (branch, `branch_main`, WAP id and branch, `EXPLAIN ANALYZE`, `PREPARE`, CTAS of
`INSERT`, `WITH … INSERT`, an unparsed statement, a frame written by name) and the unsupplied
statements (column list without it, bare `NULL`, sibling `UPDATE` of `k` and of `st.n`,
`DELETE`, MERGE of other columns, two maintenance calls).

## §3 The plain `UPDATE` correction

Sketch §3. *(2026-10-20, fix round 1: the mechanism is the session offset added to every
`TIMESTAMP_NTZ` cast in the assigned value, not a conversion with UTC; C-017.)* The pin writes `UPDATE t SET st = named_struct('v', CAST(<instant> AS
TIMESTAMP_NTZ), 'n', 1) WHERE id >= 0` and expects the refusal; it writes
`UPDATE t SET st.v = CAST(<instant> AS TIMESTAMP_NTZ) WHERE id >= 0` and expects the session wall.

## §4 The text

As of fix round 1 (2026-10-20):

``[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table
`<catalog>`.`<namespace>`.`<table>`: Cannot safely cast `<column>`.`<leaf>` to "TIMESTAMP_NTZ".
A value that is not provably a TIMESTAMP_NTZ wall time is not written into a nested
TIMESTAMP_NTZ field while the session time zone is not UTC: cast the leaf itself, as in
named_struct('<field>', CAST(<value> AS TIMESTAMP_NTZ), ...) or array(CAST(<value> AS
TIMESTAMP_NTZ)), to store the session wall time (a CAST of the whole struct, array or map is
refused, and in an UPDATE assign the field itself), or set spark.sql.session.timeZone to UTC
for the statement. SQLSTATE: KD000``

Round 0 read "A TIMESTAMP value is not written … write the field from CAST(<value> AS
TIMESTAMP_NTZ) … (in an UPDATE, assign the field itself) …"; a user who applied that cast to
the whole struct got the silent UTC wall of S1-1.

## §5 The matrix

| `struct<v: TIMESTAMP_NTZ, n: INT>` group | Main `c516c54a` | Head |
|---|---|---|
| instant, New York and Kolkata (176) | 108 UTC wall, 32 session wall, 36 raw errors | 176 refused |
| `CAST` spelling, New York and Kolkata (176) | 132 session wall, 8 UTC wall, 36 raw errors | 132 session wall, 16 refused, 28 raw errors |
| wall literal, New York and Kolkata (88) | 72 wall, 16 raw errors | unchanged |
| UTC session, every source (220) | 176 stored, 44 raw errors | unchanged |
| `struct<v: TIMESTAMP>` and top-level `TIMESTAMP_NTZ` (1320) | — | 0 moved |

Per door (instant, two zones; head refuses every one): the six DataFrame doors,
`insert_select`, `insert_by_name`, `insert_overwrite`, `insert_replace_where` 4 UTC wall each;
`insert_values`, `insert_overwrite_values` 2 UTC wall + 2 raw; `merge_insert`,
`merge_insert_literal`, `merge_insert_star`, `merge_update_whole`, `merge_update_whole_literal`,
`merge_update_star`, `update_col_where`, `update_whole_where` 8 UTC wall each;
`merge_update_field`, `merge_update_field_literal`, `update_field_where`,
`update_field_col_where` 8 session wall each; `update_col_nowhere`, `update_field_col_nowhere`,
`update_field_nowhere`, `update_whole_nowhere` 8 raw errors each.

## §6 The facade and Spark

`python/repark/tests/timestamps/test_ice_ntz_nested_refuse_1.py`: five zoned layouts × eight
writers refuse, eight writers store a naive leaf in New York, eight store the UTC wall in UTC,
three zones of the 174 recorded cells, and the live re-derivation (JVM, `REPARK_PARITY_LIVE=1`):
green alone (1 passed, 61.7 s) and co-collected after `python/repark/tests/parity` (416 passed,
13 skipped, 109.5 s).

## §7 Mutants

Each applied alone to the build, the named pin run, the source restored (`git diff` empty after).

| Mutant | Change | Killed by |
|---|---|---|
| M1 | the zone condition always on | `a_nested_microsecond_ntz_field_refuses_an_instant_outside_utc` (UTC doors refuse) |
| M2 | a failed UPDATE or MERGE typing plan read as naive | `the_type_read_follows_a_cte_skips_a_zoned_sibling_and_fails_closed` (the unplannable assignment stores) |
| M2b | a failed INSERT source plan read as naive | the same pin (the unplannable INSERT value) |
| M3 | the plain `UPDATE` sub-expression rule removed | `a_wall_or_the_hints_cast_stores_outside_utc_and_the_plain_update_refuses_the_cast` |
| M4 | the struct arm of `holds_zoned_timestamp` returns false | `every_door_refuses_an_instant_outside_utc_and_writes_nothing` |
| M5 | the field-path pruning removed | `the_type_read_follows_a_cte_skips_a_zoned_sibling_and_fails_closed` (the zoned sibling refuses) |
| M6 | `PREPARE` decided by the zone | `a_prepared_write_of_an_instant_refuses_in_a_utc_session_too` |
| M7 | no fallback to every source column when no column is picked | `the_type_read_follows_a_cte_skips_a_zoned_sibling_and_fails_closed` (the static `PARTITION` instant stores) |

An equivalent mutant: disabling the by-name pick alone is caught by that fallback (the name feed
then picks nothing and every source column is read), so it only refuses more.

## §8 Gates

Every heavy command ran under `taskset -c 0-25` with `CARGO_BUILD_JOBS=12`; the load average at
each start is given.

| Gate | Result | Wall | Load |
|---|---|---|---|
| `cargo fmt --all` | clean | — | — |
| `make verify` (`ci`, its `rust-panic-ban` and `rust-clippy` included, plus nextest and doctests) | exit 0; 7689 tests run, 7689 passed, 97 skipped | 225 s | 4.29 |
| `make rust-panic-ban` alone | exit 0 | 1 s | 10.32 |
| facade suite (`-n 16 --dist loadgroup --maxschedchunk=8`, after `uv sync --locked` and `maturin develop --profile ci`) | 20160 passed, 484 skipped, 151 xfailed, 1 failed: `perf/test_perf_unpivot_1.py::test_stack_is_linear_in_columns` (a timing exponent, 1.231 > 1.1, with the box at load 14 to 19 from other work); alone: 9 passed | 375 s | 14.67 |
| live leg, the module alone (`REPARK_PARITY_LIVE=1`) | 60 passed | 66 s | 14.77 |
| live leg co-collected after `python/repark/tests/parity` | 416 passed, 13 skipped; `test_live_spark_stores_the_recorded_session_wall` PASSED | 109 s | 14.77 |
| `make ci` after the last edit | exit 0 | 94 s | 4.08 |
| `python3 scripts/sync_map_md.py` | clean, 416 maps | — | — |

An engine flake found on the way, not this unit's: on a table with a `BINARY` column, `INSERT …
VALUES (…, x'00')` followed by `UPDATE … SET k = k + 1 WHERE id >= 10` fails 3 times in 12 with
`Mismatch between schema and batches`, in a UTC session where this unit's check does not run.
The pin dropped that column; the flake is reported in the hand-back.

## §9 Fix round 1: the rules and the cells

What each verifier finding became:

| Finding | Became | Clause |
|---|---|---|
| S1-1 a container `CAST` of an instant stores the UTC wall | a `CAST` or `TRY_CAST` from a type holding a zoned timestamp to one holding a naive one refuses, wherever the plan holds it (node, subquery, frame view, `ReplanningTempView` definition); the hint names the leaf cast and says the whole-value cast is refused | C-015, C-006 |
| S1-2 a parameterised `PREPARE` stores the UTC wall at `EXECUTE` | a `PREPARE` that holds a parameter refuses every supply of the column, in every zone; `EXECUTE` therefore runs only a decided statement | C-016 |
| S2-1 the plain `UPDATE` shifts a string-built wall; the mechanism was misnamed | the mechanism is corrected in the sketch §3, the router `map.md` row and the parity row (the session offset is added to every `TIMESTAMP_NTZ` cast in the value); outside UTC that door refuses any `TIMESTAMP_NTZ` cast in a whole-struct or array value | C-005, C-017 |
| S3-1 the text names a TIMESTAMP value for an unplannable wall source | the text is source-neutral | C-006 |
| S3-2 non-execution not pinned on its own | a counting function pins it | C-018 |
| S3-3 "174 recorded cells" imprecise | the shapes and the two walls are named | C-010 |
| S3-4 notes | unchanged and recorded (fail-closed zone spellings) | C-001 |

The extended matrix against main `c516c54a` (3168 cells; the 1980 round-0 cells on main equal
the round-0 file):

| `struct<v: TIMESTAMP_NTZ, n: INT>` group, New York and Kolkata | Main | Head |
|---|---|---|
| round-0 sources | as §5 | as §5, with the new text (192 refused) |
| `ts_struct_cast` (88) | 52 UTC wall, 36 raw errors | 88 refused |
| `ts_struct_cast_column` (88) | 48 UTC wall, 8 session wall (sibling-column doors), 32 raw errors | 72 refused; 8 session wall and 8 raw errors kept |
| `ntz_text_cast` (88) | 68 wall, 4 wall plus the offset (plain `UPDATE`), 16 raw errors | 68 wall, 8 refused, 12 raw errors |
| every UTC cell, `struct<v: TIMESTAMP>`, top-level `TIMESTAMP_NTZ` (2112 controls) | — | 0 moved |

360 cells move, every one to the refusal: 192 of round 0 and 168 new (100 UTC walls, 4 shifted
walls, 64 raw errors). The leaf cast keeps its 132 session-wall cells. The named-shape probe
(28 shapes × 3 zones, 84 cells) moves 48: 42 wrong walls to the refusal (12 container-cast
shapes × 2 zones, 4 `PREPARE` shapes × 3 zones, 3 plain-`UPDATE` casts × 2 zones) and 6 cells
of the two parameterised `PREPARE` controls, refused by design (C-016). The verifier's own
`attack.py` cases re-run on head: every S1 cell refuses; the container cast of a map panics in
the engine (`list array`) on main and head before any store, as the verifier found.

Not reached by the gate (dated rows in R-011): a value materialised before the write (a cached
frame, a table built by a container cast) keeps the UTC wall the cast produced in its `SELECT`.

## §10 Fix-round-1 mutants

Each applied alone, the new pin file run, the source restored (`cargo test` green after).

| Mutant | Change | Killed by |
|---|---|---|
| R1 | the container-cast walk removed from the INSERT and star type read | `a_container_cast_of_an_instant_refuses_on_every_door_outside_utc` |
| R2 | the container-cast walk removed from the UPDATE and MERGE type read | the same pin (the MERGE doors) |
| R3 | the `ReplanningTempView` descent removed | the same pin (the temporary view) |
| R4 | a parameterised `PREPARE` read as typed | `a_prepared_write_with_a_parameter_that_supplies_the_column_refuses_in_every_zone` |
| R5 | the plain `UPDATE` `TIMESTAMP_NTZ` cast rule removed | `the_plain_update_refuses_a_timestamp_ntz_cast_in_a_whole_struct_outside_utc` |
| R6 | the typing `SELECT` executed (`frame.collect()`) | `the_type_read_never_runs_the_source` |
| R7 | any container cast to a naive type refuses | `the_leaf_cast_and_a_container_cast_of_a_wall_still_store_outside_utc` |

## §11 Fix-round-1 gates

After `git merge origin/main` (`754fb6e5`). Every heavy command under `taskset -c 0-25` with
`CARGO_BUILD_JOBS=12`; the load average at each start is given.

| Gate | Result | Wall | Load |
|---|---|---|---|
| `cargo fmt --all -- --check` | clean | — | — |
| `make verify` | exit 0; 7695 tests run, 7695 passed, 97 skipped | 394 s | 2.59 |
| `make rust-panic-ban` | exit 0 | 1 s | 9.65 |
| `maturin develop --profile ci` | ok | 179 s | 5.77 |
| facade suite (`-n 16 --dist loadgroup --maxschedchunk=8`) | 20168 passed, 484 skipped, 151 xfailed, 0 failed | 432 s | 9.65 |
| live leg, the module alone | 67 passed | 68 s | 15.49, no other JVM |
| live leg co-collected after `python/repark/tests/parity` | 423 passed, 13 skipped; `test_live_spark_stores_the_recorded_session_wall` PASSED | 140 s | 7.28, no other JVM |
| `make ci` after the last code edit | exit 0 | 107 s | 3.53 |
| `python3 scripts/sync_map_md.py` | clean | — | — |

## §12 Fix round 2: what each finding became

### §12.1 S1-2, the plain `UPDATE`'s zoned-sub-expression arm (C-022)

The second arm of `shifts_in_an_update` (`crates/repark-spark/src/router/nested_ns.rs`) is
gone; the `TIMESTAMP_NTZ`-cast arm stays. Build of the arm's removal alone, New York
(`taskset -c 0-25`, load 5.10 at start):

| Cell | Spark 4.1.2 | main | `c20c3e25` | this build |
|---|---|---|---|---|
| `named_struct('v', to_timestamp_ntz(<instant>), 'n', 1)` | session wall | session wall | refused | session wall |
| `named_struct('v', to_timestamp_ntz(coalesce(z, <instant>)), 'n', 1)` | session wall | session wall | refused | session wall |
| `named_struct('v', <wall literal>, 'n', IF(z IS NULL, 1, 2))` | the wall | the wall | refused | the wall |
| `named_struct('v', st.v, 'n', CAST(year(coalesce(z, <instant>)) AS INT))` | the wall | the wall | refused | the wall |

The 3168-cell door matrix (`probe.py run`, 112 s) answers cell for cell as the committed
`repark-head.jsonl` of `c20c3e25` (0 cells differ; 360 moved against main, every one refused),
and the 84-cell named-shape probe (`probe.py attack`) as the committed `attack-head.jsonl`
(0 cells differ). No cell that held a wrong wall on main stores on this build.

### §12.2 S1-3, the walk's reach (C-023)

`lineage_has` (`crates/repark-spark/src/router/nested_ns.rs`) replaces the whole-statement
`plan_has` at both call sites of the type read; `plan_has` stays as the whole read of a
subquery, an aggregate's input expressions and any node the lineage does not model. It fits
the file (`nested_ns.rs` 685 lines, ceiling 1000); no new module.

Build of items 1 and 2 (`taskset -c 0-25`, load 6.89 at the probe start), New York:

| Verifier cell | Spark 4.1.2 | main | `c20c3e25` | this build |
|---|---|---|---|---|
| wall column, `WHERE EXISTS (SELECT … CAST(named_struct('v', x, 'n', 1) AS STRUCT<…>) IS NOT NULL)` | the wall | the wall | refused | the wall |
| wall column, `WHERE element_at(CAST(array(x) AS ARRAY<TIMESTAMP_NTZ>), 1) IS NOT NULL` | the wall | the wall | refused | the wall |
| wall column beside a sibling `ARRAY<STRING>` fed by `CAST(CAST(array(x) AS ARRAY<TIMESTAMP_NTZ>) AS ARRAY<STRING>)` | the wall | the wall | refused | the wall |

The 3168-cell matrix (135 s) and the 84-cell probe answer cell for cell as at `c20c3e25`
(0 cells differ in either; every one of the 360 refusals holds). The verifier's other named
shapes (`cases_a`, `cases_b`, `cases_d`, `cases_e`, UTC and New York) answer as at
`c20c3e25` but for the cells of C-022 and C-023 above, which store the value main and Spark
store, and the row order of three two-row `MERGE` reads in UTC. Every container cast that
reaches the column still refuses: `CASE`, `COALESCE`, `IF`, `struct`, `named_struct` of an
element, `transform`, `map_from_entries`, `map_from_arrays`, a CTE read twice, a CTE
self-join, `first_value … OVER`, a cast of `lag`, `TRY_CAST`, a scalar subquery, temporary,
catalog, DataFrame and nested views, MERGE sources, `BY NAME`, a reordered column list,
dynamic `INSERT OVERWRITE`, a `UNION ALL` branch, `VALUES`.

`sibling_col_cast_ctrl` still refuses, by the zoned-pairing over-refusal (R-011: the sibling
column holds its own nested `TIMESTAMP_NTZ` leaf and is written from a value with a zoned
leaf), not by the walk.

### §12.3 S1-4 and S3-2, the subquery read and the frame-view branch (C-024)

New York, `cases_r3` (UTC and New York; main `c516c54a`, the build of items 1 and 2, Spark
4.1.2 `local[1]`, load 4.20 at the Spark start, 26 s):

| Shape | Spark | main | this build |
|---|---|---|---|
| `INSERT … SELECT 2, (SELECT CAST(named_struct('v', x, 'n', 1) AS STRUCT<…>) FROM src), 0` | session wall | UTC wall | refused |
| the same through a derived table in the subquery | session wall | UTC wall | refused |
| `MERGE … UPDATE SET t.st = (SELECT <the cast> FROM src)` | session wall | UTC wall | refused |
| `IF(id IN (SELECT id FROM src WHERE element_at(CAST(array(x) AS ARRAY<TIMESTAMP_NTZ>), 1) IS NOT NULL), named_struct('v', w, 'n', 1), NULL)` | the wall | raw error (`This feature is not implemented`) | refused |
| `IF(EXISTS (SELECT 1 FROM src WHERE <the cast> IS NOT NULL), named_struct('v', w, 'n', 1), NULL)` | the wall | the wall | refused (over-refusal, by design) |
| a scalar subquery of `named_struct('v', w, 'n', 1)` | the wall | the wall | the wall |
| a scalar subquery of the leaf cast | session wall | session wall | session wall |
| the cast through a derived table, a join side, a `GROUP BY` | session wall | UTC wall | refused |

With the subquery read removed (the round-2 equivalent of the verifier's M10), the three
scalar shapes store the UTC wall, the `EXISTS` shape stores and the `IN` shape raises the
engine's error; `a_subquery_in_the_columns_expression_is_read_whole` fails on each. In UTC
every shape answers as main.

The frame-view branch (`scan.source.get_logical_plan()`) is removed, not pinned: no plan the
router builds can reach it (C-024). `a_view_the_write_reads_is_inlined_before_the_walk` pins
the premise, so a DataFusion upgrade that stops inlining reds it.

### §12.4 S2-3 and S2-4, the counts and the UTC sentence (C-025)

The named-shape probe's `insert_branch_struct_cast` wrote to `ice.ns.t.branch_bx` and read
`ice.ns.t`, so its cells read empty on main. `probe.py attack` now reads that case from the
branch (`ATTACK_READS`). Re-run on main `c516c54a` (load 6.91) and on the build of items 1 to
3 (load 6.76): the branch cells read `1767323045123456` on main in every zone, and every other
cell answers as the committed files. Against main the probe moves 48 cells, every one to the
refusal:

| Moved cells | Count | Main stored |
|---|---|---|
| container casts, 12 shapes × New York and Kolkata | 24 | the UTC wall (the 2 branch cells now read) |
| `PREPARE` of an instant parameter, 4 shapes × New York and Kolkata | 8 | the UTC wall |
| plain-`UPDATE` casts of a string, a date and an array element × New York and Kolkata | 6 | the wall plus the offset, midnight plus the offset |
| **wrong walls** | **38** | |
| `PREPARE` with a parameter in a sibling column or through the leaf cast × 3 zones | 6 | the wall, the session wall |
| `PREPARE` of an instant parameter, 4 shapes × UTC | 4 | the UTC wall, which is the session wall in UTC |
| **correct cells refused, by design** | **10** | |

Round 1's §9 read "42 wrong walls (… 4 `PREPARE` shapes × 3 zones …) and 6 cells of the two
parameterised controls"; the 4 UTC `PREPARE` cells were counted as wrong and the 2 branch cells
were unread. The verifier's cells (`cases_b`, UTC and New York) add 7 correct cells the
`PREPARE` rules refuse: a parameter only in `WHERE`, only in `LIMIT`, only in a sibling column
(6), and a parameterless `PREPARE` of an instant literal in UTC (1). A `PREPARE` whose parameter
does not supply the column (`INSERT (id, k) VALUES ($1, 0)`, `UPDATE … SET k = 9 WHERE id = $1`)
prepares and runs. The row, the v1.5.6 notes line and the sketch now say this.

### §12.5 S2-2, the map residue (C-026)

Re-run of the verifier's `residue.py` and `mapcheck.py` on the build of items 1 to 4, New York
(main gives the same answers, as the verifier measured on `c516c54a`):

| Statement | main | this build |
|---|---|---|
| `INSERT INTO ice.ns.m SELECT id, map('a', id) FROM ice.ns.src` (`MAP<STRING, INT>`) | panic `list array` | panic `list array` |
| `SELECT map('a', id) FROM ice.ns.src` | panic `list array` | panic `list array` |
| `INSERT … SELECT id, map('a', w)` and `map('a', CAST(x AS TIMESTAMP_NTZ))` and `CAST(map('a', x) AS MAP<…>)` into `MAP<STRING, TIMESTAMP_NTZ>` | panic `list array` | panic `list array` |
| `INSERT … SELECT 1, map('a', 1)` and `map('a', <wall literal>)` | stores | stores |
| `SELECT CAST(map('a', <instant>) AS MAP<STRING, TIMESTAMP_NTZ>)['a']` | `1767323045123456` | `1767323045123456` |
| `SELECT map('a', CAST(<instant> AS TIMESTAMP_NTZ))['a']` | `1767305045123456` | `1767305045123456` |

The panic is caught at the Python boundary (`a Rust panic was caught at the Python boundary`)
and comes from the engine's evaluation of `map()` over a column, not from this unit's gate,
which never executes its typing plan (C-018). The row's residue line, this ledger and the test
map now name `map(<column>)`.

### §12.6 S1-1 and S2-1 stated, C-017 corrected (C-027, C-028)

Re-run of the verifier's `cases_c` and `cases_d` on the build of items 1 and 2, New York (in UTC
every cell answers as on main):

| Statement | Spark 4.1.2 | main | this build |
|---|---|---|---|
| `INSERT … SELECT id, CAST(CAST(named_struct('v', x, 'n', 1) AS STRUCT<v: STRING, n: INT>) AS STRUCT<v: TIMESTAMP_NTZ, n: INT>), 0` | session wall | UTC wall | UTC wall |
| the same into `ARRAY<TIMESTAMP_NTZ>` through `ARRAY<STRING>` | session wall | UTC wall | UTC wall |
| `named_struct('v', CAST(CAST(array(x) AS ARRAY<STRING>)[0] AS TIMESTAMP_NTZ), 'n', 1)` | session wall | UTC wall | UTC wall |
| through `DATE` | `1767225600000000` | `1767312000000000` | `1767312000000000` |
| through `BIGINT` | `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` | UTC wall | UTC wall |
| MERGE update set through `STRING` | session wall | UTC wall | UTC wall |
| `from_json(to_json(named_struct('v', x, 'n', 1)), 'v TIMESTAMP_NTZ, n INT')` | NULL | `1767323045123000` | `1767323045123000` |
| CTAS, `CREATE OR REPLACE TABLE … AS SELECT` of a container cast of an instant | session wall | UTC wall | UTC wall |
| `REPLACE TABLE … AS SELECT`, `saveAsTable` overwrite, `writeTo().create()`, `.replace()`, `.createOrReplace()` | (not measured on Spark) | UTC wall | UTC wall |

None moves between main and the build; this round does not gate them (brief, owner's ruling
pending). `Europe/London`, 2026-01-02 (offset zero), `cases_london`: Spark, main and the build
answer the wall, the wall and the refusal for the plain `UPDATE`'s string cast, its instant
cast and an `INSERT` of a container cast of an instant.

### §12.7 S3-1, the hint on the `F.expr` door (C-029)

`cases_r3`, a frame `spark.table("ice.ns.src").select(id, <leaf>, 0).writeTo(…).append()`,
New York (UTC answers the UTC wall everywhere):

| Leaf | Spark 4.1.2 | main | build of items 1 to 3 |
|---|---|---|---|
| `F.expr("named_struct('v', CAST(x AS TIMESTAMP_NTZ), 'n', 1)")` | session wall | UTC wall | refused |
| `F.expr("named_struct('v', to_timestamp_ntz(x), 'n', 1)")` | session wall | session wall | session wall |
| `F.expr("named_struct('v', w, 'n', 1)")` | the wall | the wall | the wall |
| `F.struct(F.col("x").cast("timestamp_ntz").alias("v"), F.lit(1).alias("n"))` | session wall | session wall | session wall |
| SQL `named_struct('v', CAST(x AS TIMESTAMP_NTZ), 'n', 1)` | session wall | session wall | session wall |

The SQL text's `CAST … AS TIMESTAMP_NTZ` is rewritten to the session-aware cast function
(`keyword_lower/ntz_cast_lower.rs`) before planning; the `expr()` string of the DataFrame door
is not, so its cast is a planner cast of a zoned value and the gate refuses it. No one-line
lowering in this unit's files reaches that door, so it is a residue (R-011) and the hint stops
promising the SQL spelling there.

As of fix round 2 (2026-10-20) the text is:

``[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table
`<catalog>`.`<namespace>`.`<table>`: Cannot safely cast `<column>`.`<leaf>` to "TIMESTAMP_NTZ".
A value that is not provably a TIMESTAMP_NTZ wall time is not written into a nested
TIMESTAMP_NTZ field while the session time zone is not UTC: cast the leaf itself, as in
named_struct('<field>', CAST(<value> AS TIMESTAMP_NTZ), ...) or array(CAST(<value> AS
TIMESTAMP_NTZ)) in SQL, or col.cast("timestamp_ntz") on the leaf column of a DataFrame (a CAST
written inside expr() is refused), to store the session wall time (a CAST of the whole struct,
array or map is refused, and in an UPDATE assign the field itself), or set
spark.sql.session.timeZone to UTC for the statement. SQLSTATE: KD000``

### §12.8 The round's cells (C-030)

Build `eb81d0a5` (`maturin develop --profile ci`, 220 s, load 24.74 at start); main `c516c54a`
(the verifier's worktree and module). Every probe under `taskset -c 0-25`.

| Probe | Main | `c20c3e25` (committed) | This build |
|---|---|---|---|
| door matrix, 3168 cells (`probe.py run`, 116 s, load 6.80) | 360 cells differ from the build | 360 refused | 360 refused; 0 cells differ from `c20c3e25` in outcome; the 360 carry the round-2 hint |
| named-shape probe, 84 cells (`probe.py attack`, branch read from the branch) | 48 differ: 38 wrong walls, 10 correct | 48 refused | 48 refused, the same cells |
| the verifier's `cases_a` … `cases_e` (UTC, New York) | as the verifier measured | as the verifier measured | as `c20c3e25` but for 7 cells: the 4 of C-022 and the 3 of C-023 store the value main and Spark store |
| `cases_r3` (this round's subquery, lineage and `F.expr` shapes, UTC and New York) | see §12.3 and §12.7 | — | see §12.3 and §12.7 |

The committed `repark-head.jsonl` and `attack-head.jsonl` are this build's; the committed
`attack-main-c516c54a.jsonl` is the fixed probe on main.

### §12.9 The round's mutants (C-031)

Each applied alone, the suites `--test ntz_nested_refuse --test ntz_nested_refuse_casts --test
timestamp_ns_wall_doors --test timestamp_ns_nested_shapes`, `--lib nested` and repark-iceberg
`--lib nested_ns_gate` run, the source restored (baseline 139 passed; 140 after the last pin).
M8a, M10 and N10 survived the first pass and are killed by the pins added after it (two
subquery shapes, one unit test); each was re-run alone.

| Mutant | Change | Killed by |
|---|---|---|
| M1 | `Expr::Cast` arm removed from `casts_a_zoned_value_to_naive` | `a_container_cast_of_an_instant_refuses_on_every_door_outside_utc` and two more |
| M2 | `Expr::TryCast` arm removed | `a_container_cast_of_an_instant_refuses_on_every_door_outside_utc` |
| M3 | `PREPARE` parameter check inverted | `a_prepared_write_with_a_parameter_that_supplies_the_column_refuses_in_every_zone`, `a_prepared_write_of_an_instant_refuses_in_a_utc_session_too` |
| M4 / V3 | the plain `UPDATE`'s `TIMESTAMP_NTZ`-cast test off | `the_plain_update_refuses_a_timestamp_ntz_cast_in_a_whole_struct_outside_utc`, `a_wall_or_the_hints_cast_stores_outside_utc_and_the_plain_update_refuses_the_cast` |
| M5, M5b, V4 | the hint names dropping, recreating or overwriting | `the_refusal_names_the_table_the_column_and_the_leaf` |
| M6 / V6 | the typing `SELECT` executed | `the_type_read_never_runs_the_source`, `every_door_refuses_an_instant_outside_utc_and_writes_nothing` |
| M7 | the leaf-cast function of a zoned input read as a cast | four pins, `the_leaf_cast_and_a_container_cast_of_a_wall_still_store_outside_utc` among them |
| M8a | the `ReplanningTempView` descent of the whole read removed | `a_subquery_in_the_columns_expression_is_read_whole` (a scalar subquery over a temporary view) |
| M8b | the `ReplanningTempView` descent of the lineage removed | `a_container_cast_of_an_instant_refuses_on_every_door_outside_utc`, `the_cast_walk_reads_only_what_reaches_the_column` |
| M9 | frame-view descent removed | not applicable: the branch is removed as dead (C-024) |
| M10 | the whole read's `apply_with_subqueries` → `apply` | `a_subquery_in_the_columns_expression_is_read_whole` (a scalar subquery nested in a scalar subquery) |
| M10b | the lineage's subquery read removed | `a_subquery_in_the_columns_expression_is_read_whole` |
| M11 | the INSERT cast test removed | three pins |
| M12 | the UPDATE and MERGE cast test removed | three pins |
| M13 | the zoned-sub-expression arm restored | `the_plain_update_stores_a_wall_beside_a_zoned_sub_expression` |
| V1 | the zone condition inverted | 17 pins |
| V2 | `Etc/UTC` read as UTC | `only_a_utc_zone_lets_an_instant_through` |
| V5 | the nanosecond leaf takes the microsecond text | 9 pins |
| V7 | the outer `WITH` ignored | `the_type_read_follows_a_cte_skips_a_zoned_sibling_and_fails_closed` |
| V8 | the map arm of the zone test dropped | `every_door_refuses_an_instant_outside_utc_and_writes_nothing`, `a_zoned_timestamp_is_found_at_any_depth_and_in_any_layout` |
| V9 | the MERGE source dropped from the typing `FROM` | four pins |
| V10 | a supply with no value feed refuses | 13 pins |
| V11 | `PREPARE` decided by the zone | the two `PREPARE` pins |
| N1 | the lineage read as the whole statement | `the_cast_walk_reads_only_what_reaches_the_column` |
| N2 | a filter's predicate read | the same pin |
| N3 | a join's right side read on the left | the same pin |
| N4 | only a union's first branch read | the same pin |
| N5 | a subquery's outer references not followed | `a_subquery_in_the_columns_expression_is_read_whole` |
| N6 | an aggregate not read | `the_cast_walk_reads_only_what_reaches_the_column` |
| N7 | a values row not read | `a_container_cast_of_an_instant_refuses_on_every_door_outside_utc` |
| N8 | a window expression not read | `the_cast_walk_reads_only_what_reaches_the_column` |
| N9 | a projection's column references not followed | two pins |
| N10 | a column that resolves in no input not read | `router::nested_ns::tests::a_column_that_resolves_in_no_input_reads_every_input_whole` |

Not run: the lineage's step bound (`MAX_LINEAGE_STEPS`, fail closed past 65,536 steps); no plan
the probes build comes near it.

### §12.10 The round's gates and cost (C-032)

Every heavy command under `taskset -c 0-25` with `CARGO_BUILD_JOBS=12`; the box carried other
work at times, so the load average at each start is given.

| Gate | Result | Wall | Load |
|---|---|---|---|
| `cargo fmt --all`, `python3 scripts/sync_map_md.py`, `python3 scripts/check_model_ban.py` | clean on every commit (418 maps) | — | — |
| `make ci` on every commit | exit 0 | 84–218 s | 2.5–7.5 |
| `make check-comment-density` | not a target on this branch (dropped before #247; AGENTS.md); no comment line is added or removed in any code file (diff check) | — | — |
| `make verify` | exit 0; 7700 tests run, 7700 passed (3 slow), 97 skipped | 368 s | 2.54 |
| `make rust-panic-ban` | exit 0 | 1 s | 11.49 |
| facade suite (`-n 16 --dist loadgroup --maxschedchunk=8`) | 20168 passed, 484 skipped, 151 xfailed, 0 failed | 429 s | 10.04 |
| live leg, the module alone (`REPARK_PARITY_LIVE=1`) | 67 passed; `test_live_spark_stores_the_recorded_session_wall` PASSED | 72 s | 12.70, no other JVM |
| live leg co-collected (`tests/parity tests/writes` + the module, one run) | 5923 passed, 1 failed, 13 skipped, 3 xfailed; the module's live cell passed; the failure is `writes/test_ice_meta_delete_1.py::test_live_oracle_fixture_reproduces` (52 DRIFT cells), the carded PARITY-LIVE-DRIFT-1, which passes alone on this build (1 passed, 56 s) and which this branch does not touch | 809 s | 5.86, no other JVM |

Cost, main `c516c54a` against this build, alternating, 3 rounds; each round the median of 10
runs after 3 warm-ups (the verifier's `cost.py`); the table gives the median of the three
(load 3.8–4.4):

| Write | UTC main | UTC build | ratio | New York main | New York build | ratio |
|---|---|---|---|---|---|---|
| plain `INSERT` | 19.30 ms | 19.11 ms | 0.990 | 19.49 | 19.61 | 1.007 |
| plain `UPDATE` | 43.07 | 43.70 | 1.015 | 42.79 | 43.43 | 1.015 |
| plain `MERGE` | 73.95 | 75.08 | 1.015 | 74.01 | 74.03 | 1.000 |
| nested-NTZ `INSERT` | 23.51 | 22.90 | 0.974 | 23.25 | 24.06 | 1.035 |
| nested-NTZ `UPDATE` | 40.64 | 40.92 | 1.007 | 40.43 | 41.17 | 1.018 |
| nested-NTZ `MERGE` | 67.32 | 67.97 | 1.010 | 73.62 | 76.14 | 1.034 |


## §13 Fix round 3: what each finding became

### §13.1 S1-1, the `LATERAL` outer reference (C-033)

DataFusion 54.1.0 plans `FROM a, LATERAL (SELECT a.c AS s) l` as
`Join(a, SubqueryAlias(Subquery { subquery, outer_ref_columns }))`, and the lateral's
projection holds `Expr::OuterReferenceColumn(a.c)`. Round 2's `step` passed the column into the
subquery and `feeds` collected only `Expr::Column`, so the reference was dropped and the left
side, where the cast sits, was never read. The walk in `crates/repark-spark/src/router/nested_ns.rs`
now carries, with each entry, the scopes it sits in: a join's right side is entered with the
join's left input pushed as a scope (`Next::Lateral`), and `feeds` turns an outer reference into
`Next::Outer`, which the walk resolves in the innermost scope holding the column and follows as
a column there (with the scopes outside it). Fail closed: an outer reference that resolves in no
scope refuses, and a node read whole (`Next::Whole`) inside a scope that holds an outer
reference (`refers_out`) reads every enclosing scope whole as well. The fix stays in
`nested_ns.rs` (796 lines as measured by `wc -l`, its test module from line 711; 729 at `75475b71`; ceiling 1000).

Build `maturin develop --profile ci` (190 s, load 2.96 at start); the verifier's
`cases_lat.py` and the four lateral cases of `cases_l.py` in UTC, New York and Kolkata, every
probe under `taskset -c 0-25` and a 40 G memory cap, load 5.26 at start:

| Cells | Spark 4.1.2 | main `c516c54a` | round 2 `75475b71` | this build |
|---|---|---|---|---|
| the verifier's 17 (9 shapes: comma, `CROSS JOIN LATERAL`, `JOIN LATERAL … ON true` (New York), array element, `ARRAY` column, temporary view, `BY NAME`, MERGE insert, MERGE update; New York and Kolkata) | session wall | UTC wall | UTC wall | refused, no snapshot |
| `lateral_join_outer_ref` in Kolkata, `lateral_outer_ref`, `merge_src_join_lateral` (New York, Kolkata) | session wall | UTC wall | UTC wall | refused |
| `lat_outer_ref_wrapped` (`a.c.v`) | session wall | raw error | refused | refused |
| `lat_ctrl_wall` (a wall through the same lateral; 3 zones) | UTC wall | UTC wall | UTC wall | UTC wall |
| every lateral cell in UTC | UTC wall | UTC wall | UTC wall | UTC wall |

This round's lateral cells (`cases_r4lat.py`, 12 shapes × 3 zones, Spark, main and the build):
a lateral returning only the wall sibling `a.m` of the cast stores the wall in every zone on
all three; returning the cast `a.c`, through `LEFT JOIN LATERAL`, or through a lateral of a
lateral, Spark stores the session wall, main the UTC wall, the build refuses outside UTC. The
other seven shapes (a scalar subquery of the outer column, `explode`, a derived table, `DISTINCT`
or a `UNION ALL` inside the lateral, a field of the outer column, a `CASE`) raise on main in
every zone (`repark_lateral_projection_hoist`, `Nested identifiers are not yet supported`,
`explode` unresolved, a physical-plan error) and are refused on the build outside UTC because
their type read cannot plan (fail closed, as before this round); no correct cell is lost.

Pins: `the_cast_walk_reads_only_what_reaches_the_column` gains the lateral on `INSERT` and as a
MERGE source (on-lineage, refuse, no snapshot) and the wall-sibling lateral (off-lineage,
stores). Both lateral pins fail on round 2's walk and under the verifier's L16 (a `Subquery`
node stops the walk); the wall-sibling pin fails if the left side is read whole. The two
fail-closed arms are reached by no plan RePark can run, so each is pinned on a synthetic plan
in the file's test module: `an_outer_reference_no_enclosing_join_resolves_fails_closed` fails
when an unresolved reference is skipped, and
`an_unmodelled_node_that_refers_out_reads_the_other_side_whole` (a `DISTINCT ON` holding the
reference, cross-joined to an instant cast) fails when the scope read is dropped.

**Correction to §13.1 (2026-10-21, same round).** §13.1 says the seven lateral shapes that raise
on main are refused on the build "because their type read cannot plan". Measured on the build in
UTC, planning alone: five of them plan and raise only when run (the scalar subquery of the outer
column and its wall twin in the physical planner; the derived table, `DISTINCT` and `UNION ALL`
inside the lateral in `repark_lateral_projection_hoist`), and two fail to plan (`explode`
unresolved; `a.c.v`, `Nested identifiers are not yet supported for OuterReferenceColumn`). The
five that plan are refused by the walk: the outer reference is followed, or, for the scalar
subquery, the whole read of the subquery holds an outer reference and reads the left side whole.
No cell changes; no correct cell is lost (main raises on all seven in every zone).

### §13.2 S1-3, pins per arm and every mutant (C-034)

`the_cast_walk_reads_only_what_reaches_the_column` (`crates/repark-spark/tests/ntz_nested_refuse_casts.rs`)
gains on its on-lineage list, each refusing in New York and Kolkata with no snapshot added:
`SELECT DISTINCT id + 1, <cast>, …` (L7), a derived table `… LIMIT 5` (L9), a derived table
read beside `row_number() OVER (ORDER BY id)` (L13), `VALUES` whose second row holds the cast
(L14), a top-level `ORDER BY id` (L8), a lateral whose outer relation shares its alias `b`
with an outer relation holding a wall (R5), and a lateral whose body joins a derived table
aliased `a` over the outer `a.c` to an inner lateral of `a.c` (R6; it plans, and raises in
the optimizer on main and in UTC, so the pin holds only the walk's refusal outside UTC). `a_frame_through_a_repartition_or_an_unmodelled_node_refuses`
(`crates/repark-spark/tests/ntz_nested_refuse.rs`) writes from two frame views over the cast,
one through `repartition(RoundRobinBatch(2))` (L18) and one through `distinct_on` (L5, read
whole). A frame through `sort` does not reach the walk as a `Sort` node (the view's plan is
inlined without it), so the sort pin is the top-level `ORDER BY`.

61 mutants: the verifier's round-0 set (V1 to V11; V6 is M6), round-1 set (M1 to M13; M9's
branch is gone, M8 and M10 split into a and b) and round-2 set (N1 to N10, L1 to L19), and
this round's R1 to R7 on the lateral walk. Each was applied alone to the round's head, the
suites `--test ntz_nested_refuse --test ntz_nested_refuse_casts --test timestamp_ns_wall_doors
--test timestamp_ns_nested_shapes`, `--lib nested` and repark-iceberg `--lib nested_ns_gate`
run (baseline 143 passed), and the source restored (`git status` clean of the source after
each pass). 158–211 s each, `taskset -c 0-25`, `CARGO_BUILD_JOBS=12`. The first pass left L8,
L11, L17, R5 and R6 alive; the ORDER BY and two alias pins were added and the survivors, L5
and L18 re-run alone (baseline 143 passed).

| Mutant | Change | Killed by (pins failing) |
|---|---|---|
| M1 | `Expr::Cast` arm removed | 6, `a_container_cast_of_an_instant_refuses_on_every_door_outside_utc` among them |
| M2 | `Expr::TryCast` arm removed | `a_container_cast_of_an_instant_refuses_on_every_door_outside_utc` |
| M3 | `PREPARE` parameter check inverted | 2, the `PREPARE` pins |
| M4 / V3 | the plain `UPDATE`'s cast test off | 2, `a_wall_or_the_hints_cast_stores_outside_utc_and_the_plain_update_refuses_the_cast` among them |
| M5, M5b, V4 | the hint names a deleting or overwriting action | `the_refusal_names_the_table_the_column_and_the_leaf` |
| M6 / V6 | the typing `SELECT` executed | `the_type_read_never_runs_the_source` |
| M7 | the leaf-cast function read as a cast | 4 |
| M8a | the whole read's temporary-view descent removed | `a_subquery_in_the_columns_expression_is_read_whole` |
| M8b | the lineage's temporary-view descent removed | 2 |
| M10, M10b | the subquery descent of the whole read, the lineage's subquery read | `a_subquery_in_the_columns_expression_is_read_whole` |
| M11 | the INSERT cast test removed | 4 |
| M12 | the UPDATE and MERGE cast test removed | 3 |
| M13 | the zoned-sub-expression arm restored | `the_plain_update_stores_a_wall_beside_a_zoned_sub_expression` |
| V1 | the zone condition inverted | 18 |
| V2 | `Etc/UTC` read as UTC | `only_a_utc_zone_lets_an_instant_through` |
| V5 | the nanosecond leaf takes the microsecond text | 9 |
| V7 | the outer `WITH` ignored | `the_type_read_follows_a_cte_skips_a_zoned_sibling_and_fails_closed` |
| V8 | the map arm of the zone test dropped | 2 |
| V9 | the MERGE source dropped from the typing `FROM` | 4 |
| V10 | a supply with no value feed refuses | 13 |
| V11 | `PREPARE` decided by the zone | 2 |
| N1 to N4, N6, N8 | the lineage read whole; a filter's predicate read; a join's right side read on the left; one union branch; an aggregate not read; a window expression not read | `the_cast_walk_reads_only_what_reaches_the_column` |
| N5 | a subquery's outer references not followed | `a_subquery_in_the_columns_expression_is_read_whole` |
| N7 | a values row not read | 2 |
| N9 | a projection's column references not followed | 4 |
| N10 | a column that resolves in no input not read | `a_column_that_resolves_in_no_input_reads_every_input_whole` |
| L1, L2, L4, L12 | a projection, an alias, a subquery alias stops; whole reads skipped | 4 to 8 each |
| L3, L6, L7, L9, L10, L13, L14, L15, L16 | the last union arm; a join's right side; `DISTINCT`; a limit; a filter; a window's input columns; values past the first row; group expressions; a `Subquery` node | `the_cast_walk_reads_only_what_reaches_the_column` |
| L5 | an unmodelled node treated as safe | `a_frame_through_a_repartition_or_an_unmodelled_node_refuses`, `an_unmodelled_node_that_refers_out_reads_the_other_side_whole` |
| L8 | a sort stops | `the_cast_walk_reads_only_what_reaches_the_column` (top-level `ORDER BY`; survived the first pass) |
| L18 | a repartition stops | `a_frame_through_a_repartition_or_an_unmodelled_node_refuses` |
| L19 | the view descent read whole | 2 |
| R1 | the join's left side not pushed as a scope | `the_cast_walk_reads_only_what_reaches_the_column` (both lateral pins, as a refusal of the wall-sibling control) |
| R2 | an outer reference not followed | `the_cast_walk_reads_only_what_reaches_the_column` |
| R3 | an outer reference that resolves nowhere treated as safe | `an_outer_reference_no_enclosing_join_resolves_fails_closed` |
| R4 | a whole read in a scope ignores outer references | `an_unmodelled_node_that_refers_out_reads_the_other_side_whole` |
| R5 | an outer reference resolved in the outermost scope first | `the_cast_walk_reads_only_what_reaches_the_column` (alias-shadow lateral; survived the first pass) |
| R6 | a followed outer reference keeps the scopes inside the one it resolves in | `the_cast_walk_reads_only_what_reaches_the_column` (nested alias-shadow lateral; survived the first pass) |
| R7 | a lateral reads the whole left side | `the_cast_walk_reads_only_what_reaches_the_column` (the wall-sibling lateral) |
| L11 | the view scan maps the column with `projection.first()` | equivalent: every scan the type read reaches is built with `projection: None` (DataFusion 54.1.0 `datafusion-sql` `relation/mod.rs` and `SessionContext::read_table` call `LogicalPlanBuilder::scan(…, None)`), and only the optimizer's projection pushdown sets one, which the type read never runs (`planned()` keeps `DataFrame::logical_plan()`; no router path calls `into_optimized_plan`); the verifier's instrumented build saw no scan projection in 190 plans |
| L17 | the step bound removed | equivalent below the bound: the walk visits each (node, column) once per root, so it ends without the bound, and the bound changes an answer only on a plan of more than 65,536 such steps, where it refuses; no plan the probes build comes near it |

### §13.3 S1-2 and S2-1, the over-refusal list (C-035)

No rule changes; the list is stated with its counts in R-011's "Over-refusal, by design" and in
the v1.5.6 notes line. Re-measured on this round's build and on main `c516c54a`, every probe
under `taskset -c 0-25` and a 40 G memory cap:

| Class | Cells | Measured |
|---|---|---|
| the verifier's `cases_o.py`: a scalar subquery read whole (2 shapes), aggregate expressions read whole (3), a grouping key (1), a `CASE`/`IF` condition or a sibling field of the same `named_struct` (3); New York and Kolkata | 18 | main stores the wall in all 18 (the value Spark stores); the build refuses all 18; UTC: 15 cells, the same on both |
| an `IN`/`EXISTS` subquery in the column's expression (`IF(EXISTS (…), named_struct('v', w, 'n', 1), NULL)`, New York) | 1 | §12.3; the 4 scalar-subquery cells above belong with it |
| `PREPARE` | 17 | C-025 |
| zero-offset zones, the 28-door matrix (`ntz_nested`, 352 cells per zone) | 126 correct + 54 raw errors per zone | `Europe/London` 180 moved (126 + 54), `Etc/GMT` 180 moved (126 + 54); the verifier's `Europe/London` and `Atlantic/Reykjavik`: 126 + 54 each |
| zero-offset zones, the verifier's five instant shapes (`cases_z.py`) | 5 per zone | refused in `Etc/GMT`, `Africa/Abidjan`, `Atlantic/Reykjavik`, `Europe/London`, `Etc/UTC`, `Etc/Universal`, `Etc/Zulu` (main stores the wall in each); stored in `GMT` and `+00:00` on both |

The zone runs took 29 s (build) and 65 s (main), load 6.55 and 5.48 at start. The owner decides
whether the walk narrows (a scalar subquery by its output column, an aggregate by the
producing expression, a condition and sibling fields skipped, `IN`/`EXISTS` bodies skipped) and
whether a fixed zero-offset zone counts as UTC.

### §13.4 S3-1, S3-3 and S3-4, stated (C-036 to C-040)

S3-1: `nested_ns.rs` is 729 lines at `75475b71` (`git show 75475b71:crates/repark-spark/src/router/nested_ns.rs | wc -l`)
and 796 at this round's head; the fix-round-2 figure of 726 was wrong. S3-4: C-014 is not
edited; C-036 to C-039 carry each verifier round's outcome as a dated row. S3-3: the premise pin
is not re-planned through `planned()`; C-040 states why `session.sql` reaches the same planner
on the same text.

### §13.5 S3-2, the memory incident, carded

`LATERAL (SELECT a.c AS s FROM ice.ns.src b WHERE b.id = a.id)` over a one-row source grows
past 100 GB resident before any result on main `c516c54a` (the verifier: 112 GB; this round,
under a 40 G scope cap in UTC, stopped by the OOM killer after 166 s, load 10.17 at start). It
predates this unit and is not its gate (in UTC the gate does not run; outside UTC the type read
plans and never runs the source, C-018). Carded as LATERAL-CORRELATED-MEMORY-1
(`task/roadmap/mid-term/lateral-correlated-memory-1-card-2026-10-21.md`); the owner schedules.
No fix in this round.

### §13.6 Owner ruling 1: subqueries in the column's expression (C-041)

`feeds` (`crates/repark-spark/src/router/nested_ns.rs`) no longer reads an `Exists`,
`InSubquery` or `SetComparison` body, nor follows its outer references; `Expr::apply` still
visits the left operand. A `ScalarSubquery` becomes `Next::Lateral(body, 0, input)`: the walk
enters the subquery's plan at its one output column with the node's input as the scope, so an
`Expr::OuterReferenceColumn` in the output is followed back into the input (the C-033
mechanism) and one in the subquery's `WHERE` is not reached. Build `maturin develop --profile
ci` (190 s, load 20.13 at the end, other work on the box); probes under `taskset -c 0-25` and a
40 G cap:

| Cells | Spark 4.1.2 | main `c516c54a` | round 3 before the ruling | this build |
|---|---|---|---|---|
| `cases_o.py` scalar subquery, cast only in its `WHERE` (New York, Kolkata) | the wall | the wall | refused | the wall |
| `cases_o.py` scalar subquery, cast in a column it does not return (New York, Kolkata) | the wall | the wall | refused | the wall |
| `cases_o.py`, the other 7 shapes (New York, Kolkata) | the wall | the wall | refused | refused (ruling 2) |
| `IF(EXISTS (… <cast> …), named_struct('v', w, 'n', 1), NULL)` (New York, Kolkata) | the wall | the wall | refused | the wall |
| `IF(id IN (… <cast> …), …)` (New York, Kolkata) | the wall | raw error | refused | raw error, as main |
| a scalar subquery returning the cast; the outer column followed back out (New York, Kolkata) | session wall | UTC wall; raw error | refused | refused |
| every cell in UTC | as main | — | as main | as main |

The Rust pin adds four storing writes (the two `cases_o.py` scalar shapes, a correlated scalar
subquery whose outer reference feeds only its `WHERE`, the `EXISTS` in the value); each fails on
the round-2 walk (`git show HEAD:…nested_ns.rs`, the pin red with the refusal text).

### §13.7 Owner ruling 3: the pure UTC aliases (C-042)

`session_is_utc` (`crates/repark-spark/src/router/nested_ns.rs`) reads the canonical session
zone through `names_utc`, which matches the one set `UTC_ALIASES` (21 ids). The session's
canonicaliser maps `Z`, `GMT`, `UT`, `UTC` and the `GMT±`/`UTC±` offsets to `UTC` or an
offset and leaves the others as written, so the set lists every alias in both forms and the
unit test checks each name raw and through `SessionTimeZoneConfig::set_zone`. Build 215 s
(load 30.5, other work on the box). Probes under `taskset -c 0-25` and a 40 G cap:

| Cells | Spark 4.1.2 (the verifier's) | main `c516c54a` | before the ruling | this build |
|---|---|---|---|---|
| five instant shapes in `Etc/UTC`, `Etc/GMT`, `Etc/Universal`, `Etc/Zulu` (20) | the wall | the wall | refused | the wall |
| the same in `Africa/Abidjan`, `Atlantic/Reykjavik`, `Europe/London` (15) | the wall | the wall | refused | refused |
| the same in `GMT`, `+00:00` (10) | the wall | the wall | the wall | the wall |
| 28-door matrix, `Etc/GMT` (352) | — | — | 180 moved | 0 moved (58 s) |
| 28-door matrix, `Europe/London` (352) | — | — | 180 moved | 180 moved: 126 correct, 54 raw errors |

### §13.8 After the rulings: cells and mutants (C-043)

Final build (ruling 3's `maturin develop --profile ci`, 215 s); `probe.py run` 270 s and
`probe.py attack` 8 s, load 16.66 and 23.84 at start (other work on the box), 40 G cap:

| Probe | Verifier (round 2) | Fix round 3 before the rulings | Final head |
|---|---|---|---|
| door matrix, 3168 cells, moved against main | 360, all refused | 360, all refused; 0 differ from the committed head file | 360, all refused; 0 differ |
| named-shape probe, 84 cells, moved against main | 48 (38 + 10) | 48; 0 differ | 48; 0 differ |
| main re-run against the committed main files | 0 differ | 0 differ | — |

The matrix and the probe moved only by lateral cells would have changed; they hold none, so
they did not move. The lateral cells are §13.1's (17 refused, the control stores).

The targeted mutant pass ran each mutant alone over the same suites (baseline 144 passed, one
more test than §13.2: the alias unit test), 170–490 s each under box load up to 50:

| Mutant | Result |
|---|---|
| M1, M12, V1, N10, L5, L12, L16, L19, R1 to R7 | killed, as in §13.2 (the renamed subquery pin among the killers of M1, M12, R2, R7) |
| M8a, the whole read's temporary-view descent | survived the first pass; killed by `a_frame_through_a_repartition_or_an_unmodelled_node_refuses`, which adds a `DISTINCT ON` frame over a SQL temporary view |
| M10, the whole read's subquery descent | equivalent on stored values (C-043) |
| Q1, a scalar subquery not walked | `a_scalar_subquery_is_read_by_its_output_and_a_boolean_subquery_is_skipped` |
| Q2, a scalar subquery walked without its scope | equivalent on stored values (C-043) |
| Q3, a scalar subquery read whole (round 2) | the same pin (its storing cells) |
| Q4, an `EXISTS` body read whole (round 2) | the same pin |
| Z1, `Europe/London` read as UTC; Z2, `Africa/Abidjan` and `Atlantic/Reykjavik` read as UTC | `only_a_pure_utc_alias_counts_as_utc`, `only_a_utc_zone_lets_an_instant_through` |
| Z3, `Etc/UTC` dropped from the set | `only_a_utc_zone_lets_an_instant_through` and, once its list is written out, `only_a_pure_utc_alias_counts_as_utc` |
| Z4, the raw `UTC`/`+00:00`/`-00:00` match restored | `only_a_utc_zone_lets_an_instant_through` |
| L11, L17 | equivalent (§13.2) |

The other mutants of §13.2 mutate code and pins the rulings did not touch; their kills stand
from that pass on `a0baf803`.

`crates/repark-spark/src/router/nested_ns.rs` is 868 lines after the rulings (`wc -l`; 796 at
`a0baf803`, C-040), its test module included; ceiling 1000.

### §13.9 The round's gates and cost (C-044)

Final code head `f795a0ff`; every heavy command under `taskset -c 0-25` with
`CARGO_BUILD_JOBS=12`; the box carried other work, so the load at each start is given.

| Gate | Result | Wall | Load at start |
|---|---|---|---|
| `cargo fmt`, `python3 scripts/sync_map_md.py`, `python3 scripts/check_model_ban.py`, no comment line added in a code file | clean on every commit | — | — |
| `make ci` on every commit | exit 0 | 89–485 s | 2.2–51.6 |
| `maturin develop --profile ci` | ok | 214 s | 17.25 |
| `make verify` | exit 0; 7746 tests run, 7746 passed (4 slow), 97 skipped | 276 s | 7.76 |
| `make rust-panic-ban` | exit 0 | 1 s | 12.73 |
| facade suite (`-n 16 --dist loadgroup --maxschedchunk=8`) | 20227 passed, 1 failed, 485 skipped, 151 xfailed | 540 s | 12.73 |
| live leg, the module alone (`REPARK_PARITY_LIVE=1`, 40 G cap) | 67 passed; `test_live_spark_stores_the_recorded_session_wall` PASSED | 98 s | 13.81, no other JVM |
| live leg co-collected after `tests/parity` and `tests/writes` (40 G cap) | 5924 passed, 13 skipped, 3 xfailed, 0 failed; the live cell PASSED; no DRIFT | 902 s | 6.09, one other JVM on the box |

The facade failure is `perf/test_t2_spill_reach.py::test_sort_merge_join_spills_under_small_fair_pool`
(`Resources exhausted` in `ExternalSorterMerge` under a 64 MiB fair pool), carded as
SPILL-REACH-FAIR-POOL-FLAKE-1 on 2026-10-14. Run alone, it failed 1 of 8 runs on main
`c516c54a` and 1 of 10 runs on this head; it touches no write and no zone. It was not re-run
into the gate.

Cost, main `c516c54a` against this build. The first pass (verifier's `cost.py`, 3 rounds, each
the median of 10 runs after 3 warm-ups, load 5.3–6.2) read 1.196x for the UTC plain `UPDATE`,
1.080x for the UTC plain `INSERT` and 1.082x for the New York nested `INSERT`. Main's own rounds
spread 41.8–49.5 ms on the first of these, and in UTC the round's code runs nothing but the
alias lookup. A focused interleaved run settles them: 6 rounds of main and the build
alternating, each the median of 50 runs after 3 warm-ups, load 3.96–5.85; the ratio is the
median of the six:

| Write | UTC | New York |
|---|---|---|
| plain `INSERT` | 0.987 | 1.034 |
| plain `UPDATE` | 0.993 | 1.011 |
| nested-NTZ `INSERT` | 1.002 | 1.038 |
| nested-NTZ `UPDATE` | 0.981 | 1.032 |

Main `c516c54a` is the baseline the verifier used. The build also carries main's commits up to
`d6ee0411`.

## §14 Fix round 4: what each finding became

Verifier round 3 returned FAIL on PR #1124 at `d9943218`: two S1 findings, three S2 and four S3
(C-045). The code held on every cell the verifier measured. The round adds one pin, strips the
unit's code comments, corrects one count, merges main and records the gates; no rule in
`nested_ns.rs` changes.

### §14.1 The verdict (C-045)

| Finding | One line | Answered by |
|---|---|---|
| S1-1 | Mutant M10 survives all 144 pins; a scalar subquery returning the container cast under an `Unnest` stores the UTC wall in New York | C-046 |
| S1-2 | The unit adds 153 `///` lines to six Rust files and rewords 10 of its own | C-047 |
| S2-1 | The sibling-field shape `named_struct('v', w, 'n', size(CAST(array(x) AS ARRAY<TIMESTAMP_NTZ>)))` in a scalar subquery has no count (2 cells) | C-048 |
| S2-2 | One armed facade run hung at 99 % and no test was named | C-049 |
| S2-3 | The New York nested `INSERT` reads 1.090 (6 x 10) and 1.050 (6 x 50) against the 1.05x gate | C-049, for the owner |
| S3-1 | Q2 is equivalent on stored values, and a pin on `q2_lat_shadow` would kill it | not in scope |
| S3-2 | W1, W2 and W3 survive and are equivalent on stored values | not in scope |
| S3-3 | The "34 mutants not re-run" are 42 names, 39 runnable, all killed | stated |
| S3-4 | The builder's `.config("spark.sql.session.timeZone", z)` rejects five spellings that `SET TIME ZONE` accepts (main and head alike) | not this unit's |

### §14.2 S1-1, the M10 pin (C-046)

The verifier built M10 into a native module and ran the plan on three doors in New York
(`ice.ns.src (id INT, x TIMESTAMP, w TIMESTAMP_NTZ)`, target `ice.ns.t (id INT, st STRUCT<v:
TIMESTAMP_NTZ, n: INT>, k INT)`):

| Engine | Stored `st.v` |
|---|---|
| Spark 4.1.2 | `1767305045123456` (New York wall) |
| main | `1767323045123456` (UTC wall) |
| head `d9943218` | refused, no snapshot |
| head with M10 | `1767323045123456` (UTC wall) |

The pin reaches `Unnest` from Rust: the frame is `SELECT id, (SELECT CAST(named_struct('v', x,
'n', 1) AS STRUCT<v: TIMESTAMP_NTZ, n: INT>) FROM ice.ns.src LIMIT 1) AS st, array(0) AS k
FROM ice.ns.src`, unnested on `k` with `DataFrame::unnest_columns` and registered as a temporary
view. The walk reaches the node through the view's scan, reads it whole, and finds the cast only
inside the scalar subquery, which `apply` does not enter. The earlier pin
(`a_frame_through_a_repartition_or_an_unmodelled_node_refuses`) puts the cast directly in the
projection, where `apply` finds it too.

Kill, recorded in `task/wo/ice-ntz-nested-refuse-1/evidence/m10-kill-round-4.txt`: with M10
applied by `sed`, the pin fails (both zones: the cast row stored, one snapshot); the
`ntz_nested_refuse` binary fails that one test (8 of 9 pass), `ntz_nested_refuse_casts` passes
10 of 10, and `--lib nested` (the routes module and the router's unit tests) passes 102 with 1
ignored (IDENT-STRUCT-KW-1). The file was restored with `git checkout`.
