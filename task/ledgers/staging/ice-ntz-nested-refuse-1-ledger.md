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
residue, not gated.

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

