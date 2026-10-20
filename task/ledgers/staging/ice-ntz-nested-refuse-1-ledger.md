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

**Why.** In a session whose zone is not UTC, an instant written into a nested microsecond
`TIMESTAMP_NTZ` leaf stored the session wall through field assignment and the UTC wall through
every other door, so one table held two walls for one input. Two folds that stored the right
wall are parked (#1037, #1038); the lift is a 1.5.7 unit. Ruling T-3: refuse with a hint now,
as R-015 does for a nested `timestamp_ns` leaf.

## PROPOSITION LEDGER — ICE-NTZ-NESTED-REFUSE-1 — 2026-10-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | One walk, one supply model and one refusal serve both leaf units: `NaiveLeaf::{Nanos, Micros}` parameterises the walk and the text; the supply functions answer the supply and the feeds; R-015's decisions and text are unchanged. The zone condition and the type read sit in the router: UTC is the canonical id `UTC`, `+00:00` or `-00:00`; a `PREPARE` is decided as if the zone were not UTC; an `INSERT` source is typed with its outer `WITH`; a value the router cannot plan refuses. | The gate's unit tests; R-015's three pin files green unchanged but for one dated row (C-008); the zone and type-read pins. | **PROVEN** | §1. `nested_ns_gate.rs` (10 unit tests), `timestamp_ns_nested_shapes.rs`, `nested_ns_routes.rs`, `only_a_utc_zone_lets_an_instant_through`, `a_prepared_write_of_an_instant_refuses_in_a_utc_session_too`, `the_type_read_follows_a_cte_skips_a_zoned_sibling_and_fails_closed`. |
| C-002 | Outside UTC every door refuses a value whose type holds a zoned timestamp for a column with a nested microsecond `TIMESTAMP_NTZ` leaf, before any file is written, field assignment included. | Five shapes × eighteen doors × two zones at the Rust door, no file, no snapshot; the five field doors; the six doors of the old pin in three zones; a frame written by name. | **PROVEN** | §2. `every_door_refuses_an_instant_outside_utc_and_writes_nothing`, `a_field_assignment_of_an_instant_refuses_too`, `a_nested_microsecond_ntz_field_refuses_an_instant_outside_utc`, `a_frame_written_by_name_is_decided_by_the_column_of_that_name`. |
| C-003 | A UTC session refuses nothing and stores what main stored. | The old pin's six doors in UTC; the zone spellings. | **PROVEN** | §2. The UTC walls equal main's (`1767323045123456` on all six doors). |
| C-004 | A naive value still stores outside UTC: a wall literal or column stores itself, and `CAST(<instant> AS TIMESTAMP_NTZ)` stores the session wall Spark stores, on every door but the one of C-005. Branch and WAP routes type their value. | The wall and cast cells at the Rust door; the branch and WAP routes with a wall; the matrix (C-011). | **PROVEN** | §2, §3. `a_wall_or_the_hints_cast_stores_outside_utc_and_the_plain_update_refuses_the_cast`, `a_branch_or_wap_write_of_a_wall_passes_the_gate`. |
| C-005 | Correction 1 of the sketch: the plain `UPDATE` that assigns a struct above the leaf stores the UTC wall for `CAST(<instant> AS TIMESTAMP_NTZ)` on main, so there a value with a zoned sub-expression refuses; the field assignment of the same cast stores the session wall. | The cast cells on that door, literal and column, both modes, two zones. | **PROVEN** | §3: 8 matrix cells and 8 raw errors move to the refusal; the field assignment pin stores `1767305045123456` in New York. |
| C-006 | One text: R-015's class, first sentence and SQLSTATE, naming the table, the column and the leaf path, with one hint that names no dropping, truncating, overwriting, recreating, deleting or replacing action (ruling 76(a)). | The whole-text pin. | **PROVEN** | §4. `the_refusal_names_the_table_the_column_and_the_leaf`. |
| C-007 | The carriers and routes R-015 lists are decided as the write they run (`EXPLAIN ANALYZE`, `PREPARE`, `CREATE TABLE … AS INSERT`, `WITH … INSERT`, an unparsed statement, a branch, `branch_main`, WAP id and branch); a plain `EXPLAIN` runs; a statement that does not supply the column runs and keeps the stored wall; maintenance calls run. | The internal route pins; the unsupplied-statement pin. | **PROVEN** | §2. `a_branch_or_wap_write_of_an_instant_refuses_and_writes_nothing`, `a_statement_that_wraps_a_write_of_an_instant_is_decided_as_that_write`, `a_statement_that_does_not_supply_the_column_runs_and_carries_its_rows`. |
| C-008 | Controls do not move: a nested `TIMESTAMP` or `timestamptz_ns` leaf, a top-level `TIMESTAMP_NTZ` column, and a nested `timestamp_ns` leaf (R-015's text first). R-015's control row for a nested `TIMESTAMP_NTZ` leaf is re-pinned to R-011's refusal with a dated note. | The control pin; the matrix's 1320 control cells. | **PROVEN** | §5. `a_zoned_leaf_and_a_top_level_ntz_column_are_not_guarded`, `a_nested_zoned_or_microsecond_leaf_is_not_guarded` (re-pinned). |
| C-009 | Every DataFrame writer (append, `overwrite(condition)`, `overwritePartitions`, `insertInto` both modes, `saveAsTable` append, a cached frame, a temporary view) refuses an Arrow frame whose nested leaf is zoned (struct, dictionary child, run-end-encoded child, list, map value) outside UTC and writes no file; a naive leaf stores its wall; a UTC session stores the UTC wall. | The facade module. | **PROVEN** | §6: 56 facade tests. |
| C-010 | Spark 4.1.2 stores the session wall of an instant, and of the hint's cast, on twelve of #1037's doors in three zones (174 cells, recorded and re-derived live); RePark refuses each instant cell outside UTC and answers Spark's value in every other cell but C-005's. | The recorded fixture, the always-run comparison, the live re-derivation alone and co-collected after `tests/parity`. | **PROVEN** | §6. `ice_ntz_nested_refuse_1_spark.json`; `test_repark_refuses_where_spark_stores_an_instant_and_answers_spark_elsewhere`, `test_live_spark_stores_the_recorded_session_wall`. |
| C-011 | Against main the door matrix (28 doors, both row-level modes, five sources, three zones, three schemas: 1980 cells) moves 192 cells, every one to the refusal: 176 instant cells (108 UTC wall, 32 session wall, 36 raw errors), 8 `CAST` cells on the plain `UPDATE` door that stored the UTC wall, 8 raw errors on the same door with no `WHERE`. No other cell moves. | `probe.py` on main and on head; the cell diff. | **PROVEN** | §5. Main: `task/wo/ice-ntz-nested-refuse-1/evidence/repark-main-c516c54a.jsonl`; head: `repark-head.jsonl` beside it. |
| C-012 | Eight hand mutants are each killed by a named pin. | §7. | **PROVEN** | §7. |
| C-013 | The unit's gates are green with real exit codes. | §8. | **PROVEN** | §8. |
| C-014 | A scoped Opus verifier on a fresh agent finds no S0 or S1 defect. | The verifier's report. | **OPEN** | Verifier pending; the hand-back lists what to attack first. |

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

Sketch §3. The pin writes `UPDATE t SET st = named_struct('v', CAST(<instant> AS
TIMESTAMP_NTZ), 'n', 1) WHERE id >= 0` and expects the refusal; it writes
`UPDATE t SET st.v = CAST(<instant> AS TIMESTAMP_NTZ) WHERE id >= 0` and expects the session wall.

## §4 The text

``[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table
`<catalog>`.`<namespace>`.`<table>`: Cannot safely cast `<column>`.`<leaf>` to "TIMESTAMP_NTZ".
A TIMESTAMP value is not written into a nested TIMESTAMP_NTZ field while the session time zone
is not UTC: write the field from CAST(<value> AS TIMESTAMP_NTZ) to store the session wall time
(in an UPDATE, assign the field itself), or set spark.sql.session.timeZone to UTC for the
statement. SQLSTATE: KD000``

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
