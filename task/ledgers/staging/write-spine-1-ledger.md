# Unit ledger — WRITE-SPINE-1 steps (1) and (3) · shared kernel names, a fail-closed lookup, the door matrix enumerated from the code

**Date:** 2026-10-24 · **Branch:** `feat/write-spine-1-steps-1-3a` · **Base:** `7a789fc` ·
**Model:** Claude Opus 5.5 (`claude-opus-5-5`), lane A · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: release 1.5.6.

**Order:** [../../wo/write-spine-1/order-steps-1-3.md](../../wo/write-spine-1/order-steps-1-3.md);
card [WRITE-SPINE-1](../../roadmap/mid-term/write-spine-1-card-2026-10-22.md); door table
[crate-boundary-eval-2026-10-22.md](../../wo/write-spine-1/crate-boundary-eval-2026-10-22.md) §3.
**This ledger closes when the pull request of steps (1) and (3) merges.**

## Step 1a, the names (2026-10-24)

`crates/repark-common/src/udf_names.rs` holds seven `pub const` names. `repark-functions`
(`timestamp_ntz_cast.rs`, `timestamp_ns_cast.rs`, `cast_map.rs`, `zone_seam.rs`) and
`repark-iceberg` (`write/ntz_store.rs`, `write/store_cast.rs`, `write/struct_pairing.rs`)
re-export them under their existing identifiers. `cargo build --workspace`, `make rust-clippy`
and `make rust-panic-ban` exit 0. The evidence grep for C-001, on the step 1b head:

```
$ grep -rn -E '"__repark_(cast_timestamp_ntz|cast_timestamp_ns|cast_timestamptz_ns|cast_map|zone_seam|store_int_guard|store_pair)__"' crates/
crates/repark-common/src/udf_names.rs:1:pub const TIMESTAMP_NTZ_CAST_NAME: &str = "__repark_cast_timestamp_ntz__";
crates/repark-common/src/udf_names.rs:2:pub const TIMESTAMP_NS_CAST_NAME: &str = "__repark_cast_timestamp_ns__";
crates/repark-common/src/udf_names.rs:3:pub const TIMESTAMPTZ_NS_CAST_NAME: &str = "__repark_cast_timestamptz_ns__";
crates/repark-common/src/udf_names.rs:4:pub const CAST_MAP_NAME: &str = "__repark_cast_map__";
crates/repark-common/src/udf_names.rs:5:pub const ZONE_SEAM_NAME: &str = "__repark_zone_seam__";
crates/repark-common/src/udf_names.rs:6:pub const STORE_INT_GUARD_NAME: &str = "__repark_store_int_guard__";
crates/repark-common/src/udf_names.rs:7:pub const STORE_PAIR_KERNEL_NAME: &str = "__repark_store_pair__";
```

On the base the same grep finds ten definitions: two copies each of the three timestamp names
(`ntz_store.rs:15-17` and `timestamp_ntz_cast.rs:20`, `timestamp_ns_cast.rs:33-34`) and one each
of the other four. Test SQL text that names a kernel inside a longer string is not a definition
and is unchanged.

## Step 1b, fail closed (2026-10-24)

`zone_store_frame` in `crates/repark-iceberg/src/write/ntz_store.rs` looks the store kernel up
through `registered_store_kernel`. When `wall_kernel_reads` holds for the target and
`store_kernel_udf_name` names a kernel the session does not hold, the lookup returns
`DataFusionError::Internal` with the kernel name, and the expression map collects into
`Result<Vec<Expr>>`. A target that needs no kernel still answers `Ok(None)`, so every other arm
is unchanged.

**Correction of the order's wording, measured (2026-10-24).** The order's §2 describes the test
as "an `INSERT … BY NAME` of a wall into a `TIMESTAMP` column". That shape never reaches the
lookup: `wall_kernel_reads(Timestamp(µs, None), Timestamp(µs, Some(_)))` is false, and the
statement takes the plain-cast arm. Measured on the base code in New York, with and without
`__repark_cast_timestamp_ntz__` registered, `INSERT INTO t BY NAME SELECT id, w AS ts` of the
wall `2026-01-02 03:04:05` into a `TIMESTAMP` column stores `1767341045000000` both times (the
session instant, Spark's answer). The shape that reaches the lookup the order's mechanism names is
the reverse: an instant into a `TIMESTAMP_NTZ` column. Measured on the base code in New York:

| statement | kernel registered | stored |
|---|---|---|
| `INSERT INTO t BY NAME SELECT id, x AS ts FROM src` (`x TIMESTAMP` → `ts TIMESTAMP_NTZ`) | yes | `1767305045000000` (the New York wall) |
| the same | no | `1767323045000000` (the UTC wall) |
| `INSERT INTO t SELECT id, x FROM src` (positional) | no | `1767305045000000` (the analyzer path, no lookup) |

The pin therefore writes an instant into a `TIMESTAMP_NTZ` column BY NAME. The orchestrator
confirms or rules otherwise.

The red-then-green record, `cargo test -p repark-spark --test store_kernel_registry`:

```
head (ntz_store.rs changed):
test missing_wall_kernel_refuses_instead_of_storing_utc ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
base lookup (ntz_store.rs stashed, the test kept):
test missing_wall_kernel_refuses_instead_of_storing_utc ... FAILED
thread 'missing_wall_kernel_refuses_instead_of_storing_utc' panicked at crates/repark-spark/tests/store_kernel_registry.rs:99:10:
a missing wall kernel refuses the write: ()
test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out
```

## Step 3a, the enum (2026-10-24)

`crates/repark-spark/src/write_door.rs` holds `WriteDoor` (eighteen variants, one per row of the
evaluation's table), `ALL`, `id`, `is_spark_door` and `door_of`, whose `match` lists all 135
`sqlparser` 0.62 `Statement` variants with no wildcard arm. `router.rs` (968 → 971 lines) tags
the door in `execute_inner`: D5 on the `REPLACE WHERE` intercept, D3 on the `BY NAME` intercept,
and `door_of` once on the parsed statement; the tag is one `tracing` debug event and changes no
value. `repark-python` exposes `write_doors()`; `python/repark/tests/timestamps/` gains
`_write_spine_1_doors.py` (41 spelling names on 18 doors) and
`test_write_spine_1_enumeration.py`.

Door count: the enum holds 18; the evaluation's table holds 18.

```
cargo test -p repark-spark --lib tests::write_door
test tests::write_door::write_door_ids_are_the_evaluation_table_ids_and_the_first_ten_are_spark_doors ... ok
test tests::write_door::door_of_answers_none_for_statements_that_write_no_new_values ... ok
test tests::write_door::door_of_names_the_door_each_parsed_spark_write_takes ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 2832 filtered out

.venv/bin/python -m pytest python/repark/tests/timestamps/test_write_spine_1_enumeration.py -q
SKIPPED [1] test_write_spine_1_enumeration.py:51: could not import '_write_spine_1_spellings'
3 passed, 1 skipped
```

**Choices this step made where the order is silent (for the orchestrator to confirm):**

1. The order says the tag goes into "the existing trace/statement record". The Spark router has
   no statement record on main, so the tag is a `tracing` debug event (target
   `repark_spark::write_door`, field `door`), the crate's existing trace. Step 3b needs a way to
   read it; that is open.
2. `INSERT … BY NAME` (D3) and `INSERT … REPLACE WHERE` (D5) are pre-parse intercepts: the
   statement `door_of` receives never carries them. The router tags them at their intercepts.
3. `door_of` sees the statement and the options only. A positional `INSERT` that
   `execute_insert_routed` sends to the owned append or the session-write-conf append (D2), or to
   the schema-evolution `BY NAME` route (D3), is tagged D1; an `INSERT` into a Postgres table is
   tagged D1 although it is not an Iceberg door.
4. A `MERGE` returns one door: the door of its first value-writing clause. An update whose target,
   after the target alias or table name, has more than one part is D9 (a field); otherwise D8
   (whole column, and `SET *`). The rule reads names only, so a top-level non-struct column update
   is D8.

## Mutants and revert checks (2026-10-24)

| mutant | change | red test | answer |
|---|---|---|---|
| (b) | `WriteDoor::D9MergeUpdateField` removed from `ALL` (length 17), native module rebuilt | `test_every_spelling_names_a_door` (`assert ['D9'] == []`), `test_the_spark_flag_separates_the_spark_doors_from_the_native_and_streaming_doors` | red; restored |
| (c) | the step 1b `ntz_store.rs` change reverted (the base lookup) | `missing_wall_kernel_refuses_instead_of_storing_utc` (`a missing wall kernel refuses the write: ()`) | red; restored |
| (d) | `insert_by_name` deleted from D3 in `_write_spine_1_doors.py` | `test_every_write_door_has_a_spelling` (`assert ['D3'] == []`) | red; restored |
| (a) | `door_of` returning `None` for one writing arm | waits for the matrix (step 3b) | not run. It ran in step 3b and is red (see the step 3b mutant table below). |

## Round 2: the orchestrator's rulings on Q1 to Q6 (2026-10-24)

Recorded as given by the orchestrator (Claude, claude-fable-5-1) after it reproduced the three
commits of round 1.

- **Q1:** the C-002 shape is accepted as measured; the order's §2 wording was wrong. The two
  further fall-throughs of `zone_store_frame` are C-007 and C-008 below, OPEN, not fixed here.
- **Q2, Q4, Q5:** a `tracing` event is not a readable carrier. Build (a) a static classifier
  for Python, `write_door_of(sql, options)`, which runs the router's pre-parse intercepts and
  then `door_of` (the parsed-shape reading), and (b) a runtime record per Spark session,
  `last_write_doors(session)`, cleared at the start of each statement and written at the real
  branch points. The `tracing` event stays.
- **Q3:** D3 and D5 tagged at their intercepts, for both readings.
- **Q6:** lane B's `SPELLINGS` is a tuple of 41 frozen pydantic entries (`name`, `door`,
  `template`, `reach`, `row_level_mode`). The corpus test keeps its skip until that branch
  merges. Run here against lane B's file at `d859b91` (copied to a scratch directory on
  `PYTHONPATH`, not merged): `5 passed`.
- **Door-table correction (C-009):** `repark.sql()` runs a bare DataFusion session in
  `repark-core` and never enters `repark-sql` (`repark-python` has no edge to `repark-sql`;
  ARCHITECTURE.md line 65). D14 is re-described as "bare-session SQL via `repark.sql()`
  (repark-core, DataFusion default dialect, fork provider `insert_into`; ruling ZS-5 native
  semantics)": reachable from Python, invisible to the runtime record, classified statically.
  D15, D16, D17, D19 and D20 are `repark-sql` Rust-library doors (reach `rust_api`), measured by
  Rust tests only; D18 and D21 are unchanged. The enum is not renamed.

## Round 2 build (2026-10-24)

- `write_door_of_sql` (`write_door.rs`) and the pyfunction `write_door_of`: map-cast rewrite,
  schema-evolution strip, D5 for `REPLACE WHERE`, D3 for `BY NAME`, else `door_of`. A Postgres
  `INSERT` answers D1, because no catalog is known statically.
- `router/door_tag.rs`: the per-session record (a `SessionConfig` extension that
  `SparkExtension` installs), `clear` at the top of `execute_in_session`, `record` at the branch
  points: D5 and D3 at the intercepts; D6 at the CTAS arm; in `execute_insert_routed`, D3 on the
  schema-evolution route, D4 before the overwrite, D2 on the owned append, on write options and
  on the session-write-conf append, D1 on the plain route, and no record on the Postgres route;
  D10 before `execute_predicate_dml` runs an `UPDATE`; and in `merge/nested_assign.rs` one entry
  per arm (D7 insert; D8 for `SET *` and a whole struct column on the `paired_assignment_cast_sql`
  path; D9 for a field on the `paired_nested_cast_sql` path and for a top-level column the
  executor casts through `store_kernel_sql`). `router.rs` is 979 lines. *(Wording corrected in
  round 4: the split is the paired-cast path versus the kernel path, not struct versus field;
  a container column takes the paired cast and records D8.)*
- `crates/repark-sql/tests/write_door_native.rs`: one test per Rust-library door (D15 to D20),
  each reading the rows back; `_write_spine_1_doors.py` marks the seven `native_*` spellings of
  D15 to D20 `rust_api` and names the test functions; the enumeration test checks that each
  name exists.

```
cargo test -p repark-spark --test write_door_record
test every_spark_door_records_the_door_the_statement_took ... ok
test a_session_write_conf_append_records_the_options_door ... ok
test result: ok. 2 passed; 0 failed

cargo test -p repark-sql --test write_door_native
test result: ok. 6 passed; 0 failed

.venv/bin/python -m pytest test_write_spine_1_enumeration.py test_write_spine_1_door_carrier.py -q
18 passed, 1 skipped
```

Round 2 mutants:

| mutant | change | red test | answer |
|---|---|---|---|
| (e) | the D1 record in `execute_insert_routed` deleted | `every_spark_door_records_the_door_the_statement_took` (`left: [] right: ["D1"]`) | red; restored |
| (f) | the `clear` at the top of `execute_in_session` deleted | both `write_door_record.rs` tests (`left: ["D1", "D3"] right: ["D3"]`) | red; restored |

## Round 3: rulings Q7 to Q10 and the fork-provider UPDATE (2026-10-24)

- **Q7:** `_native.last_write_doors(session)` takes the native session; kept.
- **Q8:** a `CALL` writes no new values, so the statements it runs through `router::execute`
  may own the record (C-011 says so).
- **Q9, ruled:** an `UPDATE` the fork-provider delegation runs writes values, so it records D10
  as well. `router/door_tag.rs`'s `record_planned_update`, called by `spark_ast.rs` after
  planning, records D10 when the plan is a `LogicalPlan::Dml` with `WriteOp::Update`. That plan is the delegation, because the predicate-DML route has returned
  before the plan is built. On main the Spark door reaches the delegation with an `UPDATE`
  that has no `WHERE`: `allowed_update_with` answers `None` without a selection, so
  `try_execute_identity_dml` answers `None`. A `WHERE st.v = 7` over a struct column was tried
  first and still took the predicate-DML route.
- **Q10:** the DataFrame writers were not run through `last_write_doors`; step 3b's order
  carries the check (a writer that issues a router statement after its write would clear the
  record).

```
cargo nextest run -p repark-spark --test write_door_record
PASS an_update_the_fork_provider_runs_records_the_update_door
PASS every_spark_door_records_the_door_the_statement_took
PASS a_session_write_conf_append_records_the_options_door
```

| mutant | change | red test | answer |
|---|---|---|---|
| (g) | the `record_planned_update` call in `spark_ast.rs` deleted (the fork-provider D10 record) | `an_update_the_fork_provider_runs_records_the_update_door` (`left: [] right: ["D10"]`) | red; restored |

## Round 4: verifier findings at `8d961b9` and the rulings (2026-10-24)

The Opus 5.5 verifier on PR #1182 returned FAIL on two surviving mutants (test gaps; no value
changed) and two S2 findings the orchestrator ruled.

- **S1-1 (dedupe):** deleting the `contains` check in `door_tag::record` kept every test green.
  `a_merge_records_each_door_once_however_many_arms_take_it` now runs a MERGE with two field
  assignments and two insert arms and asserts `["D9", "D7"]`.
- **S1-2 (spelling table):** deleting one of a door's several spellings kept the tests green.
  `test_the_spelling_table_is_exactly_the_forty_one_ruled_names` compares `DOOR_SPELLINGS` with
  a frozen copy of the 41 names on their doors. When #1181 merges, step 3b replaces the frozen
  copy with lane B's `SPELLINGS`.
- **S2, ruled (D8 versus D9 by path):** the record follows the cast path the assignment takes:
  the **paired-cast path** (`paired_assignment_cast_sql` with no kernel: `SET *`, a whole struct,
  array, map or struct-in-array column) records D8, and the **kernel path** (a field through
  `paired_nested_cast_sql`, or a leaf column the executor casts through `store_kernel_sql`)
  records D9. Before this round a whole array, map or struct-in-array column recorded D9;
  `takes_paired_cast` in `merge/nested_assign.rs` now names the container types. A
  non-container leaf column records D9 whether or not its type has a store kernel (an `INT`
  leaf casts through `arrow_cast`); the rule separates containers from leaves.
  `a_merge_whole_column_update_records_the_cast_path_it_takes` pins `arr`, `m` and `sa` as D8
  and `ts` and `ntz` as D9.
- **S2, recorded (static versus runtime):** on a MERGE whole-column update, the static classifier
  cannot see the column type and answers D8 for every shape; the runtime record answers D9 for a
  leaf column. This is a documented divergence class (C-010):
  `test_a_merge_whole_column_update_diverges_statically_only_on_a_leaf_column` runs the six
  target shapes and asserts static D8 with runtime D9 on `leaf_timestamp` and
  `leaf_timestamp_ntz`, and D8 on both for `struct`, `array`, `map` and `struct_in_array`.
- **S3:** C-008 cites both width early returns; C-002 says the error class is step (2)'s to
  change.

| mutant | change | red test | answer |
|---|---|---|---|
| (h) | the dedupe in `door_tag::record` deleted | `a_merge_records_each_door_once_however_many_arms_take_it` (`left: ["D9", "D9", "D7", "D7"]`) | red; restored |
| (i) | a container column recorded D9 again (`takes_paired_cast` replaced by `folds`) | `a_merge_whole_column_update_records_the_cast_path_it_takes` (`left: ["D9"] right: ["D8"]`) | red; restored |
| (j) | `df_overwrite_condition` deleted from D5 | `test_the_spelling_table_is_exactly_the_forty_one_ruled_names` (`1 failed, 4 passed, 1 skipped`; no other test reds) | red; restored |

```
cargo nextest run -p repark-spark --test write_door_record --test store_kernel_registry
7 tests run: 7 passed
.venv/bin/python -m pytest test_write_spine_1_enumeration.py test_write_spine_1_door_carrier.py -q
20 passed, 1 skipped
```

## PROPOSITION LEDGER — WRITE-SPINE-1 steps (1) and (3) — 2026-10-24

| id | claim | evidence | status | proof |
|---|---|---|---|---|
| C-001 | Every UDF name both `repark-functions` and `repark-iceberg` name is one constant in `repark_common::udf_names`, and no call site changed. | `grep -rn '"__repark_' crates/` shows each of the seven strings once, in `udf_names.rs`. | **PROVEN** | the grep above; the step 1a diff changes only the seven definitions into re-exports; `cargo build --workspace`, `make rust-clippy` green. |
| C-002 | A session whose NTZ wall kernel is not registered refuses an `INSERT … BY NAME` of an instant into a `TIMESTAMP_NTZ` column, where main stores the UTC wall (corrected 2026-10-24 from the order's "a wall into a `TIMESTAMP` column", measured above). | `store_kernel_registry.rs`, red on main, green on head. | **PROVEN** | `missing_wall_kernel_refuses_instead_of_storing_utc`: red with the base lookup, green on head; the same test's registered control stores `1767305045123456`. The refusal is `DataFusionError::Internal`, whose display asks the user to report a DataFusion bug; the class is step (2)'s to change, not this step's. |
| C-003 | `WriteDoor::ALL` holds exactly the eighteen value-writing doors of the evaluation's table, and `door_of` is an exhaustive match over the dispatched statement shapes. | `write_door.rs`; the enumeration tests; a removed variant reds them. | **PROVEN** | `write_door_ids_are_the_evaluation_table_ids_and_the_first_ten_are_spark_doors`; `door_of` lists every `Statement` variant with no wildcard (compiles under `make rust-clippy`); the two `door_of` unit pins; mutant (b). The four choices of step 3a above await the orchestrator. |
| C-004 | Every door has at least one spelling and every spelling names a door. | `every_write_door_has_a_spelling`, `every_spelling_names_a_door`. | **PROVEN** | `test_every_write_door_has_a_spelling`, `test_every_spelling_names_a_door` green; mutants (b) and (d) red them. The spelling-corpus test skips until lane B's `_write_spine_1_spellings.py` lands. |
| C-005 | The door matrix on this head equals main's recorded answers on every cell, and classifies against the Spark fixture as the step-0 census table says. | `test_write_spine_1_doors.py` against the two fixtures. | **PROVEN** | step 3b. Proven 2026-10-24: the replay recorded with `_record_write_spine_1.py --main --base e67b2669` (no product change since main `6bd98bc8`) compares 12,192 cells, 0 differing, with the same id set and the status included; wall 3 m 21 s; counts 7,135 EQUAL, 1,731 DIFFERENT, 2,380 REFUSED, 946 ERROR, 0 UNRECORDED. The census recount test `test_fixture_cells_are_the_census` and `test_head_replay_moves_exactly_the_step_2_cells` pin it (pins: write-spine-1/C-005). The replay document is not committed. |
| C-006 | The four mutants of step 5 each red a named test. | the ledger's mutant table. | **PROVEN** | (b), (c) and (d) red their tests (table above); (a) waits for the matrix. Step 3b closes it: mutant (a) ran in step 3b and is red, and each of the step 3b mutants (table in the step 3b section below) reds a named test (pins: write-spine-1/C-006). |
| C-007 | `zone_store_frame` returns the frame unconverted when the session analyzer fails on the planned write (`analyzed_types` answers `None`). | Reading of `ntz_store.rs` (`let Some(planned) = analyzed_types(…) else { return Ok(frame) }`). | **PROVEN** | fail-open; closed by step (2)'s conform stage. Closed 2026-10-25 (WRITE-SPINE-1 step 2): the analyzer error now propagates (`planned_types`); a width mismatch resolves targets by name instead of returning the frame; an unknown listed column refuses (`table_targets`). |
| C-008 | `zone_store_frame` returns the frame unconverted when a width does not match: the frame width differs from the target width, or the planned width differs from the target width. | Reading of `ntz_store.rs`: the frame-width early return at `:203` (`targets.len() != frame.schema().fields().len()`) and the planned-width early return at `:209` (`if planned.len() != targets.len() { return Ok(frame) }`); both are fail-open sites for step (2). | **PROVEN** | fail-open; closed by step (2)'s conform stage. Closed 2026-10-25 (WRITE-SPINE-1 step 2, amended in fix round 1): the analyzer error now propagates (`planned_types`); a by-name width mismatch resolves targets by name, and a positional width mismatch is reported by the door's own arity check before the stage converts anything; an unknown listed column refuses (`table_targets`). |
| C-009 | The eighteen-door table is corrected (orchestrator ruling, 2026-10-24): D14 is bare-session SQL through `repark.sql()` in `repark-core`; D15 to D20 are `repark-sql` Rust-library doors with reach `rust_api`; D18 and D21 are unchanged. | `_write_spine_1_doors.py` marks D15 to D20's spellings `rust_api`; `test_every_rust_api_only_door_names_a_rust_test_that_exists`. | **PROVEN** | the enumeration test green; `rust_api_only_doors()` is exactly D15 to D20. |
| C-010 | `write_door_of(sql, options)` names the door of each Spark spelling without running it, through the router's intercepts and `door_of` (the parsed-shape reading). | `test_write_door_of_names_the_door_of_each_spark_spelling_without_running_it`. | **PROVEN** | 12 parametrized cases green: D1 to D10 and `None` for DELETE and SELECT. Documented divergence class (round 4): on a MERGE whole-column update (`SET t.c = s.c`) the static reading answers D8 for every column type; the runtime record answers D9 for a leaf `TIMESTAMP` or `TIMESTAMP_NTZ` column (the kernel path) and D8 for a struct, array, map or struct-in-array column. Pinned by `test_a_merge_whole_column_update_diverges_statically_only_on_a_leaf_column`. |
| C-011 | A Spark session records the set of doors its last statement took, at the branch points, cleared per statement; a Postgres target and `repark.sql()` record nothing. | `write_door_record.rs`; `test_last_write_doors_records_the_doors_each_statement_took`; `test_the_bare_sql_session_records_no_door`. | **PROVEN** | the three Rust pins and both Python pins green; mutants (e), (f) and (g) red them. The Postgres route is by reading (it returns before any record); no Postgres pin. An `UPDATE` the fork provider runs records D10 (round 3, `an_update_the_fork_provider_runs_records_the_update_door`). A `CALL` writes no new values, so the statements it runs through `router::execute` own the record (ruling Q8). |
| C-012 | Every door no Python spelling executes (D15 to D20) is written through by a named Rust test that reads the rows back. | `crates/repark-sql/tests/write_door_native.rs`; the enumeration test's name check. | **PROVEN** | six tests green; `RUST_TESTS` names each and the enumeration test finds each `async fn`. |

## Step (3b): the door matrix and the runtime record (2026-10-24)

The door matrix, `test_write_spine_1_doors.py`, runs against the two step-0 fixtures. The mutant
labels below are the step 3b builder's labels; they are not the step 1 and 3a labels above (there
(b) is the `WriteDoor` removal). The sample holds 338 `spark_sql` cells and 70 DataFrame cells; the
three gate files run 31 passed in 20.5 s; the runtime-sample test takes 14.8 s and the DataFrame test
2.8 s, against a `timeout(900)` marker.

| mutant | change | red test(s) | failure line |
|---|---|---|---|
| (a) | `crates/repark-spark/src/write_door.rs:108` `Statement::Insert(insert) => Some(insert_door(insert, options))` becomes `Statement::Insert(_) => None` | `test_static_door_agrees_with_the_corpus_on_every_spark_sql_cell` (960 cells answer None), `test_static_door_classifies_native_sql_cells` (720 cells), `test_write_door_of_names_the_door_of_each_spark_spelling_without_running_it` (D1, D2, D4 cases) | `Left contains 960 more items, first extra item: 'insert_values.append.leaf_ltz.inst.utc: None'` |
| (b) | `_write_spine_1_spellings.py`: `merge_insert_star` door `D7` becomes `D8` | `test_the_spelling_table_is_exactly_the_forty_one_ruled_names`, `test_static_door_agrees_with_the_corpus_on_every_spark_sql_cell`, `test_runtime_record_agrees_with_the_corpus_on_the_sample` | `3 failed, 13 passed` |
| (c) | `crates/repark-spark/src/router.rs:97` `door_tag::clear(ctx);` removed | `test_runtime_record_agrees_with_the_corpus_on_the_sample`, `test_dataframe_writers_record_their_doors`, both `test_a_recorded_divergence_holds_as_measured` cases, plus the step-3a carrier tests | `Left contains 378 more items, first extra item: "insert_values.append.leaf_ltz.inst.utc: a SELECT recorded ['D1']"` |
| (d) | the C-010 branch of the static `spark_sql` test removed | `test_static_door_agrees_with_the_corpus_on_every_spark_sql_cell` | `Left contains 192 more items, first extra item: 'merge_update_field.cow.leaf_ltz.inst.utc: D8'` |
| M4 | `D9` removed from ZS9-AHEAD-OF-DOOR's `doors` | `test_a_recorded_divergence_holds_as_measured[ZS9-AHEAD-OF-DOOR]` | `assert 960 == 976`; with the count line also removed, the live check still fails: `'merge_update_field.cow.struct_ntz.inst.new_york: []'` (the D9 cell records `[]` where D9 is expected) |
| M5 | `cells=976` becomes `975` | `test_a_recorded_divergence_holds_as_measured[ZS9-AHEAD-OF-DOOR]` | `assert 976 == 975` |

Mutant (a) does not fail the runtime tests, because the runtime record of an INSERT is written at the
router's branch points, not from `door_of`; only the static tests read `door_of`, so they are the ones
that red.

## PROPOSITION LEDGER — WRITE-SPINE-1 step (3b) — 2026-10-24

C-005 and C-006 close in the table above: their verdict cells changed in place and their text is
kept, because the grammar gate forbids a second row with the same id. C-013 to C-016 are new.

| id | claim | evidence | status | proof |
|---|---|---|---|---|
| C-013 | The static door reading, `write_door_of(sql, options)`, answers the corpus door on every fixture `spark_sql` cell except the C-010 class, and classifies every fixture `native_sql` cell as the Spark router reads its text. | `test_static_door_agrees_with_the_corpus_on_every_spark_sql_cell` covers all 8,112 `spark_sql` cells: each answer equals the corpus door, and the 192 C-010 cells answer D8 (their corpus door is D9; the class is the documented divergence of C-010). `test_static_door_classifies_native_sql_cells` covers all 2,400 `native_sql` cells against `NATIVE_TEXT_SPARK_READING` (`native_insert` D1, `native_insert_conf` D1, `native_insert_overwrite_partition` D4, `native_ctas` D6, `native_merge` D8, `native_update` D10, `native_delete` None); that table is the Spark router's reading of native text, not the door taken (Q2 ruling). | **PROVEN** | The two static tests above hold on this head (pins: write-spine-1/C-013). The fixtures' `native_sql` cells are measurements of D14 whatever door their spelling name carries, because their text runs through `repark.sql()`, the bare repark-core session (C-009). |
| C-014 | The runtime door record, `last_write_doors` read right after each write, holds the corpus door on the Spark sample and on the DataFrame writers, and a `SELECT` records nothing. | `test_runtime_record_agrees_with_the_corpus_on_the_sample` runs 338 `spark_sql` cells (113 UTC, 113 New York, 112 Kolkata; copy-on-write and merge-on-read both sampled): each record holds the corpus door, a measured divergence (C-016) records its measured record, the C-010 class records exactly [D9], and a `SELECT` after each cell records nothing. `test_dataframe_writers_record_their_doors` runs 70 cells (7 writers by 10 targets): each writer records its corpus door or a divergence row's record. No writer showed an empty record from a router statement issued after its write (ruling Q10's case: not observed). | **PROVEN** | The two runtime tests above hold on this head, and `test_a_recorded_divergence_holds_as_measured` pins the two divergence rows of C-016 as measured (pins: write-spine-1/C-014). |
| C-015 | One source of spelling truth: `DOOR_SPELLINGS` is derived from the corpus `SPELLINGS` under the Q1 ruling, and the public answers do not change. | `_write_spine_1_doors.py` builds `DOOR_SPELLINGS` from `_write_spine_1_spellings.SPELLINGS`, grouped by door in door-number order, and `RUST_API_SPELLINGS` as the corpus's `rust_api` entries plus its `native_sql` entries on D15 to D20 (Q1 ruling under C-009). `test_the_spelling_table_is_exactly_the_forty_one_ruled_names` keeps a frozen copy of the 41 names and compares the names per door as sorted lists; the derived tuples follow corpus order, which differs from the frozen copy's order on D1 and D4. `test_every_spelling_name_has_its_template_in_the_spelling_corpus` imports the corpus directly, with no skip. | **PROVEN** | The two named pins hold: `test_the_spelling_table_is_exactly_the_forty_one_ruled_names` and `test_every_rust_api_only_door_names_a_rust_test_that_exists` (pins: write-spine-1/C-015). The public answers (`spelling_reach`, `rust_api_only_doors`, `spelling_doors`) give the same answers as before. |
| C-016 | The runtime record misses the corpus door on two measured rows, and a conform stage placed at the door branch points would not see those statements. | Measured over all 9,792 facade cells in all three zones: outside the C-010 class, 1,552 cells miss the corpus door, and every one falls in one of two rows. `ZS9-AHEAD-OF-DOOR` (976 cells; doors D1, D2, D3, D4, D5, D7, D8, D9, D10): the main answer raises a ZS-9 marker (`A value that is not provably a TIMESTAMP_NTZ wall time`, or `The session time zone is not UTC, and a nested TIMESTAMP_NTZ`) and the record is empty, because the ZS-9 nested-zone refusal raises at planning, before the router reaches the branch point of any door. `D10-STORE-CAST-AHEAD-OF-DOOR` (576 UPDATE cells; door D10): the main answer raises `Cannot write incompatible data for the table` and the record is empty, because the store-assignment refusal raises before D10 is recorded, while an INSERT or a MERGE with the same refusal records its door. `test_a_recorded_divergence_holds_as_measured` pins both rows as measured. Fix round 1 (2026-10-24, commit `c804cddd`): the D10 row is defined by the marker together with door D10, because INSERT and MERGE raise the same store-assignment text and still record their door; there are 1,512 marker cells over nine doors, and 576 of them are D10 cells with an empty record. The pin now assigns every census cell to a row by markers alone and asserts the per-row count and door set, after the Muse MAX verifier's S2 of 2026-10-25: its mutant M4, the ZS9 row without D9, survived the first pin; it now fails with 960 against 976, and a count changed to 975 also fails. | **PROVEN** | OPEN: step 2 decides where the conform stage sits. The ZS-9 refusal fires before every door's record point, so a stage placed at the door branch points would not see these statements (pins: write-spine-1/C-016). Closed 2026-10-25 (step 2): the stage sits after each door's record point. The ZS9 and D10 store-cast rows stand as measured, unchanged by design (the replay keeps all 976 ZS-9 cells REFUSED). |

## Step (2): the conform stage (2026-10-25)

**Builder:** Claude Opus 5.5 (`claude-opus-5-5`), lane A, branch `feat/write-spine-1-step-2`, base `origin/main` `cf6641a4`. Commits `68ff5a8d` (order), `f76a673a` (sketch) and the three build slices `0db0c3af`, `18baab29`, `d45eb72d`. **Clerk:** the maps, this section, the proposition rows, the evidence file, the cards and the parity rows were written by the Haiku 5.5 clerk (`claude-haiku-5-5`) from the builder's notes; the clerk commit changes no code and no expected value.

Built in three slices. 288 cells move, not the sketch's 328 (F-2 and F-3 account for the 40-cell difference). Verification is pending. The order's Outcome records the same.

### Rulings WS2-12 to WS2-26 (2026-10-25)

- WS2-12 (Q1): `zone_seam_udf(false)` is registered by name in `repark-functions` `registration.rs` and in the ANSI dialect's `on_session_built`. Done in slice 1.
- WS2-13 (Q2): `NARROW_TIMESTAMP_NS_NAME` moves into `repark_common::udf_names`, the narrowing UDF is registered beside the wall kernel on both session builders, and the stage names it fail-closed. The microsecond wall floor stays on `TIMESTAMP_NTZ_CAST_NAME`. The order's WS2-3 named the wrong kernel; the sketch's correction 3 is the record. Done in slice 2.
- WS2-14 (Q3): B2. The stage's plan pass rewrites a nested `arrow_cast` whose input has a zone leaf into the seam call, using analyzed types, inside the row-merge CASE too. The site B text is unchanged. Pin 6 keeps its name. As built, the rewrite applies only when the `arrow_cast` argument reads a column (see Findings F-2 and F-3).
- WS2-15 (Q4): pin 1 walks `WriteDoor::ALL` through an exhaustive match with no wildcard. The routed doors show their stage site. The named arms are PlanConformed (D1, D2, D5), TypesCopied (D6, D17), ForkProvider (D14), NativeStager (D15, D16) and Control (D18).
- WS2-16 (Q5): D19 and D20 take the stage like D7 to D10, with no native special case. A native-door zone ruling is its own card (note for the card list).
- WS2-17 (Q6): D21 on a bare `repark-core` session refuses fail-closed with a Plan error that names the missing kernel and the dialects that register it. Streaming-track note, dated 2026-10-25: the micro-batch session must install the dialect kernels before D21 writes a timestamp value that needs a conversion. A session built without the Spark or ANSI dialect refuses that write.
- WS2-18 (Q7): the UPDATE with no WHERE runs on the fork provider and classifies as ForkProvider beside D14. It is not an H2 halt, and its 432 F8 cells stay unchanged (measured). OPEN ledger row below.
- WS2-19 (Q8): measured before slice 3. See "WS2-4 measurement".
- WS2-20 (Q9): the D4 PARTITION path gets the full rule set with the flags of the overwrite path, plus a Rust pin for a float-to-integer overflow PARTITION write.
- WS2-21 (Q10): a move is any change in the recorded answer (status, step, type, values, error class, first line). A move that does not end EQUAL is H3. This supersedes WS2-8's status-only wording.
- WS2-22 (Q11): `wrap_store_outputs` and `judge_wrap_store_assignable` moved into the stage. The five helpers became `pub(crate)` in place. No wrapper stays behind.
- WS2-23 (Q12): line edits allowed in the named files. Two test files outside that list were also edited, because a pin had no other home: `crates/repark-core/src/microbatch/run_tests.rs` (D21 driver pins; the public `start` refuses a local catalog, `driver.rs:760`, so only a crate test can run the driver on a memory catalog) and `crates/repark-spark/tests/ntz_into_ltz_nested.rs` (pin 6, named by the order).
- WS2-24 (Q13): the leaf kernel text of D8, D9 and D10 stays as the stage's text face. The plan pass leaves an already target-typed leaf alone.
- WS2-25 (Q-B1, orchestrator ruling 2026-10-25): accepted. The site-B typed rewrite seams only an `arrow_cast` whose argument reads a column; the 40 F5 nested-literal cells (32 on D8, 8 on D10) stay DIFFERENT as on main. The literal form is carded (NESTED-LITERAL-SEAM-1, `task/roadmap/mid-term/nested-literal-seam-1-card-2026-10-25.md`) to fold before step 4, because step 4's ZS-9 retirement would otherwise leave 80 literal cells non-EQUAL (F-2, F-3).
- WS2-26 (Q-B2, orchestrator ruling 2026-10-25): step 4's order takes the mutant A+B table (726 of 976 EQUAL) as its input; the mutant A table stays as the record of WS2-9 as ordered. Both tables are recorded under C-023.

### WS2-4 measurement (Q8, 2026-10-25)

New York, table `(id INT, ntz TIMESTAMP_NTZ, ts TIMESTAMP, st STRUCT<a: TIMESTAMP>)`, seed `ntz = TIMESTAMP_NTZ '2026-01-01 00:00:00'`.
Spark 4.1.2 (live, Iceberg 1.11.0) stores `1769922000000000` for `SET ts = add_months(ntz, 1)`
with and without WHERE, and for `SET st.a = ...` and `SET st = named_struct('a', add_months(ntz, 1))`,
with and without WHERE. The head (slice 1 build) stores `1769922000000000` on every WHERE form.
The two struct forms without WHERE (the fork sub-path) raise `Arrow error: Invalid argument error:
arguments need to have the same data type`. Pin 4 pins the three WHERE forms. The no-WHERE struct
forms go into the OPEN row below, and nothing is built around them.

### Findings (dated 2026-10-25)

- F-1, for clerk-grade card NESTED-TEXT-LEAF-STORE-1: a text leaf into a nested timestamp stores the UTC reading where Spark refuses `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`. This step does not move these 102 cells; the stage excludes text leaves. Ids, main value and Spark's refusal are in [the evidence file](../../wo/write-spine-1/evidence/step-2-census-2026-10-25.md).
- F-2: on D8 (MERGE update, whole struct or SET *), a nested date or wall literal under the row-merge CASE stays on main's UTC reading. The site-B rewrite seams only an `arrow_cast` whose argument reads a column. Measured on the first slice-1 build, which seamed literals too: 40 D8 cells, 24 of them EQUAL on main, failed with `Arrow error: ... column types must match schema types` (the projected list loses its `PARQUET:field_id` metadata). The 32 F5 D8 literal cells therefore stay DIFFERENT, as on main. A plain SELECT with the same CASE works; the cause is not found inside this budget.
- F-3: a constant nested value is not safe to seam outside a CASE either. With literals allowed outside a CASE, the existing pin `a_plain_update_whose_only_naive_leaf_is_a_try_cast_stores_spark_instant` failed. `TRY_CAST(instant AS TIMESTAMP_NTZ)` inside a literal struct came out as the UTC wall, and the seam then read it in New York (`1767341045123456` against Spark's `1767323045123456`). So the column-reference guard applies everywhere. As a result, 8 D10 `update_whole_where` struct cells (a date literal, or a scalar-subquery date source) also stay DIFFERENT, as on main; also on a partitioned target (Muse probe P25), the same mechanism, outside the census.
- F-4: the order's §2 named `store_overflow.rs` and `store_cast.rs` as "at their ceilings". They are under the default ceiling, not exception rows (sketch correction 1).
- F-5 (S1, verification round 1, C-026): a date into a `MAP<TIMESTAMP, STRING>` key stores UTC midnight on D3 where Spark stores the session midnight, from a literal and from a column. `INSERT INTO t BY NAME SELECT 1 AS id, map(DATE '2026-01-02', 'x') AS m` into `(id INT, m MAP<TIMESTAMP, STRING>)`: New York base `1767312000000000`, head `1767312000000000`, Spark `1767330000000000`; Kolkata base and head `1767312000000000`, Spark `1767292200000000`. The stage's map arm clones the key field and partials only the value, so a key pair never reaches the seam. Pre-existing, outside the census; see the verification round 1 subsection and card NESTED-MAP-KEY-ZONE-1.

### Census (C-022), head `d45eb72d`, 2026-10-25

`_record_write_spine_1.py --main --base d45eb72d` on the `maturin develop --profile ci` build, diffed against `write_spine_1_main_7a789fc5.json`. A move is any change in the recorded answer (WS2-21).
Counts: 7,423 EQUAL, 1,443 DIFFERENT, 2,380 REFUSED, 946 ERROR, 0 UNRECORDED (main: 7,135 / 1,731 / 2,380 / 946).
**288 cells moved, all DIFFERENT → EQUAL, all family F5 (a date into a nested TIMESTAMP outside UTC).** Zero EQUAL cells left EQUAL, zero moves end elsewhere, and zero ZS-9 cells moved: REFUSED is 2,380 on both.

| door | F5 cells moved |
|---|---|
| D3 | 16 |
| D4 | 56 |
| D7 | 96 |
| D8 | 64 (32 literal cells stay DIFFERENT, F-2) |
| D10 | 56 (8 literal or subquery struct cells stay DIFFERENT, F-3) |

The sketch's expectation was 328. Findings F-2 and F-3 account for the 40-cell difference. Earlier slice-1 builds, which also seamed literals, are recorded in F-2 and F-3.
Pin 8: `test_write_spine_1_doors.py test_write_spine_1_enumeration.py test_write_spine_1_door_carrier.py test_zone_seam_1.py test_zone_oracle_1.py`: 93 passed, 2 skipped (no replay document; live tier off).

### Mutant table (one per site, one per rule), 2026-10-25

Each mutant was one uncommitted edit, reverted after its run. Site mutants make the stage return its input unchanged for that site, which is equivalent to removing the door's call.

| mutant | red tests |
|---|---|
| site InsertByName (D3) skips | `every_value_writing_door_passes_the_stage`, `a_missing_kernel_refuses_on_every_routed_door`, `the_sink_guard_never_fires_on_a_routed_door`, `a_nested_zone_pair_without_the_seam_refuses_and_records_nothing` |
| site InsertOverwrite (D4) skips | pin 1, pin 2, pin 3, `a_partition_overwrite_guards_an_integer_overflow_at_plan_time` |
| site PartitionOverwrite (D4) skips | pin 1, `a_partition_overwrite_guards_an_integer_overflow_at_plan_time` |
| site MergeInsert (D7, D19) skips | pin 1, pin 2, pin 3 |
| site MergeUpdate (D8, D9, D19) skips | pin 1, pin 6 |
| site PredicateUpdate (D10, D20) skips | pin 1, pin 4 |
| site MicroBatchAppend (D21) skips | pin 1, pin 2, pin 3, `the_micro_batch_sink_passes_the_stage`, `a_bare_session_micro_batch_refuses_a_conversion_without_its_kernel`, `the_stage_record_holds_each_site_once_until_cleared` |
| rule: nested zone seam (frame form) off | `a_nested_zone_pair_without_the_seam_refuses_and_records_nothing` (the census F5 rows on D3, D4, D7 are its value-level proof) |
| rule: site-B typed rewrite off | pin 6, pin 4 |
| rule: a missing kernel stores the value unconverted (fail open) | pin 2, `a_bare_session_micro_batch_refuses_a_conversion_without_its_kernel` |
| rule: floor into a µs instant off | pin 3 |
| rule: staging-sink pairing guard off (by hand) | pin 5 (`the_staging_sink_pairs_on_every_branch`) |

### WS2-9: step-4 evidence (C-023, OPEN until step 4), 2026-10-25

Two uncommitted mutant builds on `d45eb72d`, each replaying the full census and then reading the 976 ZS-9 ids (main's answer holds a ZS-9 marker). Both mutants are reverted, and the module is rebuilt at the head. On both runs no cell outside the 976 differs from the head replay.

**Mutant A (the order's): `refuse_naive_into_instant` returns `Ok(())`.** Of the 976: 286 EQUAL, 64 DIFFERENT, 50 ERROR, 576 REFUSED.
- The 560 F3 cells (an instant into a nested TIMESTAMP_NTZ) stay REFUSED. R-011's retained arm (`refuse_zoned_micros`, marker 1) still refuses them, so mutant A alone cannot measure them.
- F4 (a wall into a nested TIMESTAMP, 392 cells): 286 EQUAL, 40 DIFFERENT, 50 ERROR, 16 REFUSED.
- The 24 F1 cells stay DIFFERENT, because Spark raises `SCALAR_SUBQUERY_IN_VALUES`.

**Mutant A+B (extra, for step 4's full scope): `refuse_zoned_micros` also returns `Ok(())`.** Of the 976: **726 EQUAL**, 104 DIFFERENT, 110 ERROR, 36 REFUSED.

| family, door | EQUAL | DIFFERENT | ERROR | REFUSED |
|---|---|---|---|---|
| F1 D1 / D4 | 0 | 8 / 16 | 0 | 0 |
| F3 D1, D2, D3, D4, D5, D7, D9 | 72, 16, 16, 56, 32, 96, 16 | 0 | 0 | 0 |
| F3 D8 | 64 | 32 | 0 | 0 |
| F3 D10 | 72 | 8 | 60 | 20 |
| F4 D3, D7 | 16, 96 | 0 | 0 | 0 |
| F4 D4 | 54 | 0 | 2 | 0 |
| F4 D8 | 64 | 32 | 0 | 0 |
| F4 D10 | 56 | 8 | 48 | 16 |

What the non-EQUAL cells are:
- The 80 D8/D10 DIFFERENT cells are findings F-2 and F-3: nested literals the typed rewrite leaves on main's UTC reading.
- The 108 D10 ERROR and 36 D10 REFUSED cells run on the fork sub-path with no WHERE. They are the Arrow `arguments need to have the same data type` / `column types must match schema types` errors and the F2 `UPDATE with SET column = (SELECT ...)` refusal; see the OPEN row.
- The 2 D4 ERROR cells are `insert_overwrite_values` into a map, which fails on the sink's map entry names (census F8).
- The 24 F1 cells are DIFFERENT by design (Spark raises `SCALAR_SUBQUERY_IN_VALUES`).

Per-cell tables (id, family, Spark value, mutant value, status): mutant A and mutant A+B are in [the evidence file](../../wo/write-spine-1/evidence/step-2-census-2026-10-25.md).

## Step (2), fix round 1 (2026-10-25)

**What PR #1189's CI showed.** The facade shards 0 and 2 and the Rust workspace run went red on the step (2) build, after the census had been green. The failures were 106 carve-out cells (the `r3_part` cells of [ice-struct-pairing-1](ice-struct-pairing-1-ledger.md) C-031 that Spark stores and main refuses with hint H-1), 6 partition refusal-text cells (the `ow_part` cells), 1 arity-order cell and 1 Rust pin, all on the D4 INSERT OVERWRITE path after WS2-20, which paired a struct at plan time on the PARTITION site. 113 cells in all, outside the census: the census is not the facade, and CI runs the facade. Fix round 1 is commit `c3f22afb` (slice 4), which changes `crates/repark-iceberg/src/write/conform_stage.rs` and `crates/repark-spark/src/insert_overwrite/store.rs` only.

**Rulings.**
- WS2-27: the carve-out stands. On the PartitionOverwrite site the stage applies the zone rule, the floor and the overflow guard but no longer pairs a struct at plan time, so `INSERT OVERWRITE ... PARTITION` refuses a struct whose field names are not the table's, in order, with hint H-1, as on main (ice-struct-pairing-1 C-031, owner ruling 58(a)).
- WS2-28: the text face. The stage's store-assignment gate stands aside on the PartitionOverwrite site (`gate_op` is set only for the InsertOverwrite site in `insert_overwrite/store.rs`), so the partition path's own refusal text answers.
- WS2-29: arity first. A positional stage request whose width differs from its targets passes the plan unchanged and records its site, so INSERT OVERWRITE reports the column-count mismatch before any store-assignment refusal, as on main. C-008's closure is amended to say so.

**Gate lines (the builder's report).**
- Three files in full (`writes/test_ice_struct_pairing_1_round3.py`, `timestamps/test_ntz_store_doors_1.py`, `writes/test_cast_overflow_insert_1.py`): 571 passed.
- `python/repark/tests/writes` (`-n 8`): 5,519 passed, 39 skipped, 3 xfailed.
- `python/repark/tests/timestamps` (`-n 8`): 1,862 passed, 53 skipped.
- Census recount: the same 288 cells, DIFFERENT to EQUAL; the replay equals the `d45eb72d` replay on all 12,192 cells.
- Workspace nextest: 7,892 passed, 97 skipped (the orchestrator's record of the builder run). The builder's notes record the targeted run `cargo nextest run -p repark-spark -p repark-sql -p repark-iceberg -E 'binary(struct_pairing_doors) | binary(write_door_native) | test(conform_stage)'`: 27 passed.
- `make rust-fmt-check rust-clippy rust-panic-ban`: exit 0.

**Mutants (each reverted after its run).**

| mutant | red |
|---|---|
| carve-out guard off (`pairs` always true) | `the_partition_overwrite_route_refuses_an_unpaired_struct_with_hint_h1` (the 106 facade cells are the same case in CI) |
| text face off (gate on the PARTITION site) | six `test_write_door_stores_sparks_value[*/ow_part]` cells |
| arity after store-assign (width rule off) | `test_vo4_1_insert_into_overwrite_reports_arity_first` |

**Carve-out measurement.** Before any change, on the `d45eb72d` build, the 106 carve-out cells stored Spark's rows on 106 of 106 (0 differ, 0 error). The ids and the measurement are in the [step (2) evidence file](../../wo/write-spine-1/evidence/step-2-census-2026-10-25.md), and the owner's question is carded as [PARTITION-PAIRING-CARVE-OUT-CLOSE-1](../../roadmap/mid-term/partition-pairing-carve-out-close-1-card-2026-10-25.md).

## Step (2), verification round 1 (2026-10-25)

**Verdict: FAIL.** The Muse MAX critic's round 1 on PR #1189 (head `5ce6524c`, base `cf6641a4`) found one S1 and two S2 gaps. All three are pre-existing (base stores the same values), none is a regression of this step. Under the critic's rule a PASS with any S1 is invalid, so the verdict is FAIL. The orchestrator reproduced the S1 and P25 on the head build and on Spark 4.1.2 in New York. The probe table and the recount table are in the [step (2) evidence file](../../wo/write-spine-1/evidence/step-2-census-2026-10-25.md), section "Verification round 1".

**Claim results.**
- Census: confirmed exactly. Replay with `--main --base 5ce6524c`: 7,423 EQUAL, 1,443 DIFFERENT, 2,380 REFUSED, 946 ERROR, 0 UNRECORDED. Whole-record diff against main: 288 moves, all DIFFERENT to EQUAL, no same-status change, the REFUSED id set identical; by door D3 16, D4 56, D7 96, D8 64, D10 56; every move is F5-shaped; the stayers are 32 D8 `merge_update_whole` date literals (F-2) and 8 D10 `update_whole_where` struct cells (F-3).
- Probes: 28 statements × 2 zones (America/New_York, Asia/Kolkata) × head, base and Spark 4.1.2 = 168 runs, all outside the census shapes. Every base-to-head change (22 cells) ends at Spark's value. The ledger-named rows match as named. Two gaps are new: S1 (F-5, C-026) and S2 (P25, under F-3).
- Fail-closed: confirmed on every routed door. The pin `a_missing_kernel_refuses_on_every_routed_door` is green; D8, D19 and D20 were checked by temporary tests (each refuses, names its kernel and writes nothing; reverted). The D19 and D20 class differs (C-027).
- Mutants: 6 of 6 red. M1 floor arm removed (pin 3); M2 `seam_call` identity fallback (`a_nested_zone_pair_without_the_seam_refuses_and_records_nothing`); M3 `record_site` dropped on the success path (pin 1); M4 `Date32` out of `is_zone_leaf` (`the_partial_type_converts_only_wall_date_and_instant_leaves`); M5 D21 conform call removed (`the_micro_batch_sink_passes_the_stage`); M6 column-reference guard removed (the F-3 pin, value `1767341045123456`). Each mutant was reverted.
- Arity: confirmed on D1, D2, D4, D4 PARTITION, D5, D7, D14, D15 and D16 (18 Python statements) and on D19 (two ANSI MERGE statements): every wrong-count write refuses and leaves its rows unchanged. A D7 `INSERT *` with an extra source column stores the row Spark stores, so it is no finding. D16 refuses partitioned inserts before writing (a pre-existing capability refusal). D18 and D21 have no statement surface.
- Rules: the comment grep over the code diff is empty; `make rust-clippy` and `make rust-panic-ban` exit 0; ledger grammar, docs links and both map checks are clean; the diff carries no account id, bucket, region, home or scratch path.

**Findings.**

*S1, F-5 (C-026): a date into a map key.* The statement `INSERT INTO t BY NAME SELECT 1 AS id, map(DATE '2026-01-02', 'x') AS m` into `(id INT, m MAP<TIMESTAMP, STRING>)`, door D3, stores `1767312000000000` on base and head in both zones, where Spark stores `1767330000000000` in New York and `1767292200000000` in Kolkata. The column-key form (`map(c_date, 'x')`, probe P28) gives the same triple. Mechanism: the stage's Map arm (`zone_partial` in `conform_stage.rs`) clones the key field and partials only the value, so a key pair never reaches the seam; the ZS-9 nested refusal fires on wall and instant key pairs (probes P21 and P22 refuse, the refuse direction), but a date into a TIMESTAMP key slips through and stores UTC. The unit pin `a_map_value_converts_and_its_key_keeps_its_field` covers `Utf8` keys only, and the census has no timestamp-key cell. Pre-existing; carded as NESTED-MAP-KEY-ZONE-1.

*S2, F-3 widened (P25): a partitioned UPDATE target.* `UPDATE t SET st = named_struct('a', DATE '2026-01-02') WHERE k = 0` on a partitioned `(id INT, st STRUCT<a: TIMESTAMP>, k INT)`, door D10, stores `1767312000000000` on base and head in both zones, where Spark stores `1767330000000000` in New York and `1767292200000000` in Kolkata. It is the F-3 mechanism on a partitioned target, outside the census and outside F-3's letter; the same fix scope applies (the column-reference guard), carded in NESTED-LITERAL-SEAM-1.

*S2, C-027: the D19 and D20 missing-kernel refusal class.* The refusal is an Analysis `UNRESOLVED_ROUTINE` naming `__repark_cast_timestamp_ntz__`, not the stage's Plan text that pin 2 captures on the other doors. On native MERGE and UPDATE the kernel name enters through SQL text (`update_cast.rs`), so the analyzer fires first. The outcome matches claim 3 (the statement refuses before writing and the rows are unchanged). Recorded as C-027; the class is to unify in the native-door zone ruling unit.

**Ruling WS2-30 (2026-10-25).** The step is not changed. The gaps are recorded (F-5, F-3 widened, C-026, C-027) and carded: NESTED-MAP-KEY-ZONE-1 (new, folds with NESTED-LITERAL-SEAM-1 before step 4), a dated line on NESTED-LITERAL-SEAM-1 for P25, and a dated line on NATIVE-DOOR-ZONE-RULING-1 for C-027. The census does not cover timestamp keys; that axis is carded, not measured here. The Opus verifier round is pending.

## Step (2), verification round 1 (Opus 5.5) and fix round 2 (2026-10-25)

**Verdict: FAIL.** The Opus 5.5 verifier's round 1 on PR #1189 (head `5ce6524c`, base `cf6641a4`) returned FAIL with one S1 cost finding, two S1 mutant findings, one S2 on C-022's pin and two S3. The orchestrator reproduced the cost finding and mutant C2. The findings and their rulings:

- **S1, cost (WS2-31).** The ZONE-SEAM-1 perf set is not within 1.05x on the three nested MERGE statements. The verifier ran 20 alternating rounds (10-run medians, one process per round and build, 3 warm-ups): UTC nested-NTZ MERGE 1.070 (head slower in 20 of 20 rounds), UTC LTZ-nested MERGE 1.062 (17 of 20), New York nested-NTZ MERGE 1.060, New York LTZ-nested MERGE 1.073 (17 of 20). Plain MERGE was 0.997 and 1.004; every other cell was within 1.023. The orchestrator's ten-round reproduction gave 1.061 and 1.056 for the nested-NTZ pair (UTC, New York) and 1.077 and 1.090 for the LTZ-nested pair (UTC, New York). The ledger never recorded the text of the U-8 perf statements, so the verifier rebuilt the set by name and shape. Suspected cause: `conform_stage` re-ran the analyzer in `planned_types` on every MERGE and UPDATE call. Profiled and cut in fix round 2 (below).
- **S1, mutant C2 (WS2-32).** Dropping the case-insensitive fallback in `resolve_target` left all 6,181 tests of the four crates green. The head is correct; the orchestrator reproduced the consequence: a D21 frame whose `TS` differs in case from `ts` stores the UTC wall under the mutant (the head stores the New York wall, `-18000000000`).
- **S1, mutant C7 (WS2-32).** Positional resolution whenever the widths match, ignoring `by_name`, also left 6,181 tests green. On a by-name door a reordered frame is refused under the mutant, where the head converts it.
- **S2, C-022's pin (WS2-33).** `test_head_replay_equals_main_fixture` asserted zero moves against the main fixture, the opposite of the census claim. With a replay document present it failed (288 differing keys); CI skipped it.
- **S3, D21 call coverage (WS2-34).** Pin 1's D21 arm calls `conform_table_frame` directly, so deleting the `run.rs` call left pins 1 to 3 green; the `run_tests` pins were red.
- **S3, D21 store-assignment gate (WS2-35).** D21 now refuses at plan time where main refused at the sink. The same class and types; the label changes. The fork-path UPDATE with no WHERE that the verifier observed is the C-024 family, unchanged from main and already OPEN.

**Rulings.**
- WS2-31: the cost is cut at its source, not accepted. The MERGE insert, MERGE update and predicate UPDATE sites hand the stage the plan their door already analyzed, and the stage takes the planned types from that plan's own schema (`analyzed_by_the_door`). The frame sites (D3, D4, D21) still analyze once. No rule changed.
- WS2-32: C2 and C7 get pins in `crates/repark-sql/tests/write_door_native.rs`, not in `conform_stage/tests.rs`, because the real wall kernel exists only in a Spark or ANSI session, which `repark-iceberg` cannot build.
- WS2-33: C-022's pin is replaced by `test_head_replay_moves_exactly_the_step_2_cells`, which asserts that the moves of a head replay equal the frozen 288-id file `write_spine_1_step2_moves.json`, and that each is EQUAL on the head. The replay document is not committed, so the recount is a verification-time measurement and CI still skips the test.
- WS2-34: pin 1's D21 arm calls `conform_table_frame` directly. The `run.rs` call is proven by `the_micro_batch_sink_passes_the_stage` and `a_bare_session_micro_batch_refuses_a_conversion_without_its_kernel`, which the verifier measured red when the call is deleted.
- WS2-35: D21 now runs the store-assignment gate (`gate_op` "append") at plan time. The refusal class is main's; the label changes from `INSERT OVERWRITE` to `append`, and the refusal comes before any file is staged. The streaming track confirms the wording it wants (C-029, OPEN).

**Profile.** A temporary, uncommitted timing build on the LTZ-nested MERGE in New York (20 statements after 3 warm-ups, box load 28): the MergeUpdate analysis pass took 3.1 ms and the MergeInsert analysis pass 1.7 ms, of a 74.8 ms statement median. The leaf and output passes took under 0.12 ms each. The analyzer re-run was the whole regression, about 6 % of the statement.

**Cut.** The MERGE insert, MERGE update and predicate UPDATE sites (`MergeInsert`, `MergeUpdate`, `PredicateUpdate`, all fed by `analyzed_store_source`) take the planned types from the plan their door already analyzed. The frame sites analyze once. No rule changed; the census is the proof, below.

**Perf, run 1** (one cell over: plain UPDATE in UTC at 1.053, with one 76.4 ms round against 58.0 ms; the cut removes the analyzer pass from that path and adds no work; the verifier measured that cell at 1.027 or below before the cut). Ten alternating rounds, 3 warm-ups, median of 10 per statement, head slower counted in rounds.

| zone | cell | base ms | head ms | ratio | head slower |
|---|---|---|---|---|---|
| UTC | plain INSERT | 26.21 | 25.99 | 0.992 | 4/10 |
| UTC | plain UPDATE | 51.60 | 54.34 | 1.053 | 8/10 |
| UTC | plain MERGE | 65.05 | 66.16 | 1.017 | 5/10 |
| UTC | nested-NTZ INSERT | 28.38 | 28.38 | 1.000 | 4/10 |
| UTC | nested-NTZ UPDATE | 55.22 | 56.26 | 1.019 | 7/10 |
| UTC | nested-NTZ MERGE | 72.47 | 73.94 | 1.020 | 8/10 |
| UTC | LTZ-nested MERGE | 48.78 | 46.65 | 0.956 | 5/10 |
| America_New_York | plain INSERT | 26.07 | 26.47 | 1.015 | 7/10 |
| America_New_York | plain UPDATE | 57.49 | 54.94 | 0.956 | 6/10 |
| America_New_York | plain MERGE | 66.94 | 65.79 | 0.983 | 2/10 |
| America_New_York | nested-NTZ INSERT | 28.44 | 28.49 | 1.002 | 4/10 |
| America_New_York | nested-NTZ UPDATE | 57.92 | 56.35 | 0.973 | 4/10 |
| America_New_York | nested-NTZ MERGE | 77.04 | 76.11 | 0.988 | 5/10 |
| America_New_York | LTZ-nested MERGE | 49.18 | 48.58 | 0.988 | 3/10 |

Loads recorded by the processes: start 3.55 to 9.92, end 3.55 to 9.28; every process was launched only when `/proc/loadavg` read below 5.

**Perf, run 2** (the gate run: every cell at or below 1.036; nested MERGE 0.977 UTC and 1.016 New York, LTZ-nested MERGE 0.978 UTC and 1.009 New York).

| zone | cell | base ms | head ms | ratio | head slower |
|---|---|---|---|---|---|
| UTC | plain INSERT | 25.88 | 25.98 | 1.004 | 6/10 |
| UTC | plain UPDATE | 51.48 | 50.95 | 0.990 | 3/10 |
| UTC | plain MERGE | 64.15 | 62.10 | 0.968 | 1/10 |
| UTC | nested-NTZ INSERT | 27.37 | 26.90 | 0.983 | 5/10 |
| UTC | nested-NTZ UPDATE | 54.79 | 54.27 | 0.991 | 3/10 |
| UTC | nested-NTZ MERGE | 72.20 | 70.56 | 0.977 | 1/10 |
| UTC | LTZ-nested MERGE | 45.06 | 44.08 | 0.978 | 2/10 |
| America_New_York | plain INSERT | 25.18 | 26.10 | 1.036 | 7/10 |
| America_New_York | plain UPDATE | 50.37 | 50.42 | 1.001 | 6/10 |
| America_New_York | plain MERGE | 62.53 | 62.19 | 0.994 | 5/10 |
| America_New_York | nested-NTZ INSERT | 27.80 | 28.04 | 1.009 | 5/10 |
| America_New_York | nested-NTZ UPDATE | 55.05 | 55.51 | 1.008 | 5/10 |
| America_New_York | nested-NTZ MERGE | 72.76 | 73.93 | 1.016 | 7/10 |
| America_New_York | LTZ-nested MERGE | 46.37 | 46.79 | 1.009 | 7/10 |

Loads recorded by the processes: start 3.12 to 5.58, end 3.22 to 5.45; every process was launched only when `/proc/loadavg` read below 5.

**Census recount.** The replay of the fix-round-2 tree equals the slice-4 replay on all 12,192 cells. The same 288 cells move DIFFERENT to EQUAL, and `test_head_replay_moves_exactly_the_step_2_cells` passes against it.

**Mutants, fix round 2** (each reverted after its run).

| mutant | red |
|---|---|
| C2: drop the `eq_ignore_ascii_case` fallback in `resolve_target` | `a_by_name_column_that_differs_only_in_case_resolves_to_its_target` |
| C7: positional whenever the widths match (ignore `by_name`) | `a_by_name_frame_in_another_order_resolves_by_name_not_position` |
| WS2-33 control: one id of the frozen move set swapped | `test_head_replay_moves_exactly_the_step_2_cells` |

**D21 store-assignment measurement (WS2-35).** One statement: a micro-batch append of `TIMESTAMP '2026-01-02 03:04:05'` into an `INT` column. Main refuses at the sink: `INSERT OVERWRITE cannot store-assign column v: source type Timestamp(µs, "UTC") is not ANSI-store-assignable to target type Int32 (Spark INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST ...)`. The head refuses at plan time: `append cannot store-assign column v: ...`, the same class and types.

**Gates, fix round 2.** The census recount: equal on all 12,192 cells, the same 288 moves. The three fix-round-1 files: 571 passed. `python/repark/tests/timestamps` (`-n 8`): 1,863 passed, 52 skipped. `make rust-fmt-check rust-clippy rust-panic-ban`: exit 0. `cargo nextest run --workspace --no-fail-fast`: 7,894 passed, 97 skipped. The comment grep is empty.

**Proposition rows.** C-017 gains the D21 call-coverage sentence (WS2-34) and the two new pins. C-022 cites `test_head_replay_moves_exactly_the_step_2_cells` and `test_fixture_cells_are_the_census`, and its text says the recount is a verification-time measurement against the frozen 288-id file. C-005's citation of the old test name is amended in place. C-028 (the ZONE-SEAM-1 perf set within 1.05x on the gate run) is PROVEN; C-029 (the D21 store-assignment gate note) is OPEN.

## Step (2), verification round 2 (2026-10-26, head 056b9058)

**Verdict: PASS under WS2-36.** The Muse MAX critic's round 3 returned PASS on head `056b9058`. The Opus 5.5 verifier's round 2 returned FAIL on one S1, the plain INSERT cost cell under the brief's ten-round base-first rule alone; the orchestrator accepted that verdict as a PASS under WS2-36 (ruling below). Both verdicts are PASS, so the union is met, and no S1 or S2 stays open on either side. Rows C-030 and C-031 are added to the step (2) proposition table below; C-028 and C-029 are not edited.

**Muse MAX round 3 (PASS).** No S1 and no S2. The census replay on the head moves 288 cells, all DIFFERENT to EQUAL, and the move set equals the frozen file `write_spine_1_step2_moves.json` and round 1's move set. 56 of 56 probe records (28 statements in two zones) are byte-equal to round 1. Six of six own mutants are red: R2, R3 and R4 corrupt the reused-plan branch; T1, T2 and T3 corrupt `resolve_target`. One equivalence probe, which forces the reused-plan branch on every site, changes nothing on 12,192 census cells, the 56 probes and the full suites. No live Spark run was made; round 1's Spark values are cited.

**Opus 5.5 round 2 (FAIL on item 1; items 2 to 6 pass).**

- *Item 1 (S1, cost).* Under the ten-round base-first harness the plain INSERT cell read 1.054 in UTC (run 1, head slower in 5 of 10) and 1.111 in New York (run 2, head slower in 8 of 10). The same New York cell read 0.946 in run 1. Both builds show bimodal process times, about 15.5 or 20 ms per process. A controlled 20-round run with the order alternated and 30-run medians read 0.999 (UTC) and 0.994 (New York); round 1 read 0.981 and 0.996 over 30 rounds. Fix round 2 does not touch the plain INSERT path. The two readings above 1.05 are the only ones in the two runs; every other reading lies between 0.946 and 1.030. The nested-NTZ MERGE cells that WS2-31 cut read 1.002 to 1.016 in both runs. The tables are in the [step (2) cost evidence file](../../wo/write-spine-1/evidence/step-2-cost-2026-10-26.md).
- *Item 2 (reused analysis).* A fresh analysis equals the reused plan's analysis on 1,286 suite checks (385 MergeInsert, 522 MergeUpdate, 379 PredicateUpdate), 2,960 census checks (888, 1,160, 912) and 143 shape probes, with 0 divergences. The verdict's shape-probe block states 141 checks and a further 2 IN-subquery checks; the total is 143.
- *Item 3 (recount).* Exactly 288 moves, all DIFFERENT to EQUAL and all in family F5. Counts: 7,423 EQUAL, 1,443 DIFFERENT, 2,380 REFUSED, 946 ERROR, 0 UNRECORDED. `test_head_replay_moves_exactly_the_step_2_cells` passes against the replay and fails with one frozen id swapped.
- *Mutants.* C2 and C7 each red their pin: `a_by_name_column_that_differs_only_in_case_resolves_to_its_target` and `a_by_name_frame_in_another_order_resolves_by_name_not_position`. The verifier's own mutant C8 survives all tests (C-031, below).
- *Item 5 (D21).* A micro-batch append of a TIMESTAMP into an INT column refuses on main with `INSERT OVERWRITE` and on head with `append`, the same class and types. Neither build stages a file, writes a snapshot or stores a row.
- *Item 6 (rules).* The comment grep over the branch diff is empty; no absolute or scratch path is added; `make rust-clippy` and `make rust-panic-ban` exit 0; the ledger grammar, the Rust file-size gate, the crate DAG, the ledger check and the lib-rs check are clean.

**Disposition of the item-1 S1 under WS2-36.** The S1 is a measurement-protocol finding, not a code finding. The plain INSERT path is unchanged since round 1, the census and probes are unchanged, and no value moves. Under WS2-36 the cell is inside the gate; the 12-round alternated run in the ruling below is its record, and C-030 states the scoped claim.

**Opus S3 notes.**

1. *Mutant C8.* The case-insensitive fallback in `resolve_target`, applied only when the frame width equals the target width, survives all 6,183 tests of the four crates. It changes the stage's answer for a width-mismatched frame and stores no row: D3 builds full-width projections, and D21's sink refuses the column-count mismatch. Graded S3. Recorded as C-031, OPEN; a pin is owed by WRITE-SPINE-1 step 4 or NESTED-LITERAL-SEAM-1.
2. *Run selection in C-028.* The verifier notes that C-028 cites run 2 of two ten-round runs, and that one ten-round base-first run cannot hold a 1.05 gate on this box. Disposition: the gate's protocol is WS2-36, and its measurement is C-030. C-028 stays as written; its two tables remain its record.

**Ruling WS2-36 (2026-10-26, measure before ruling).** Recorded as given by the orchestrator after it reproduced the cell under the alternated protocol:

- WRITE-SPINE-1's cost gate is measured under alternated base and head order, with 30-run medians over at least 12 rounds.
- The plain INSERT cell is inside the 1.05 gate under that protocol: UTC 1.039 (head slower in 6 of 12) and New York 0.993 (head slower in 7 of 12), over 12 rounds. The nested-NTZ INSERT reads UTC 1.017 and New York 0.988. The start loads of that run are 2.3 to 2.4.
- The base-first excursions (the same New York cell read 0.946 in one run and 1.111 in the next) are protocol noise on 15 to 25 ms statements.
- The Opus round-2 verdict is accepted as a PASS under WS2-36. Muse round 3 is a PASS. The union is met, and the PR merges on the owner's standing grant.

## PROPOSITION LEDGER — WRITE-SPINE-1 step (2) — 2026-10-25

C-007, C-008 and C-016 close in the table above: their verdict cells changed in place and their text is kept, because the grammar gate forbids a second row with the same id. C-017 to C-024 are new. C-023 stays OPEN until step 4 reads the WS2-9 tables; C-024 is OPEN as the fork-provider input to step 4.

| id | claim | evidence | status | proof |
|---|---|---|---|---|
| C-017 | One plan-time conform stage: every routed Iceberg write door passes it, and it refuses fail-closed when a kernel it names is missing. | `every_value_writing_door_passes_the_stage`, `a_missing_kernel_refuses_on_every_routed_door` (pins 1 and 2); the mutant table above, one mutant per site and per rule. | **PROVEN** | Both tests green on the head; every site mutant reds the tests the mutant table names; the fail-open mutant reds pin 2. pin 1's D21 arm calls `conform_table_frame` directly; the `run.rs` call that the micro-batch sink makes is proven by `the_micro_batch_sink_passes_the_stage` and `a_bare_session_micro_batch_refuses_a_conversion_without_its_kernel`, which the verifier measured red when the call is deleted. The case-insensitive fallback and the by-name order of `resolve_target` are pinned by `a_by_name_column_that_differs_only_in_case_resolves_to_its_target` and `a_by_name_frame_in_another_order_resolves_by_name_not_position`; the two mutants (fallback dropped, positional whenever widths match) each red exactly their pin. pins: write-spine-1/C-017 |
| C-018 | The -1 ns floor runs through the stage, and the sink guard never fires on a routed door. | `the_sink_guard_never_fires_on_a_routed_door` (pin 3). | **PROVEN** | -1 ns stores as -1 µs (the floor) on D3, D4, D7, D9, D10 and D21, and D21's own stager accepts it. pins: write-spine-1/C-018 |
| C-019 | Site B names the seam: a nested zone pair in an assignment is converted through the seam, and the typed plan pass rewrites a column-reading `arrow_cast` (WS2-14, F-2, F-3). | `a_nested_zone_pair_in_an_assignment_names_the_seam` (pin 6). | **PROVEN** | Green on the head; the site-B rewrite mutant reds pins 4 and 6. pins: write-spine-1/C-019 |
| C-020 | The add_months fold: a plain UPDATE from `add_months` of a `TIMESTAMP_NTZ` stores the session midnight on the leaf, field and whole-struct WHERE forms. | `a_plain_update_from_add_months_stores_session_midnight` (pin 4), value `1769922000000000`. | **PROVEN** | Green on the head and equal to Spark's live value (WS2-4); the no-WHERE struct forms are C-024, not this row. pins: write-spine-1/C-020 |
| C-021 | The staging sink pairs on every branch: the unpartitioned and lineage branches refuse an unpaired struct, as the partitioned branch does (WS2-5). | `the_staging_sink_pairs_on_every_branch` (pin 5). | **PROVEN** | Without the guard the unpartitioned branch stores the batch, and the fork writer matched the fields by name. pins: write-spine-1/C-021 |
| C-022 | Never worse: the census on the head moves 288 cells and no other cell. Every move is DIFFERENT to EQUAL, all in family F5. | The census recount (section above): 7,423 EQUAL, 1,443 DIFFERENT, 2,380 REFUSED, 946 ERROR, 0 UNRECORDED against main's 7,135, 1,731, 2,380, 946; zero EQUAL cells left EQUAL; zero ZS-9 cells moved. | **PROVEN** | Recount on `d45eb72d`, diffed against `write_spine_1_main_7a789fc5.json`; the moves by door are D3 16, D4 56, D7 96, D8 64, D10 56. The recount is a verification-time measurement: the replay document is not committed, and `test_head_replay_moves_exactly_the_step_2_cells` asserts the moves against the frozen 288-id file `write_spine_1_step2_moves.json`; `test_fixture_cells_are_the_census` pins the fixtures. pins: write-spine-1/C-022 |
| C-023 | Step-4 evidence: the 976 ZS-9 cells under mutant A and under mutant A+B. | The WS2-9 tables (mutant A and mutant A+B) in the evidence file; mutant A+B gives 726 EQUAL, 104 DIFFERENT, 110 ERROR and 36 REFUSED. | OPEN | OPEN until step 4 reads both tables as its input (WS2-26). |
| C-024 | The fork sub-path: an UPDATE with no WHERE runs on the fork provider and sits outside the stage (WS2-18). | `plain.rs:221` and `plain_identity_needs_fork`; the 432 census F8 cells (`update_*_nowhere`) are unchanged on the head; the add_months struct forms without WHERE raise the CoW error `arguments need to have the same data type`. | OPEN | Input for step 4 and a fork card. It records the unchanged cells, not a fix. |
| C-025 | The carve-out of ice-struct-pairing-1 C-031 stands on the PartitionOverwrite site: the stage does not pair a struct there, and the partition refusal text and the arity-first order are as on main. | The 106 carve-out cells of `test_the_partition_carve_out_refuses_with_the_workaround` (ids in the step (2) evidence file, fix round 1); `the_partition_overwrite_route_refuses_an_unpaired_struct_with_hint_h1`; the six `ow_part` cells of `test_write_door_stores_sparks_value`; `test_vo4_1_insert_into_overwrite_reports_arity_first`. | **PROVEN** | Each named test green on `c3f22afb`; the three fix-round mutants each red their pin (table in the fix round 1 subsection). pins: write-spine-1/C-025 |
| C-026 | A date into a MAP key of TIMESTAMP type stores the UTC midnight on D3 outside UTC where Spark stores the session midnight, from a literal and from a column. The stage's map arm converts the value and keeps the key field by design, so the key pair never reaches the seam. | Muse MAX round 1, probes P03 and P28, New York and Kolkata (F-5 and the verification round 1 subsection); pre-existing, base equals head; the census has no timestamp-key cell. | OPEN | Closes with NESTED-MAP-KEY-ZONE-1 (card, grade C); fold with NESTED-LITERAL-SEAM-1 before step 4. |
| C-027 | The D19 and D20 missing-kernel refusal is an Analysis `UNRESOLVED_ROUTINE` naming the kernel, not the stage's Plan text. The fail-closed outcome is the same: the statement refuses and no row is written. | The temporary D19 and D20 tests of verification round 1 (green, reverted); pin 2 on the routed doors (`a_missing_kernel_refuses_on_every_routed_door`). | OPEN | The class is to unify in the native-door zone ruling unit (NATIVE-DOOR-ZONE-RULING-1). |
| C-028 | The ZONE-SEAM-1 perf set on the fixed head is within 1.05x of base on every cell of the gate run (run 2: worst 1.036, plain INSERT New York). Run 1 has one cell at 1.053 (plain UPDATE UTC, one 76 ms outlier round), recorded in the subsection above. | The perf tables of the fix round 2 subsection (run 1 and run 2, ten alternating rounds, median of 10, 3 warm-ups); the census pin `test_head_replay_moves_exactly_the_step_2_cells` keeps the moves unchanged. | **PROVEN** | The measurement is taken by a lane-local harness that is not in the repository; the tables are its record. pins: write-spine-1/C-028 |
| C-029 | D21 runs the store-assignment gate at plan time (`gate_op` append). A micro-batch append of a TIMESTAMP into an INT column refuses at plan time with `append cannot store-assign column`, where main refused at the sink with `INSERT OVERWRITE cannot store-assign column`. Same class and types; the label changes and the refusal comes before any file is staged. | The WS2-35 measurement on one statement (fix round 2 subsection). | OPEN | The streaming track confirms the wording it wants (the streaming note beside WS2-17 in the CDC sink plan). |
| C-030 | Under the alternated base and head order with 30-run medians over at least 12 rounds (WS2-36), the head's plain INSERT cell is within 1.05x of base in UTC and in America/New_York. | The orchestrator's twelve-round alternated run (UTC plain INSERT 1.039, head slower in 6 of 12; New York 0.993, head slower in 7 of 12) and the Opus 5.5 round 2 controlled twenty-round alternated run (UTC 0.999, New York 0.994), in the [step (2) cost evidence file](../../wo/write-spine-1/evidence/step-2-cost-2026-10-26.md) and the order's Outcome line of 2026-10-26. | **PROVEN** | The measurements are taken by lane-local harnesses that are not in the repository; the tables in the evidence file are their record, as for C-028. The citation the grammar reads sits in the conform stage map (`pins: write-spine-1/C-030`), the shape C-028 uses in the timestamps map. pins: write-spine-1/C-030 |
| C-031 | The Opus 5.5 verifier's mutant C8 (the case-insensitive fallback in `resolve_target`, applied only when the frame width equals the target width) survives all 6,183 tests of the four crates. It changes the stage's answer for a width-mismatched frame and stores no row: D3 builds full-width projections, and D21's sink refuses the column-count mismatch. | The Opus 5.5 round 2 verdict, item 3 (own mutant C8): 13 of 13 `write_door_native` tests and 6,183 of 6,183 four-crate tests green under C8; the stage-API probe on a width-mismatched New York frame answers differently under C8 and stores nothing on either build; the D21 micro-batch probe refuses on both builds and stores nothing. | OPEN | Graded S3. A pin is owed by WRITE-SPINE-1 step 4 or NESTED-LITERAL-SEAM-1. |
