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
| (a) | `door_of` returning `None` for one writing arm | waits for the matrix (step 3b) | not run |

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
  executor casts through `store_kernel_sql`). `router.rs` is 979 lines.
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

## PROPOSITION LEDGER — WRITE-SPINE-1 steps (1) and (3) — 2026-10-24

| id | claim | evidence | status | proof |
|---|---|---|---|---|
| C-001 | Every UDF name both `repark-functions` and `repark-iceberg` name is one constant in `repark_common::udf_names`, and no call site changed. | `grep -rn '"__repark_' crates/` shows each of the seven strings once, in `udf_names.rs`. | **PROVEN** | the grep above; the step 1a diff changes only the seven definitions into re-exports; `cargo build --workspace`, `make rust-clippy` green. |
| C-002 | A session whose NTZ wall kernel is not registered refuses an `INSERT … BY NAME` of an instant into a `TIMESTAMP_NTZ` column, where main stores the UTC wall (corrected 2026-10-24 from the order's "a wall into a `TIMESTAMP` column", measured above). | `store_kernel_registry.rs`, red on main, green on head. | **PROVEN** | `missing_wall_kernel_refuses_instead_of_storing_utc`: red with the base lookup, green on head; the same test's registered control stores `1767305045123456`. |
| C-003 | `WriteDoor::ALL` holds exactly the eighteen value-writing doors of the evaluation's table, and `door_of` is an exhaustive match over the dispatched statement shapes. | `write_door.rs`; the enumeration tests; a removed variant reds them. | **PROVEN** | `write_door_ids_are_the_evaluation_table_ids_and_the_first_ten_are_spark_doors`; `door_of` lists every `Statement` variant with no wildcard (compiles under `make rust-clippy`); the two `door_of` unit pins; mutant (b). The four choices of step 3a above await the orchestrator. |
| C-004 | Every door has at least one spelling and every spelling names a door. | `every_write_door_has_a_spelling`, `every_spelling_names_a_door`. | **PROVEN** | `test_every_write_door_has_a_spelling`, `test_every_spelling_names_a_door` green; mutants (b) and (d) red them. The spelling-corpus test skips until lane B's `_write_spine_1_spellings.py` lands. |
| C-005 | The door matrix on this head equals main's recorded answers on every cell, and classifies against the Spark fixture as the step-0 census table says. | `test_write_spine_1_doors.py` against the two fixtures. | OPEN | step 3b. |
| C-006 | The four mutants of step 5 each red a named test. | the ledger's mutant table. | OPEN | (b), (c) and (d) red their tests (table above); (a) waits for the matrix. |
| C-007 | `zone_store_frame` returns the frame unconverted when the session analyzer fails on the planned write (`analyzed_types` answers `None`). | Reading of `ntz_store.rs` (`let Some(planned) = analyzed_types(…) else { return Ok(frame) }`). | OPEN | fail-open; closed by step (2)'s conform stage. |
| C-008 | `zone_store_frame` returns the frame unconverted when the planned width differs from the target width. | Reading of `ntz_store.rs` (`if planned.len() != targets.len() { return Ok(frame) }`). | OPEN | fail-open; closed by step (2)'s conform stage. |
| C-009 | The eighteen-door table is corrected (orchestrator ruling, 2026-10-24): D14 is bare-session SQL through `repark.sql()` in `repark-core`; D15 to D20 are `repark-sql` Rust-library doors with reach `rust_api`; D18 and D21 are unchanged. | `_write_spine_1_doors.py` marks D15 to D20's spellings `rust_api`; `test_every_rust_api_only_door_names_a_rust_test_that_exists`. | **PROVEN** | the enumeration test green; `rust_api_only_doors()` is exactly D15 to D20. |
| C-010 | `write_door_of(sql, options)` names the door of each Spark spelling without running it, through the router's intercepts and `door_of` (the parsed-shape reading). | `test_write_door_of_names_the_door_of_each_spark_spelling_without_running_it`. | **PROVEN** | 12 parametrized cases green: D1 to D10 and `None` for DELETE and SELECT. |
| C-011 | A Spark session records the set of doors its last statement took, at the branch points, cleared per statement; a Postgres target and `repark.sql()` record nothing. | `write_door_record.rs`; `test_last_write_doors_records_the_doors_each_statement_took`; `test_the_bare_sql_session_records_no_door`. | **PROVEN** | the three Rust pins and both Python pins green; mutants (e), (f) and (g) red them. The Postgres route is by reading (it returns before any record); no Postgres pin. An `UPDATE` the fork provider runs records D10 (round 3, `an_update_the_fork_provider_runs_records_the_update_door`). A `CALL` writes no new values, so the statements it runs through `router::execute` own the record (ruling Q8). |
| C-012 | Every door no Python spelling executes (D15 to D20) is written through by a named Rust test that reads the rows back. | `crates/repark-sql/tests/write_door_native.rs`; the enumeration test's name check. | **PROVEN** | six tests green; `RUST_TESTS` names each and the enumeration test finds each `async fn`. |
