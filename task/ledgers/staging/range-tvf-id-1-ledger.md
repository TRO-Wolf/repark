# Unit ledger — RANGE-TVF-ID-1 · the range() table function names its column id like Spark

## Round 1 (2026-09-18)

**Date:** 2026-09-18 · **Branch:** `fix/range-id-1` · **Base:** `origin/main` `179d63aa` ·
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **Rubric:** STANDARD. `risk_tier: standard`.
**Registry:** `RANGE-TVF-ID-1` **FIXED 2026-09-18**.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Both SQL doors resolve `range` to DataFusion's built-in table
function, whose column is `value`; Spark 4.1.2 names it `id`, non-nullable, and
accepts 1–4 arguments with string coercion and a loud zero-step refusal. The
DataFrame door (`spark.range`) is already right. The fix is a RePark-owned
table function registered in `ReparkSessionBuilder::build`, so both doors move
together; `generate_series` keeps its `value` column.

**Not in this unit:** `generate_series` semantics, `STATUS.md`, `Cargo.toml`,
`Cargo.lock`, tier-2 live runs.

## PROPOSITION LEDGER — RANGE-TVF-ID-1 — 2026-09-18

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Every `range(...)` SQL value form answers `struct<id:bigint>`, non-nullable, with Spark's rows, on the facade door (`session.sql`) and the native door (`repark.sql`). | `test_sql_range_cell_answers_recorded_schema_and_rows` over `range_tvf_id_1/range_tvf_id_1_spark_oracle.json`; §1 verbatim commands. | **PROVEN** | Red on main for the named reason (§1a); green after the fix on the rebuilt release native: 29 passed (§4). |
| C-002 | The column-name contract holds: `SELECT id`, `r.id`, `r(x)` answer, `sum(id)` answers 45 nullable, and `SELECT value` refuses with `AnalysisException` (class only — RePark carries no `UNRESOLVED_COLUMN` token). | Same pin plus `test_sql_sum_cell_answers_rows_with_qualified_display_name` and `test_sql_range_refusal_matches_spark_class[select_value]`; §1 verbatim commands. | **PROVEN** | Green after the fix (§4) with one declared divergence: unaliased `sum(id)` renders `sum(range().id)` — the systemic table-function qualifier leak, identical on `generate_series` (`sum(generate_series().value)`), owned by the SQL door (FNP-6D residual, run 16c). Fixing the shared display path here would be a passenger rewrite, so the pin asserts the qualified name explicitly instead of laundering it. |
| C-003 | The argument contract holds: the 4-argument form is accepted with rows unaffected, negative step, empty range, and string coercion answer; zero step refuses with `AnalysisException` (class only — no `FAILED_FUNCTION_CALL` token). | Same pins over the `range_a_b_step_parts`, `range_negative_step`, `range_empty`, `range_string_arg`, `range_zero_step` cells; §1 verbatim commands. | **PROVEN** | Green after the fix (§4). The 4th argument is accepted and ignored without literal validation: DataFusion folds constant 4th args to literals before the provider sees them, so a literal check would be dead weight. |
| C-004 | The `spark.range(...)` DataFrame door answers `struct<id:bigint>`, non-nullable, with Spark's rows. | `test_spark_range_door_answers_recorded_schema_and_rows`; §1 verbatim commands. | **PROVEN** | Green on the release native before the fix (3 passed, §1a); kept as the regression pin. |
| C-005 | A dated FIXED registry row names the repark before/after, the Spark oracle, the pins, and the rationale. | Registry row `RANGE-TVF-ID-1` in `docs/spark-sql-iceberg-parity.md`. | **PROVEN** | Row `RANGE-TVF-ID-1` sits in §7 beside the function-parity FIXED rows (after `FN-APPROXPCT-ACC-1`): repark before/after, Spark recorded oracle, pins, rationale with the `sum(range().id)` residual, dated 2026-09-18. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## 1. Measured on the release native in `.venv` (verbatim)

### 1a. The pins are red on main for the named reason (2026-09-18)

```
.venv/bin/python -m pytest python/repark/tests/test_range_tvf_id_1.py -q -p no:cacheprovider -n 4
22 failed, 7 passed in 1.26s
```

The value-cell failure reads `assert 'struct<value:bigint>' == 'struct<id:bigint>'`
on both doors. The 7 passes are the two `range_zero_step` refusal cells (already
`AnalysisException`) and the three `spark.range` DataFrame-door cells (C-004).

Both doors resolve `range` today: `SELECT * FROM range(3)` answers
`value: int64 not null [(0,), (1,), (2,)]` through `session.sql` (the facade
Spark door) and through `repark.sql` (the native ANSI door) — probed on the
rebuilt release native before the fix.

## 2. Fix design (Rust-first)

New `repark-core` module `range_table.rs` owns `SparkRangeFunc`, a
`TableFunctionImpl` that coerces 1–4 literal arguments (Int64, Utf8 parsed as
Spark parses them, Null for the empty frame), refuses zero step as a planning
error, and delegates row generation to DataFusion's `GenerateSeriesTable` with
`include_end = false` under a non-nullable `id: Int64` schema.
`ReparkSessionBuilder::build` registers it over the built-in `range`, so the
facade door and the native door share the one registration; `generate_series`
is untouched. Rust pins live file-backed in `range_table/tests.rs`.

## 3. Targeted checks

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_range_tvf_id_1.py -q -p no:cacheprovider -n 4` | 0 — 29 passed |
| `test_profiles1_table_properties.py` + `test_date_fn_1.py` + `test_f1_sql_expander.py` (`-n 4`) | 0 — 84 passed |
| `test_perf_ice_catalog_io_1.py` (all range-seed tests; glue/s3tables acceptance and the peak-RSS subprocess excluded) | 0 — 33 passed |
| `cargo test -p repark-core range_table` | 0 — 12 passed |
| `cargo test -p repark-distributed --test local_executor` | 0 — 3 passed |
| `cargo test -p repark-distributed --features cluster --test multi_stage session_spill_dir…` | 0 — 1 passed |
| `cargo test -p repark-distributed --features cluster --test cluster_two_executors cancel_mid_flight` | 0 — 1 passed |
| `cargo clippy --locked -p repark-core --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 — clean (repo `rust-clippy` flags) |
| `cargo clippy --locked -p repark-core --lib -- -D clippy::disallowed_methods -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::todo -D clippy::unimplemented -D clippy::unreachable` | 0 — clean (repo `rust-panic-ban` flags, crate scope) |
| `cargo fmt --all --check` | 0 — clean |
| `.venv/bin/python -m ruff check` + `ruff format --check` on the new Python files | 0 — clean |

Per the brief's machine rule, `make verify`, `make preflight`, `make py-test*`, a
whole-workspace `cargo test`, the whole facade suite, and the parity harness were
deliberately not run.

## 4. Gate log

Step-2 commit: fixture, recorder, red pins, this ledger. `22 failed, 7 passed`.

Step-3 commit: `crates/repark-core/src/range_table.rs` (+ file-backed tests,
`range_table/map.md`, `src/map.md` row, one-line `build` registration) plus the
`value` → `id` migration in `test_profiles1_table_properties.py` and the three
`repark-distributed` test files. Rust: 12 passed; clippy (repo flags),
panic-ban (`--lib`), and `cargo fmt --check` clean. Pins: 29 passed on the
rebuilt release native (the `sum_id` schema-string cell became the declared
qualifier-name pin, C-002).

Step-4 commit: registry row `RANGE-TVF-ID-1` (**FIXED 2026-09-18**) plus this
ledger (C-005 PROVEN, verdict 5/5). Test map rows for the fixture, pins, and
migrations landed in steps 2–3.

## 5. Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: range-tvf-id-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every recorded cell is measured against the release native on both
        SQL doors (§1a red, §3 green) and the DataFrame door; each pin asserts the
        measured schema string, nullability, and rows, and the value cells redden
        when the column is `value`.
      artifacts: [python/repark/tests/test_range_tvf_id_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The Spark cells are the verbatim orchestrator recording (Spark 4.1.2,
        local[2], UTC), committed under range_tvf_id_1/ with SHA-256; the committed
        `_record_range_tvf_id_1.py` driver re-derives them and exits non-zero on drift.
      artifacts: [python/repark/tests/_record_range_tvf_id_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Refusal paths are pinned by class on both doors (zero step, SELECT
        value); the 4-argument acceptance, negative step, empty frame, and string
        coercion are pinned as answers. No silent path exists.
      artifacts: [python/repark/tests/test_range_tvf_id_1.py, crates/repark-core/src/range_table/tests.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The new provider holds no mutable state (pure argument coercion over
        DataFusion's generator); registration runs once inside the sync build().
      artifacts: [crates/repark-core/src/range_table.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Memory sessions under temp dirs only; no network, no credentials, no
        AWS; the Spark oracle arrived as a recorded file, not a live JVM run.
      artifacts: [task/ledgers/staging/range-tvf-id-1-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Every pinned value is a measured value (§1, §3 verbatim blocks), not
        prose: the `id` schema strings, the row lists, the AnalysisException classes,
        the declared `sum(range().id)` display name.
      artifacts: [python/repark/tests/test_range_tvf_id_1.py]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim anywhere in the change.
    - id: AT-8
      status: ATTACKED
      evidence: `git status` shows only the fixture, pins, recorder, provider plus
        tests, the two maps, the three fallout migrations with map notes, the registry
        row, and this ledger; no Cargo.toml, lockfile, workflow, or pin change.
      artifacts: [task/ledgers/staging/range-tvf-id-1-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: The fix lives in its registered home (registry row RANGE-TVF-ID-1);
        the fixture map, the tests map, the staging map, the core src map, the
        range_table map, and the distributed-tests map carry the entries in the same
        commits.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: N/A
      justification: Single-round fix unit; no prior round to regress.
  complete: true
```

## Hand-back

`Model: muse-spark-1.3-contributor`. `risk_tier: standard`. Round 1 CONCLUDED with all
five clauses PROVEN, pins green, comment-ban hits=0.
