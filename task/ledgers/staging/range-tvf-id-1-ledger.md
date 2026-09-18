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
| C-001 | Every `range(...)` SQL value form answers `struct<id:bigint>`, non-nullable, with Spark's rows, on the facade door (`session.sql`) and the native door (`repark.sql`). | `test_sql_range_cell_answers_recorded_schema_and_rows` over `range_tvf_id_1/range_tvf_id_1_spark_oracle.json`; §1 verbatim commands. | **OPEN** | Release native at `179d63aa`: both doors answer `struct<value:bigint>` — red for the named reason, `assert 'struct<value:bigint>' == 'struct<id:bigint>'` (§1a). |
| C-002 | The column-name contract holds: `SELECT id`, `r.id`, `r(x)`, `sum(id)` answer, and `SELECT value` refuses with `AnalysisException` (class only — RePark carries no `UNRESOLVED_COLUMN` token). | Same pin plus `test_sql_range_refusal_matches_spark_class[select_value]`; §1 verbatim commands. | **OPEN** | `SELECT value FROM range(3)` answers on the release native (the column exists there), so the refusal pin reds (§1a). |
| C-003 | The argument contract holds: the 4-argument form is accepted with rows unaffected, negative step, empty range, and string coercion answer; zero step refuses with `AnalysisException` (class only — no `FAILED_FUNCTION_CALL` token). | Same pins over the `range_a_b_step_parts`, `range_negative_step`, `range_empty`, `range_string_arg`, `range_zero_step` cells; §1 verbatim commands. | **OPEN** | The 4-argument form refuses on the release native (`range function requires 1 to 3 arguments`); the string form answers `value`; zero step already refuses with `AnalysisException` (§1a). |
| C-004 | The `spark.range(...)` DataFrame door answers `struct<id:bigint>`, non-nullable, with Spark's rows. | `test_spark_range_door_answers_recorded_schema_and_rows`; §1 verbatim commands. | **PROVEN** | Green on the release native before the fix (3 passed, §1a); kept as the regression pin. |
| C-005 | A dated FIXED registry row names the repark before/after, the Spark oracle, the pins, and the rationale. | Registry row `RANGE-TVF-ID-1` in `docs/spark-sql-iceberg-parity.md`. | **OPEN** | Row not yet written (step 4). |

VERDICT: 5 clauses, 1 PROVEN, 4 OPEN, 0 REJECTED.

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

Recorded in §4 on the unit's last commit.

## 4. Gate log

Step-2 commit: fixture, recorder, red pins, this ledger. `22 failed, 7 passed`.

## Hand-back

`Model: muse-spark-1.3-contributor`. `risk_tier: standard`. Round 1 in flight:
fixture committed, pins red on main for the named reason, fix not yet written.
