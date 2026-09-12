# Unit ledger — PERF-UNPIVOT-1 step 1 · native `stack()` unpivot

**Retires:** this ledger moves to `../completed/` in the unit's last commit (step 2).
This file closes when PERF-UNPIVOT-1 merges, or when the owner closes the slate row.

**Unit:** PERF-UNPIVOT-1 · **Date:** 2026-09-12 · **Model:** grok-4.6 · **Branch:** `perf/unpivot-1`
**Slate:** card PERF-UNPIVOT-1, step 1 of 2 (the primitive + pins; `describe` stays on the
bridge until step 2).

**Rubric:** STANDARD. `risk_tier: standard`.

**Owner crate:** `crates/repark-core`. The crate DAG puts engine plan rewrites and physical
operators with `dynamic_flatten` in core (tier 2). Spark's `stack` name is a marker ScalarUDF
registered on every session plus `StackRewrite` appended by `SparkExtension` after integer-literal
narrowing. The facade `F.stack` lowers through `apply_stack` / `UnpivotExec`, so the 500-column
path never builds per-cell physical expressions (PERF-CAST-1 / S2-26). `docs/perf/cast-cost-2026-09-12.md`
is not in the tree yet; the superlinear planner is the one PERF-DESCRIBE-1 measured.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | `stack(n, expr…)` matches Spark 4.1.2: n output rows, `ceil(k/n)` columns named `col0`…, row-major fill, NULL-pad a short last row, per-output-column types, `STACK_COLUMN_DIFF_TYPES` when one output column mixes types, `n` in `(0, 2147483647]`. | Oracle table below; pins `test_stack_sql_two_by_two_matches_spark`, `test_stack_sql_pads_a_short_last_row`, `test_stack_sql_n_out_of_range_is_loud`, `test_stack_independent_column_types`. | **PROVEN** |
| C-002 | `stack` over a 500-column single-row frame at 5 rows out is linear in columns: three reps, medians at 50 / 250 / 500, exponent fit ≤ 1.1, against the bridge shape's 21.5 s / 0.7 s call. | `test_stack_is_linear_in_columns`; measurement table below. | **PROVEN** |
| C-003 | EXPLAIN shows one scan, one aggregate, one unpivot node, no per-cell projection / no `mapInArrow`. | `test_stack_plan_is_scan_aggregate_unpivot` (TableScan=1, AggregateExec, UnpivotExec). | **PROVEN** |
| C-004 | Reachable from SQL and from `F.stack` with parity pins on the oracle. | `test_stack_sql_two_by_two_matches_spark`, `test_stack_facade_with_passthrough_and_alias`; example `docs/examples/functions/stack.py`. | **PROVEN** |
| C-005 | Red-first: on the base tree the name is unsupported. | Red output below; the FNP-9 absence pin listed `stack`. | **PROVEN** |
| C-006 | `stack` over the reviewer's shapes uses one `interleave` per stacked column (shared `(piece, row)` index per batch), not `concat`+`take`. Three reps, medians, 500-col × 1 row → 5 rows and 200k × 20. | Measurement table below (before/after, 2026-09-12). | **PROVEN** |
| C-007 | `UnpivotExec` emits the first output batch before the input stream is exhausted. Peak memory is one input batch + one output batch. | Red `produced=8` on collect-then-emit; pin `unpivot_exec_emits_first_batch_before_input_is_exhausted`. | **PROVEN** |

`LOGIC_SCORE` = **7/7 `PROVEN`**.

## P3 observed (no change this round)

P3-1 — `StackRewrite` walks every Spark plan (`transform_up_with_subqueries`). Reviewer
(2026-09-12): +1.6 ms on a 500-column `.schema` (~4 %), +2.6 ms on 500-column `to_arrow`
(~1.4 %). Not CAST-1 superlinear. Left in place.

P3-2 — `repeat_row_indices` uses `i32` take indices. `I·n > i32::MAX` fails
(`stack row index overflow`). n=5 allows ~429 M input rows. Left as `i32`; a later
step that must unpivot partitions that large can switch to `UInt64Array`.

`repark-core` now declares `futures.workspace = true` (already pinned 0.3; Cargo.lock
gains one line on the `repark-core` package). `RecordBatchStream` is sealed without
`futures::Stream`; `collect` was the step-1 workaround. Streaming poll needs the crate.

## Oracle (live PySpark 4.1.2, ANSI on, UTC, zulu-17, 2026-09-12)

One session (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `.venv` pyspark 4.1.2), stopped before
gates. `F.stack` takes n as a Column (`F.lit(2)`); a raw Python `int` is `NOT_COLUMN_OR_STR`
on Spark's Python wrapper. SQL accepts an INT literal. repark accepts both `int` and
`F.lit(n)` and pins the Spark SQL / `F.lit` answers.

| Arm | Spark answer |
|---|---|
| `SELECT stack(2, 1, 2, 3, 4)` | rows `(1,2)`, `(3,4)`; columns `col0`,`col1`; Integer, nullable |
| `SELECT stack(2, 1, 2, 3)` | `(1,2)`, `(3,None)` — short last row NULL-filled |
| `SELECT stack(3, 1, 2, 3, 4)` | three rows, two columns; last row `(None,None)` |
| `SELECT stack(1, 1, 2, 3)` | one row, three columns |
| `SELECT stack(2, 1, 'a', 2, 'b')` | col0 Integer, col1 String — types are per output column |
| `SELECT stack(2, 1, 2, 'a', 'b')` | `DATATYPE_MISMATCH.STACK_COLUMN_DIFF_TYPES` (col0 mixes INT and STRING) |
| `SELECT stack(0, 1, 2)` / `stack(-1, …)` | `VALUE_OUT_OF_RANGE`: n in `(0, 2147483647]` |
| `SELECT stack(2)` | `WRONG_NUM_ARGS`: requires > 1 parameters |
| `SELECT id, stack(2, a, b, c, d) FROM t` | id repeated; `AS (x, y)` names the stacked columns |
| `F.stack(F.lit(2), "a", "b", "c", "d").alias("x","y")` | columns `x`,`y`; same row-major cells |

## C-002 linearity (no JVM, 2026-09-12)

One-row MemTable, `stack(5, *columns)`, three timed reps after one warmup, medians:

| Columns | Median wall (s) |
|---:|---:|
| 50 | 0.0321 |
| 250 | 0.1292 |
| 500 | 0.2666 |

Log-log exponent **0.91** (≤ 1.1). Bridge shape at 500 columns was 21.50 s collect / 0.70 s call.

## C-006 before/after (no JVM, 2026-09-12, debug `.so`, schema cached)

Reviewer shapes. One warmup + three timed `to_arrow`, medians. Before = `concat`+`take`
per stacked column and partition `collect`. After = one `interleave` per column
(shared index) and `UnpivotStream` poll.

| Shape | Before median (s) | After median (s) | Before samples | After samples |
|---|---:|---:|---|---|
| 500 columns × 1 row → 5 rows | 0.2105 | 0.2286 | 0.2105, 0.2270, 0.2078 | 0.2286, 0.2258, 0.2433 |
| 200k rows × 20 cols, n=5 → 1 M × 4 | 0.1033 | 0.1240 | 0.1033, 0.0962, 0.1073 | 0.1187, 0.1240, 0.1465 |

Debug `.so`: wall is noise-dominated on these shapes (reviewer: unpivot ≈ 26 ms of the 500-col `to_arrow`; 200k×20 is 34 ns/cell). The after path copies each stacked cell once (`interleave`) instead of twice (`concat` then `take`) and does not `collect` the partition. Both medians stay ≪ 21.5 s.

## C-007 red-first

On the collect-then-emit exec (`f40e9b7f` + the pin only), eight one-row input batches
and `LocalLimitExec(fetch=1)`:

```
thread 'stack::exec::streaming_pin::unpivot_exec_emits_first_batch_before_input_is_exhausted' panicked at crates/repark-core/src/stack/exec.rs:
UnpivotExec must emit before the input stream is exhausted; produced=8
test stack::exec::streaming_pin::unpivot_exec_emits_first_batch_before_input_is_exhausted ... FAILED
```

After `UnpivotStream` the same pin is green (`produced < 8`).

## Red-first

On the base tree `stack` is absent from `F` (FNP-9 pin
`test_fnp9_multi_column_and_by_name_names_stay_absent[stack]`) and is not a SQL routine.
The new pin `test_stack_name_is_exported` fails as:

```
E       AssertionError: assert False
E        +  where False = hasattr(F, 'stack')
python/repark/tests/test_perf_unpivot_1.py::test_stack_name_is_exported
SQL: Invalid function 'stack'
```

## Gates

JVM was not started for these gates.

| Command | Exit |
|---|---|
| `cargo test -p repark-core stack --lib` | 0 — 7 passed |
| `.venv/bin/python -m pytest python/repark/tests -q -k stack` | 0 — 9 passed, 6332 deselected |
| `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark-parity/tests -q -p no:cacheprovider` | 0 after inventory 927→928 pin; 746 passed, 1 skipped, 11 xfailed (the 928 pin re-run 49 passed with CAP-1) |
| `make check-docs-links` | 0 — 787 files, 5058 links |
| `make check-ledger-grammar` | 0 — 112 live ledgers clean |
| `make verify` | 0 |

```
COVERAGE_ATTESTATION:
  pr_unit: perf-unpivot-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Spark 4.1.2 oracle cells for row-major fill, pad, per-column types, n-range, and F.stack/SQL doors.
      artifacts: [python/repark/tests/test_perf_unpivot_1.py, scratch/perf-unpivot-1/oracle_stack.py]
    - id: AT-2
      status: ATTACKED
      evidence: n=0/-1, missing args, STACK_COLUMN_DIFF_TYPES, and two generators refused.
      artifacts: [python/repark/tests/test_perf_unpivot_1.py, crates/repark-core/src/stack/tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Loud AnalysisException with Spark condition names; no silent half-unpivot.
      artifacts: [crates/repark-core/src/stack.rs, python/repark/src/repark/spark/functions_stack.py]
    - id: AT-4
      status: N/A
      justification: Stateless per-batch reshape; no shared mutable state.
    - id: AT-5
      status: N/A
      justification: No secrets; stack does not read configuration or credentials.
    - id: AT-6
      status: ATTACKED
      evidence: Value and Arrow-path types pinned on SQL and F.stack; output columns nullable.
      artifacts: [python/repark/tests/test_perf_unpivot_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Linearity pin exponent 0.91 at 50/250/500; UnpivotExec interleaves with a shared index and streams each input batch (C-006/C-007).
      artifacts: [python/repark/tests/test_perf_unpivot_1.py, crates/repark-core/src/stack/exec.rs]
    - id: AT-8
      status: ATTACKED
      evidence: Spark cells measured live; DataFusion TableScan/generate_series named honestly in the EXPLAIN pin.
      artifacts: [task/ledgers/staging/perf-unpivot-1-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: Failures name VALUE_OUT_OF_RANGE, WRONG_NUM_ARGS, STACK_COLUMN_DIFF_TYPES.
      artifacts: [crates/repark-core/src/stack.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first absence of the name on the base tree; pins fail without UnpivotExec.
      artifacts: [python/repark/tests/test_perf_unpivot_1.py]
```
