# Unit ledger — PERF-UNPIVOT-1 · native `stack()` unpivot; `describe` back on a pure plan

**Retires:** this ledger moves to `../completed/` in the unit's last commit (step 2).
This file closes when PERF-UNPIVOT-1 merges, or when the owner closes the slate row.

**Unit:** PERF-UNPIVOT-1 · **Date:** 2026-09-12 · **Branch:** `perf/unpivot-1`
**Model:** grok-4.6 (step 1, `perf/unpivot-1`) · swe-2-high (step 2, `perf/unpivot-1-s2`)
**Slate:** card PERF-UNPIVOT-1. Step 1 of 2 shipped the primitive + pins (#542); step 2
of 2 (this round) moves `describe`/`summary` back to a pure plan (aggregate →
stack-order projection → `UnpivotExec`) and deletes the `mapInArrow` bridge.

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
| C-008 | `describe`/`summary` plan = scan → one aggregate → one unpivot node, no `mapInArrow`/Python bridge anywhere in the physical plan. | `test_describe_plan_is_scan_aggregate_unpivot` (`_map_bridge is None`, `UnpivotExec` + `AggregateExec`, `TableScan`=1, no `mapInArrow`); red-first output below. | **PROVEN** |
| C-009 | The DF-DESCRIBE-STR-1 and PERF-DESCRIBE-1 suites pass with every accepted-answer assertion unchanged (D-3). | The C-009 note below; the `-k "describe or summary"` run in the gates table. | **PROVEN** |
| C-010 | The laziness pin stays green unchanged: constructing `describe()` runs no scan. | `test_describe_runs_nothing_until_an_action` — unchanged, green in the gates table. | **PROVEN** |
| C-011 | `_summary_unpivot` and the bridge helpers are deleted (no dead code); `statistics.py` stays under its ceiling with baselines honest. | The C-011 note below: file 372→337 under the 1000 default; no EXCEPTIONS row exists in `scripts/check_lib_py.py` or the CAP-1 mirror to ratchet. | **PROVEN** |
| C-012 | `describe()` measured on a RELEASE module (`__debug_assertions__` False): 500-column × 200k and 20-column × 200k, warmup + three reps, medians, base (bridge) vs branch (plan). | The C-012 table below; raw reps in `scratch/perf-unpivot-1/describe_{base,branch}.json`. | **PROVEN** |
| C-013 | Gates: the two named suites + the laziness pin + the plan-shape pin + `test_perf_unpivot_1.py`; `make verify`; the whole parity suite. | The step-2 gates table below. | **PROVEN** |

`LOGIC_SCORE` = **13/13 `PROVEN`**.

## Step 2 (swe-2-high, `perf/unpivot-1-s2`, 2026-09-12)

`describe`/`summary` lower to `stack` over the single aggregate row: the chunked
aggregates (unchanged, one per ~50 columns, cross-joined) now emit raw cells, one
projection lays out stat-name literals plus one `CAST(cell AS Utf8)` per column per
stat row in row-major order, and `stack_dataframe` (`UnpivotExec`) emits the grid.
Every cell is cast because a describe column mixes count/mean/stddev/min/max — the
cast moved OUT of the aggregate into the projection because `docs/perf/cast-cost-2026-09-12.md`
measures projection-side casts ~13× cheaper than in-aggregate at 2500 (13.8 s vs
175.9 s). All-string cells keep every answer byte-identical (D-3).

### C-008 red-first

On the base tree the bridge is present, so the pin fails:

```
described = frame.describe()
>       assert described._map_bridge is None
E       AssertionError: assert {'parent': DataFrame[__repark_stat_0: bigint, ...
'arrow_schema': summary: string\nf0: string\nf1: string} is None
FAILED python/repark/tests/test_perf_unpivot_1.py::test_describe_plan_is_scan_aggregate_unpivot
1 failed in 0.66s
```

### C-009 — the mechanism line moved; every answer pin is byte-identical

`test_describe_scans_the_source_once` pinned the old mechanism
(`described._map_bridge is not None`, `_map_bridge["parent"]._explain_text`, and the
in-aggregate cast inventory `CAST(min(`/`CAST(max(`). A pin asserting the bridge
exists cannot coexist with the card's own "no `mapInArrow` bridge" deliverable, so
the test's mechanism assertions were rewritten to the new shape (`_map_bridge is
None`, `UnpivotExec`, `mapInArrow` absent, one `CAST(__repark_stat_` per cell in each
plan rendering); every answer assertion (`collect() == [...]`, the laziness spy, the
duplicate-stat rows) is unchanged, and DF-DESCRIBE-STR-1's pins in
`test_examples_dataframe_{a,c,d}.py` are untouched.

### C-011 — no baseline row to ratchet

`_summary_unpivot`, `positions`, `functools`/`pyarrow` imports, `arrow_schema`,
`out_schema`, `engine_stringified` and the `mapInArrow` call are deleted; the
`StructField`/`StructType` imports went with them. `statistics.py` 372→337 stays under
the 1000-line default — it holds no EXCEPTIONS row in `scripts/check_lib_py.py`, so
the CAP-1 mirror (`_PYTHON_BASELINES`) has no row either; both tables are unchanged
and `test_cap_1_exception_tables_equal_the_measured_debt` stays green.

### C-012 — release-module medians (2026-09-12)

`.venv/bin/maturin develop --release` in `python/repark`; `repark._native.__debug_assertions__`
is False. Frame: `spark.range(200_000)` + `id % 997 + i` double columns `.eager()`;
one warmup + three timed reps per shape; `call` = `describe()` plan build, `collect`
= the action; medians. Raw: `scratch/perf-unpivot-1/describe_{base,branch}.json`.

| Shape | Base call (s) | Base collect (s) | Branch call (s) | Branch collect (s) |
|---|---:|---:|---:|---:|
| 20 cols × 200k | 0.002 | 0.047 | 0.004 | 0.051 |
| 500 cols × 200k | 0.100 | 10.19 | 0.42 | 12.94 |

The pure plan is ~27 % slower at 500 columns — the 2500 projection-side casts cost
what the cast-cost doc measured — and level at 20 columns. Not the PERF-CAST-1 wall
(the superlinear shapes that measured 35–176 s); the bridge's 21.5 s figure was a
debug build — on release it is 10.19 s. Both shapes stay far inside the linear
regime; `make develop` (debug) was restored before the gates.

### Step-2 gates

JVM was not started for these gates.

| Command | Exit |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_perf_describe_1.py python/repark/tests/test_perf_unpivot_1.py -q` | 0 — 12 passed (both named suites + laziness + plan-shape pin) |
| `.venv/bin/python -m pytest python/repark/tests -q -k "describe or summary"` | 0 — 37 passed, 6 skipped |
| `.venv/bin/python -m pytest python/repark/tests/test_facade_2_column_display_goldens.py -q` (in the combined run) | 0 — 3 passed |
| `python3 scripts/check_ledger_grammar.py` | 0 — 118 live ledgers clean |
| `make verify` | 0 |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q` | 0 — 749 passed, 1 skipped, 12 xfailed (669 s) |

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
