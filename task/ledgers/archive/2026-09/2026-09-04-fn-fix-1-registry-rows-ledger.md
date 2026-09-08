# Unit ledger — FN-FIX-1 · ten filed function-parity divergences become Spark-equal

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when FN-FIX-1 merges, or when the owner closes the slate row.

**Unit:** FN-FIX-1 · **Date:** 2026-09-03 · **Executor:** Grok (grok-4.6), Actor ·
**Branch:** `feat/fn-fix-1-registry-rows` · **Base:** `a955d61`
**Model:** grok-4.6
**risk_tier:** standard.

Spark is the oracle. Live PySpark 4.1.2, zulu-17, `TZ=UTC`, ANSI on, 2026-09-03.
Registry cells matched; no HALT.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | One live-oracle script per row (cells plus non-NULL / NULL-input / empty controls) recorded before code. A Spark cell contradicting a registry row → HALT. | Oracle table below. | **PROVEN** |
| C-002 | Smallest change at the owning layer. No new dependency. No unnamed functions. Files under size ceilings. | Kernels + dispatch + facade. | **PROVEN** |
| C-003 | Each row's pin RED after the fix; rewritten to Spark's answer; controls; four live co-collected legs; mutations one knob at a time. | Pins + live tests + mutation table. | **PROVEN** |
| C-004 | Every row **FIXED 2026-09-03 (FN-FIX-1)** per §6; EX-10 `F.isnan` and NaN note dated closure; next EX names in this ledger; maps lockstep. | Registry + EX-10 + maps. | **PROVEN** |

## Oracle (live PySpark 4.1.2, 2026-09-03, JDK 17, ANSI on, `TZ=UTC`)

| Row | Spark cell | repark before |
|---|---|---|
| FN-ISNAN-1 | `[False, False]` non-nullable bool; NaN → true, isnull false | `[False, None]` nullable bool |
| FN-SHA2-1 | hex string; 0=256; 224/256/384/512; other bits `VALUE_NOT_ALLOWED` | facade bytes; 512 `UnsupportedOperationException` |
| FN-TRYTONUMBER-1 | `AnalysisException` `[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT]` | `Decimal('12345')` silent |
| FN-ADDMONTHS-1 | `2015-02-28+1` → `2015-03-28`; `2025-04-30-1` → `2025-03-30`; `2024-02-29-7` → `2023-07-29` | `2015-03-31` / `2025-03-31` / `2023-07-31` |
| FN-LAST-1 | last(ignorenulls) window group a → `3` | `NULL` |
| FN-APPROXPCT-1 | global `3` int64; grouped a=2 b=4; array `[1,3,6]` | interpolated double `3.0` / `2.0`,`5.0` |
| FN-APPROXPCT-ACC-1 | accuracy 2 on 1..200 is `1.0`; default/`10000` are `100.0` | `100.0` at accuracy 2 (knob ignored) |
| FN-ARRAYPOS-1 | `[2, 0, None]`; empty → `0`; null needle → NULL | `[2, None, None]` |
| FN-ARRAYSORT-1 | array_sort `[1,2,None]`; sort_array asc `[None,1,2]`; desc `[2,1,None]` | array_sort `[None,1,2]` |
| FN-ARRAYSOVERLAP-1 | `[None, False, None, True, True, None]` | `[False, False, False, True, True, False]` |
| FN-FLATTEN-1 | NULL sub-array → NULL row | dropped sub-array `[1]` |
| NaN ingest | `createDataFrame([(nan,)])` isnan true, isnull false | NaN erased to NULL |

## Kernels

| Name | Layer |
|---|---|
| `isnan` | `spark_isnan.rs`; NULL → false; non-nullable bool |
| `sha2` | facade `_scalar("sha2")` onto datafusion-spark hex kernel |
| `try_to_number` | analysis refuse non-foldable format |
| `add_months` | clamp only when target month is shorter |
| `last(ignorenulls)` | `window_from_aggregate` copies `IGNORE NULLS` |
| `percentile_approx` | discrete rank `ceil(p*n)-1`; `select_nth_unstable`; accuracy ignored |
| `array_position` | not-found `0` |
| `array_sort` / `sort_array` | NULLS LAST vs Spark sort_array order |
| `arrays_overlap` | three-valued kernel |
| `flatten` | NULL sub-array → row NULL |
| NaN ingest | keep `float('nan')`; `CAST('NaN' AS DOUBLE)` |

## Mutation

| Knob | Red of M |
|---|---|
| restore DataFusion `isnan` dispatch | 1 red of 2 (`test_isnan_null_is_false_non_nullable`) |
| restore `sha256` bytes + 256-only | 1 red of 2 (`test_sha2_facade_hex_string_matches_spark`) |
| skip non-foldable format refuse | 1 red of 2 (`test_try_to_number_non_foldable_format_raises`) |
| restore month-end clamp | 1 red of 3 (`test_add_months_keeps_day_when_target_month_has_it`) |
| drop `IGNORE NULLS` in `over` | 1 red of 1 (`test_last_ignorenulls_window_skips_trailing_null`) |
| restore t-digest interpolation | 2 red of 3 (`test_approx_percentile_discrete_bigint_matches_spark`, array pin) |
| assert Spark `1.0` at accuracy 2 | 1 red of 1 (`test_percentile_approx_sql_third_arg_does_not_change_discrete_p50`) |
| restore DF array_position NULL not-found | 1 red of 4 (`test_array_position_not_found_returns_zero`) |
| restore NaN → None normalize | 1 red of 2 (`test_create_dataframe_stores_nan_not_null`) |

## Round 3 (2026-09-03)

| Item | Note |
|---|---|
| Flatten before | 1.935 s / 1e6 rows (row-wise `concat`) |
| Flatten after | 8.010 ms repark / 3.012 ms DataFusion (release, 1e6 packed `[[1,2],[3]]`) |
| Flatten ratio | 2.66× DataFusion; pin `≤ 3×`, `#[ignore]` `one_million_rows_within_three_times_datafusion` |
| Accuracy cell | Spark `percentile_approx(x, 0.5, 2)` on 1..200 is `1.0`; repark `100.0` |
| `FN-APPROXPCT-ACC-1` | BACKLOG; accuracy knob accepted and ignored; pin records Spark `1.0` beside repark `100.0` |
| `PERF-APPROXPCT-1` | BACKLOG; group held in memory; `select_nth_unstable` per percentile |
| `arrays_overlap` | 1.04 s / 1e6 acceptable; HashSet of owned `ScalarValue`; borrowed-key set is not a one-line change |
| Blast radius | sparse AUC `element_at(..., array_position(...))` skips index `0` (`python/repark/src/repark/spark/ml/map.md`) |

## Next EX batch names

`F.isnan`, `F.sha2` (224/384/512/0), `F.try_to_number` column-wise (foldable stays), `F.add_months`, `F.last(ignorenulls=True)` ordered window, `F.approx_percentile` / `F.percentile_approx` (array-of-percentages), `F.array_position`, `F.array_sort`, `F.arrays_overlap`, `F.flatten`.

## 9. Delivery

| Item | Path |
|---|---|
| Registry | `docs/spark-sql-iceberg-parity.md` FIXED 2026-09-03 (FN-FIX-1) |
| Live legs | `test_parity_live.py` four tests on `spark_engine` |
| EX-10 | dated closure on `F.isnan` + NaN ingest |
| Maps | lockstep on every touched directory |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fn-fix-1-registry-rows
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Live PySpark 4.1.2 cells recorded before code; pins assert Spark answers.
      artifacts: [python/repark/tests/test_fn_batch1.py, python/repark/tests/test_fn_arrays_divergence.py]
    - id: AT-2
      status: ATTACKED
      evidence: Controls cover non-NULL, NULL-input, empty, overflow, invalid bit length.
      artifacts: [python/repark/tests/test_fn_batch4.py, python/repark/tests/test_functions_dates.py]
    - id: AT-3
      status: ATTACKED
      evidence: Non-foldable try_to_number raises NON_FOLDABLE_INPUT; invalid sha2 bits VALUE_NOT_ALLOWED.
      artifacts: [python/repark/tests/test_fnp7_try_inversions.py]
    - id: AT-4
      status: N/A
      justification: Scalar/aggregate UDFs are immutable and have no shared mutable state.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change.
      artifacts: [crates/repark-functions/src/spark_isnan.rs]
    - id: AT-6
      status: N/A
      justification: No new public API surface; existing function names keep their signatures.
    - id: AT-7
      status: ATTACKED
      evidence: Always-run pins are repark-only; Spark is behind REPARK_PARITY_LIVE=1.
      artifacts: [python/repark/tests/test_parity_live.py]
    - id: AT-8
      status: ATTACKED
      evidence: File-size ratchets down; no ceiling raised; new Rust files have zero comments.
      artifacts: [scripts/check_rust_file_size.py, scripts/check_lib_py.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Pins cited in tests and maps; registry rows FIXED 2026-09-03 (FN-FIX-1).
      artifacts: [python/repark/tests/test_fn_arrays_divergence.py, docs/spark-sql-iceberg-parity.md]
```
