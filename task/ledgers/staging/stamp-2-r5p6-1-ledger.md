# Unit ledger — STAMP-2-R5P6-1 · attribute and remove the r5p6 excess

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move).

**Unit:** STAMP-2-R5P6-1 · **Date:** 2026-10-07 · **Model:** claude-opus-5-5 (high) · **Branch:** `perf/stamp-2-r5p6-1` · **Base:** `13de60e1` (the #968 head, stack `wip/feat/attr-id-1-2026-10-01`)

**Rubric:** STANDARD. `risk_tier: standard`.

**Owner ruling (2026-10-07):** no carve-out for #968. The release confirmation measured
1.1053 against the ≤ 1.10 limit (work-equal like median of 3); r5p6 carried 3.17 s of the
4.13 s excess (12,272 cells, ratio 1.184).

## Scope

1. Attribute the excess with cProfile (and perf) on 500 r5p6 cells on `7f45e460` and `13de60e1`.
2. Make the sort lineage trace and the twin search lazy where the sort key is unique. If the
   cost lies elsewhere, fix it there with the same rule (lazy where the answer is already
   settled) and record why.
3. Pre-measure; the gate record is the orchestrator's quiet three-run confirmation.

The full attribution, with every table and artifact path, is
`/tmp/oc-worker/direct/wo/attr-id-1/stamp2-r5p6-1/attribution.md`.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The attribute-token select plans natively only for an attribute-exact projection: every token held by the frame, the join SQL a bare token or `coalesce(token, <0..999999999 \| CAST(<that> AS T)>)` with `T` in C-007's whitelist, each token replaced by the first-held engine reproducing the column's `_sql_expr`, and the native output names equal to the SQL route's. Any other projection keeps the SQL route unchanged. | `test_select_coalesce_route_follows_the_literal_shape`, `test_inexact_fill_literal_keeps_the_sql_replan`, `test_second_twin_reference_keeps_the_sql_replan`; mutations M2, M3, M4 below. | **PROVEN** |
| C-002 | `fillna` (scalar and mapping) builds each position from the bound column at the first position holding its id whenever a target carries an id and the frame takes the attribute-token route, so twin fills skip the SQL re-plan with the SQL route's answers (columns, dtypes, rows, display/engine names, id sharing), including a union whose later input differs at the twin positions. | `test_twin_fill_skips_the_sql_replan_with_equal_answers`, `test_twin_fill_mapping_skips_the_sql_replan_with_equal_answers`; mutation M1 below. | **PROVEN** |
| C-003 | The binding semantics do not change: the same refusals, the same display-name renames, the same answers on every attr-id pin, the sort grid and the SM-2 pins. | The C-003 evidence table below. | **PROVEN** |
| C-004 | A unique sort key binds without the lineage trace or the twin search, on the Rust binder and on the facade door; an ambiguous Project key reaches them. | `bind_free_names_sort_binds_unique_key_without_tracing_lineage` (`SORT_TRACES`, `#[cfg(test)]` only) and `test_unique_sort_key_binds_without_the_lineage_trace`; the forced-trace mutation below. | **PROVEN** |
| C-005 | Attribution: no stack code carries the 0.64 pt between `7f45e460` and `13de60e1`; the r5p6 excess over main is construction-side and the largest single block is the `fillna` SQL re-plan through `sql_built`. | The attribution table below and `attribution.md` §1–§3. | **PROVEN** |
| C-007 | The CAST arm admits only `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE` and `DECIMAL(p,s)`: the targets whose native cast of `0`, `1` and `999999999` equals the SQL route's in value, type, nullability and error, measured on a twin column of the target type and across a 13-type twin matrix (overflow on `TINYINT`, `SMALLINT` and narrow decimals raises the same error on both routes). Every other target (`TIMESTAMP`, `DATE`, `TIMESTAMP_NTZ`, `STRING`, `BOOLEAN`, `BINARY`, nested CASTs) keeps the SQL route, so `coalesce(<TIMESTAMP, DATE, TIMESTAMP_NTZ or void twin>, CAST(n AS TIMESTAMP))` answers `n` seconds as head and Spark do, not `n` microseconds (verifier S1). | `test_coalesce_cast_to_a_native_type_skips_the_sql_replan`, `test_coalesce_cast_to_a_datetime_keeps_the_sql_replan`; mutations M6, M7, M8 below. | **PROVEN** |
| C-008 | A native exactness attempt that raises (the native `select`, the name read or the spawn in `_attr_exact_plan`) is a miss: `_attr_exact_child` returns `None` and the SQL route runs unchanged, so a refusal keeps head's SQL-route text (`coalesce(Boolean, Int64)`, not the native planner's `coalesce(Boolean, Int32)`; verifier S2, 117 grid cells). | `test_raising_native_probe_keeps_the_sql_route_refusal`; mutation M9 below. | **PROVEN** |
| C-009 | The literal bounds hold on both arms: `1000000000` (ten digits), `-1` (a sign) and `1.5` (a fraction), bare and as `CAST(<that> AS BIGINT)`, keep the SQL route with equal answers, while `0` and `999999999` bare take the native route (verifier S3). | `test_out_of_shape_literals_keep_the_sql_replan`; mutations M10, M11, M12 below. | **PROVEN** |
| C-010 | `SORT_TRACES` (`#[cfg(test)]` only) counts all four lineage doors (`sort_hits_meet_at_join`, `sort_sourced_twin_engine`, `sort_input_carries_twice`, `sort_output_carries_twice`): a unique key leaves it unmoved and the ambiguous Project key moves it by exactly 4, one per door. The facade spy names the same four; `sort_output_carries_twice` has no `_native` export, so the spy asserts it stays unexported and patches the doors the facade can call (verifier S3). | `bind_free_names_sort_binds_unique_key_without_tracing_lineage`, `test_unique_sort_key_binds_without_the_lineage_trace`; mutations M13, M14 below. | **PROVEN** |
| C-006 | Pre-measure: the fix brings the whole like set under 1.10 and r5p6 from 1.18 to about 1.13 against main `575f57ca`, single runs by `l4l_gate.py`'s method. | The pre-measure table below. | **PROVEN** |

`LOGIC_SCORE` = **10/10 `PROVEN`**.

## Attribution table (C-005)

| Comparison | Measurement | Result |
|---|---|---|
| base `7f45e460` → head `13de60e1`, 500 cells, cProfile ×2 | work ratio | 0.9885, 1.0693; Python delta ≈ 5 ms (USING-mark bookkeeping); no native per-call change |
| base → head, full r5p6, cell median of 3 | work ratio | **0.9975** |
| base → head, other 23 families, cell median of 2 | work ratio | **1.0032** |
| main → head, r5p6 like cells | `fn()` / collect | +2.70 s / +0.25 s (85 % construction) |
| main → head, r5p6 like cells, cProfile | `sql_built` | +0.84 s: `_select_via_attr_sql` (fill) 0 → 283 calls, `_join_on_condition_h1` 112 → 466 |
| main → head, r5p6 like cells, cProfile | `stamp_attribute_ids`, `__arrow_c_stream__`, `count` | +0.31 s, +0.31 s, +0.10 s |
| main → head, r5p6 like cells, perf self | interpreter, metadata `HashMap` clones, allocator | +0.34 s, +0.20 s, +0.11 s |
| main → head, construction micro | `fillna(0, subset=["v"])` | 177 µs → 1,075–1,253 µs |
| sort trace doors on 12,272 like cells | calls | 12 (twin search), 66 (`sort_hits_meet_at_join`) |

The 0.64 pt between the 2026-10-06 record (1.0964 against `4a643e56`) and the 2026-10-07
confirmation (1.1053 against `575f57ca`) is smaller than the confirmation's own three-run
spread (0.0124) and spans two different mains; `4a643e56` was not rebuilt, so the main-side
share is inferred.

## The laziness rule

The attribute-token SQL route (scratch view of attribute copies, then `sql_built`) exists
to resolve `__REPARK_ATTR_` tokens: each token binds the first position of the frame holding
its id. When every column's native expression already names exactly those fields, with the
SQL text's own typing, the route's answer is settled and the re-plan is skipped. The
exactness test is C-001's four conditions. `fill` makes its twin positions satisfy them by
building from the first-held position — the field the route would read — instead of
relying on twin values being equal (a union breaks that). Outer, sort-marked and
metadata-carrying columns answer identically on both routes (probed), so they carry no
guard. The sort lineage trace and twin search were already lazy (C-004); this unit pins it.

## C-003 evidence

| Check | Result |
|---|---|
| Full replay corpus (24 families, 43,989 cells) on the fix against head's three gate runs | 0 deterministic changes; the only differences are cells on the gate's nondet list (`freqItems` order, `cp_*_join_parent` casing) |
| attr-id and sort suites (27 files, `-n 8`, release `.venv`) | 869 passed, 9 skipped, 3 xfailed — identical on the unmodified tree; 875 with this unit's 6 pins |
| Every suite touching `fillna` (13 files) | 486 passed, 65 skipped, 3 xfailed — identical on the unmodified tree |
| Sort grid (`reverify2/sort4`, 394 cells) | 394/394 equal to `s4_orch.json` and `s4_sm2d.json` |
| `cargo test -p repark-core --lib` | 1,246 passed, 1 ignored |
| Like-set membership in the pre-measure | 26,032 / 12,272 cells, unchanged — no outcome class moved |

## Mutations (red-first)

| ID | Mutation | Red |
|---|---|---|
| M1 | `_fill_sources` stops binding the first-held position | both fill-route pins |
| M2 | the literal-shape check accepts any shape | the decimal-fill and `coalesce`-shape pins |
| M3 | the binding-equivalence check accepts any spelling | the second-twin pin |
| M4 | the native-name check accepts any names | the `coalesce`-shape pin (`alias("a b")`) |
| M6 | the CAST alternative is deleted (verifier V9) | `test_coalesce_cast_to_a_native_type_skips_the_sql_replan` |
| M7 | the CAST whitelist gains `TIMESTAMP` | `test_coalesce_cast_to_a_datetime_keeps_the_sql_replan` |
| M8 | the CAST arm takes any type token again (`[A-Z_]+(\(…\))?`, verifier V8 and the S1 shape) | `test_coalesce_cast_to_a_datetime_keeps_the_sql_replan` |
| M9 | `_attr_exact_child` lets the native attempt's exception propagate | `test_raising_native_probe_keeps_the_sql_route_refusal` |
| M10 | either arm accepts a sign, `-?\d{1,9}` (verifier V1; bare and CAST run separately) | `test_out_of_shape_literals_keep_the_sql_replan`, both runs |
| M11 | either arm accepts ten digits, `\d{1,10}` (verifier V2; bare and CAST run separately) | `test_out_of_shape_literals_keep_the_sql_replan`, both runs |
| M12 | either arm accepts a fraction, `\d{1,9}(\.\d+)?` (verifier V3; bare and CAST run separately) | bare: the decimal-fill, `coalesce`-shape and bounds pins; CAST: the bounds pin |
| M13 | the `Bound` arm calls `sort_output_carries_twice` or `sort_input_carries_twice` (the verifier's escape; run separately) | the counter assertion (`attr_id_s3b.rs:460`), both runs |
| M14 | `sort_output_carries_twice` or `sort_input_carries_twice` stops counting (run separately) | the exact-count assertion (`attr_id_s3b.rs:463`), both runs |
| M5 | the twin search runs in the unique-key (`Bound`) arm | only the counter assertion (`attr_id_s3b.rs:460`); every binding answer and the other 97 `attr_id` tests stay green |

## Pre-measure (C-006)

`/tmp/xr5p6` (release) against `/tmp/xgatemain` (`575f57ca`, release), unpinned as `run.sh`,
interleaved main / head / fix, two rounds, `l4l_gate.py`'s method:

| Side, run | Whole like set | r5p6 family | 500 cells |
|---|---|---|---|
| head, 1 | 1.1035 | 1.1866 | 1.141 |
| head, 2 | 1.1034 | 1.1779 | 1.167 |
| **fix, 1** | **1.0668** | **1.1312** | 1.066 |
| **fix, 2** | **1.0766** | **1.1243** | 1.076 |

Per-cell medians of both rounds: whole like set 1.1035 → **1.0717**; r5p6 1.182 → 1.128
(−0.95 s); r5p6t −0.12 s; r5p7 −0.10 s. This is a pre-measure; the gate record is the
orchestrator's quiet three-run confirmation.

```
COVERAGE_ATTESTATION:
  pr_unit: stamp-2-r5p6-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked against its pin or measurement; the brief's ruled fix was measured first and found already in place (12 trace calls on 12,272 like cells), so the fallback rule was applied where the attribution puts the largest block and the reason is recorded.
      artifacts: [task/ledgers/staging/stamp-2-r5p6-1-ledger.md, python/repark/tests/test_stamp_2_r5p6_1.py, crates/repark-core/src/session/tests/attr_id_s3b.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Literal boundaries probed on both doors — 0, 7, 999999999 and 2147483647 type INT, 3000000000 BIGINT, decimals DECIMAL through SQL; the shape admits only up to nine digits and no sign; ten-digit, negative and fractional literals on both arms (C-009), decimal and spaced-alias inputs are pinned to the SQL route.
      artifacts: [python/repark/src/repark/spark/dataframe/join_attr_tokens.py, python/repark/tests/test_stamp_2_r5p6_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Refusals are unchanged — the token rewrite (and its MISSING_ATTRIBUTES refusal) still runs before the route decision; a raising native attempt falls back to the SQL route (C-008), so refusal text is head's; the self-join fill and a Boolean coalesce refuse identically on both routes and are pinned.
      artifacts: [python/repark/src/repark/spark/dataframe/join_attr_tokens.py, python/repark/tests/test_stamp_2_r5p6_1.py]
    - id: AT-4
      status: ATTACKED
      evidence: The SORT_TRACES counter is thread-local so parallel test threads cannot race it; the native route registers no scratch view, so no temp-view cleanup ordering remains on that path.
      artifacts: [crates/repark-core/src/session/df_guards/attr_lineage.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, input parsing or secret handling changes; the native route plans the frame's own expressions.
    - id: AT-6
      status: ATTACKED
      evidence: Full replay corpus of 43,989 cells shows no deterministic answer change; ids, display/engine names and dtypes are compared directly in the route pins.
      artifacts: [python/repark/tests/test_stamp_2_r5p6_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: The unit is a performance fix; cProfile, perf and the l4l pre-measure are recorded in attribution.md and above.
      artifacts: [task/ledgers/staging/stamp-2-r5p6-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: The SQL route's contract (first-held token binding, engine naming, needs-identity bookkeeping) is reused verbatim through _with_attr_names and _display_is_engine rather than re-derived.
      artifacts: [python/repark/src/repark/spark/dataframe/join_attr_tokens.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: Every added branch has a pinned input that changes its output (M1–M5); guards whose removal changed no output (outer, sort marker, alias metadata, token locality) were probed and deleted.
      artifacts: [python/repark/tests/test_stamp_2_r5p6_1.py, crates/repark-core/src/session/tests/attr_id_s3b.rs]
  reattested: []
  complete: true
```
