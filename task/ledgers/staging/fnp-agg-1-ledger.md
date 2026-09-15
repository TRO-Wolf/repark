# Charter ledger — FNP-AGG-1 · the missing aggregates (1.5 Spark-parity campaign)

**Date:** 2026-09-15 · **Branch:** `feat/fnp-agg-1` · **Base:** `bee2cde3` · **Model:**
muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `EX-FN-9` flips to FIXED in a later step; one section-7 row per residual
divergence measured there.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 1.5 Spark-parity campaign measured fourteen aggregate names against live
PySpark 4.1.2 (`/tmp/oc-worker/pa-agg/agg_misc_spark_oracle.json`, 2026-09-14): eleven are
absent from the facade on both doors and three (`kurtosis`, `skewness`, `mode`) are
engine-gap stubs. This unit builds the Rust kernels and facade names and pins every cell.

**Not in this round (run 16a, step 1 only).** No product code, no cargo: this round writes
the ledger, copies the fixture, and lands the red-first pins. Steps 2–5 (kernels, facade,
`count_min_sketch`, registry) are later rounds on build clones.

## Decisions (orchestrator rulings under G-2, copied from the card)

| Id | Ruling |
|---|---|
| D-1 | Kernels in `crates/repark-functions/src/` (new modules listed in its `map.md`), registered in `register_all` (`lib.rs`) for the SQL door. The facade reaches a new aggregate through the aggregate-kind match tables in `crates/repark-python/src/column/function_dispatch.rs` (`unary_aggregate_udaf`, `binary_aggregate_udaf`) — ADD arms only; run 15c converges existing rows there, so never edit or reorder existing arms. |
| D-2 | Never edit `dataframe/**`, `column.py`, `session/**`, `catalog.py`, `types.py`, the SQL parser/dialect. `cube`/`rollup` live in GroupedData (run 15b's files): use them as they are. |
| D-3 | Registry: flip `EX-FN-9` (kurtosis/skewness/mode) to FIXED with the new pins; add one section-7 row per residual divergence measured, each with a pin. |
| D-4 | Pins: `python/repark/tests/test_fnp_agg_1.py`, both doors, parametrized over the oracle cells, red first on the base tree with the red summary pasted into the ledger; Rust unit tests beside each UDAF. Floats compare exactly (Spark's value); nullability is pinned except the VALUES-derived grouping key. |
| D-5 | Performance: each UDAF implements `merge_batch`/`state` (multi-partition safe); `retract_batch` is NOT required. No per-row allocation in `update_batch` beyond what the algorithm needs. |

## Owner rulings applied in this round

| Id | Ruling | Applied |
|---|---|---|
| OWNER-2026-08-26 | No comments in code (`//`, `///`, `//!` in Rust, `#` lines in Python/TOML/YAML/shell beyond `# noqa`). | The pin file carries docstrings only; the comment-ban grep in §Gates. |
| OWNER-2026-09-14 | Rust first: every new function lands in Rust with the Python facade a thin wrapper. | Step scope: pins only, no product code either side. |
| OWNER-2026-09-14 | Shape rule: a name is done when it answers PySpark 4.1.2 on BOTH doors or raises Spark's own error class with a dated registry row. | The pins assert both doors plus error conditions; Spark-refused SQL names (`product`, `listagg_distinct`, `string_agg_distinct`, `sum_distinct`) are pinned as `UNRESOLVED_ROUTINE` refusals. |

## Proposition ledger

| Clause | Claim | Verdict | Evidence |
|---|---|---|---|
| C-001 | The sixteen card names are present on `repark.spark.functions` with PySpark 4.1.2 signatures, including `sum_distinct` / `sumDistinct` with the `FutureWarning`. | OPEN | Red on the base: `test_facade_names_present_and_exported` fails (`AttributeError: ... has no attribute 'any_value'`); 14 of 16 signature pins fail — only `kurtosis`/`skewness` pass (already `(col)`); `test_sumDistinct_warns_spark_deprecation` fails (no such attribute). |
| C-002 | The Python-door value cells answer the fixture rows, types and nullability. | OPEN | Red on the base: 68 of 70 agg-fixture Python cells fail (only the 2 pre-existing `listagg` cells pass); all 12 alias-fixture `sum_distinct`/`sumDistinct` Python cells fail. Per name failed: any_value 6, max_by 8, min_by 6, percentile 6, product 6, listagg_distinct 6, string_agg_distinct 6, grouping_id 4, histogram_numeric 4, kurtosis 4, skewness 4, mode 6, count_min_sketch 2, sum_distinct 8, sumDistinct 4. |
| C-003 | The SQL-door value cells answer, and Spark-refused SQL names stay refused. | OPEN | Red on the base: all 33 agg-fixture SQL value cells fail and all 4 alias `sum(DISTINCT)` SQL value cells fail. Per name failed: listagg 4, kurtosis 4, skewness 4, mode 4, percentile 3, any_value 2, max_by 2, min_by 2, grouping_id 2, histogram_numeric 2, count_min_sketch 4, sum_distinct 4. |
| C-004 | The error cells fail with Spark's own error condition on their own door. | OPEN | Red on the base: all 29 error pins fail (27 agg + 2 alias `sum_distinct`). Per condition failed: `UNRESOLVED_ROUTINE` 10 (product 6, listagg_distinct 2, string_agg_distinct 2), `DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE` 6 (percentile 4, histogram_numeric 2), `UNSUPPORTED_GROUPING_EXPRESSION` 4, `WRONG_NUM_ARGS.WITHOUT_SUGGESTION` 4, `GROUPING_ID_COLUMN_MISMATCH` 2, `CAST_INVALID_INPUT` 1 (percentile over strings, ANSI on). The `UNRESOLVED_ROUTINE` cells fail because RePark does not refuse with Spark's condition today. |
| C-005 | Multi-partition merge: a Rust test splits input across two partitions per UDAF. | OPEN | Later step: no kernels exist yet. |
| C-006 | No regression: the touched suites stay green. | OPEN | Later step: no product change in this round. |
| C-007 | Registry (`EX-FN-9` → FIXED, section-7 rows) and every touched `map.md` in lockstep. | OPEN | Later step: this round lists the new fixture and pin file in `python/repark/tests/map.md` and this ledger in `task/ledgers/staging/map.md`. |

VERDICT: 7 clauses, 0 PROVEN, 7 OPEN, 0 REJECTED.

## Gates (run 16a, step 1)

| gate | result |
|---|---|
| `PYTHONPATH=python/repark/src .venv/bin/python -m pytest python/repark/tests/test_fnp_agg_1.py -q` | 162 failed, 4 passed (the 4: `kurtosis`/`skewness` signature pins and the 2 pre-existing `listagg` Python cells) |
| `uvx ruff@0.15.22 check python/repark/tests/test_fnp_agg_1.py` | exit 0, all checks passed |
| `uvx ruff@0.15.22 format --check python/repark/tests/test_fnp_agg_1.py` | exit 0, already formatted |
| comment-ban grep over the staged diff | prints nothing |
