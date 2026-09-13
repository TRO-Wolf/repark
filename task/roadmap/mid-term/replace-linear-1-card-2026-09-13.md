# Card REPLACE-LINEAR-1 — `DataFrame.replace` builds one searched CASE per column

**Date:** 2026-09-13 · **Authored by:** run 12b under G-5 · **Source:** ABS-EXPR-1's C-004 audit residue
([abs-expr-1-ledger.md](../../ledgers/staging/abs-expr-1-ledger.md), row "`DataFrame.replace` dict loop").

## Why

`DataFrame.replace` (`python/repark/src/repark/spark/dataframe/core.py`, the `for old, new in mapping.items()` loop)
wraps the running expression once per mapping entry:
`expression = when(expression == lit(old), lit(new)).otherwise(expression)`. Each level embeds the previous expression
twice, in the condition and in the `otherwise`, so the plan for N entries references the column 2^N times. ABS-EXPR-1
measured ~×3.3 memory per entry: 28 MB at a 12-entry dict, 297 MB at 16. A 40-entry dict cannot plan.

## Decisions

| id | decision |
|---|---|
| D-1 | For each target column build ONE searched CASE, `CASE WHEN col = a THEN b WHEN col = c THEN d … ELSE col END`. Each branch references the bound column once and the running expression is never nested, so plan size is linear in the number of entries. The projection keeps today's naming, origin and SQL-text metadata. |
| D-2 | Semantics stay byte-identical to today's answers wherever today's answer matches Spark. Before the change, step 0 pins against live PySpark 4.1.2: a NULL key and a NULL value in `to_replace`/`value`, `subset` as str, list and tuple, a subset naming a missing column, the type coercion rules (int key on a double column, bool vs int, string key on a numeric column, a mixed-type dict, `value` of another type than the column), list `to_replace` with a scalar or list `value`, and the mapping order case `{1: 2, 2: 3}`. |
| D-3 | Today's loop is sequential, so `{1: 2, 2: 3}` turns 1 into 3, while a searched CASE turns 1 into 2. If the oracle answers 2 (simultaneous), the CASE follows Spark and the ledger records the corrected cell. If step 0 finds any other cell where today's answer differs from Spark, the round HALTs and the orchestrator rules before step 1. |
| D-4 | No new public name. The file-size ceiling of `core.py` only moves down; helper code goes to a sibling module listed in `dataframe/map.md`. |

## Steps

| step | worker | what |
|---|---|---|
| 0 | Devin | Measure: the D-2 oracle cells (live Spark, box alone, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`), today's facade answers beside them, and the red-first memory pin. The pin is a 40-entry replace dict planned and collected in a subprocess under `RLIMIT_AS` (S2-8: `VmSize_at_apply + 3 × limit`) with a per-entry RSS bound, in the style of `python/repark/tests/test_abs_expr_1.py`. It must be red on the base tree. |
| 1 | Devin | D-1 rewrite; the answer pins and the memory pin green; the ledger's COVERAGE_ATTESTATION. |

**Home:** `python/repark/src/repark/spark/dataframe/core.py` (`replace`) and a sibling helper module if needed,
`python/repark/tests/test_replace_linear_1.py`, an oracle fixture or pin file beside it. **Gates:** the new pins,
existing `replace` pins (`grep -rln "\.replace(" python/repark/tests`), `make verify`, `make preflight`, the parity suite;
S2-21 Python reviewer and Grok critic-logic before the PR.

## Pointers

- Up: [map.md](map.md)
