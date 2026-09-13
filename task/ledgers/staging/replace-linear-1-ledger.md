# Unit ledger — REPLACE-LINEAR-1 · `DataFrame.replace` builds one searched CASE — step 0

**Date:** 2026-09-13 · **Branch:** `fix/replace-linear-1` · **Base:** `9efb6a65` (`main`,
overnight-11 docs)
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card REPLACE-LINEAR-1 (run 12b under G-5, ABS-EXPR-1 C-004 audit residue):
the `DataFrame.replace` dict loop wraps the running expression once per mapping entry
(`when(expression == lit(old), lit(new)).otherwise(expression)`), embedding the previous
expression twice per level — 2^N column references for N entries. ABS-EXPR-1 measured
~×3.3 memory per entry (28 MB at 12, 297 MB at 16); a 40-entry dict cannot plan. Step 0
measures the D-2 oracle cells on live PySpark 4.1.2 beside today's facade answers and
lands the red-first 40-entry memory pin. No product code changes in this step.

**HALT (D-3).** The oracle measurement found cells beyond the mapping-order case where
today's answer differs from Spark — the card rules the round halts for an orchestrator
ruling before step 1. The divergent cells are enumerated under Evidence → C-001.

**Not in this step:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`, and every product file — the rewrite is step 1.

**Parked 2026-09-13 (run 12b orchestrator):** step 0 HALTed on D-3 with nine non-mapping-order divergences (C-001).
Whether step 1 also brings those cells to Spark's answers is a semantic change beyond the owner's "semantics identical
to today's" instruction, so it is owner question Q-R1 in
`task/roadmap/mid-term/overnight-report-2026-09-13-run12b.md` (docs PR). Step 1 waits
for that ruling; C-002..C-005 stay OPEN.

## PROPOSITION LEDGER — REPLACE-LINEAR-1 step 0 — 2026-09-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The D-2 oracle cells are measured on live PySpark 4.1.2 (one `local[1]` session, ANSI on, stopped before the gates) beside today's repark answers on the same input frames (explicit DDL schemas): answer rows AND result schema (type, nullability) per cell, including `{1: 2, 2: 3}` order semantics, NULL key and NULL value, `subset` as str/list/tuple, a missing-column subset, the type-coercion cells, list `to_replace` with scalar/list `value`, a mixed-type dict, NaN keys, and a backtick-needed column name. | The measured table under Evidence + the pin cells encoding it in `python/repark/tests/test_replace_linear_1.py`. | PROVEN | Measured 2026-09-13, PySpark 4.1.2, `spark.sql.ansi.enabled` default. Nine cells differ from the oracle beyond the sanctioned mapping-order cell — D-3 HALT, table below. |
| C-002 | A 40-entry `replace` dict plans and collects under a bounded RSS delta — bound = `max(64 MB, 40 × per-entry delta of a flat 40-column select × 2)` — in a subprocess under `RLIMIT_AS = VmSize_at_apply + 3 × 8 GB` (S2-8). Red on the base tree (exponential dict loop), green after the D-1 rewrite. | `test_replace_dict_depth40_memory_linear` red output pasted in Evidence, then green. | OPEN | Pin landed env-gated (`REPARK_REPLACE_LINEAR_1_MEM=1`) so the default suite stays green until step 1; red demonstrated 2026-09-13 — worker dies inside `frame.replace` (`MemoryError` → `pyo3_runtime.PanicException: PyObject pointer is null` in `case_when`). |
| C-003 | After the D-1 rewrite, repark's answers on every C-001 cell equal the oracle's (the `{1: 2, 2: 3}` cell takes the oracle's simultaneous answer 2, per D-3) — pending the HALT ruling on the nine non-mapping-order divergent cells. | The step-1 pin asserts the ruled answers per cell. | OPEN | Blocked on the D-3 ruling: whether step 1 fixes the facade-level divergences (list `to_replace`, missing-subset refusal, non-matching-key no-match, value cast-to-column-type, NULL-key refusal) or pins them as disclosed gaps. |
| C-004 | The projected column's naming/origin metadata is unchanged by the rewrite — the `origin_plan_id`/`origin_field` branch and the plain `alias` branch carry the same `spark_display`, `projection_name`, `stable_name`, `has_free_attribute`, `join_sql_expr`, `sql_expr` as today. | Step-1 pins on a join-origin frame and a plain frame. | OPEN | — |
| C-005 | The existing `replace` pin (`python/repark/tests/test_df_easy.py::test_describe_summary_replace`, the only `df.replace(` call site in the suite) stays green. | Named pin green unchanged. | OPEN | — |

## Evidence

### C-001 oracle measurement (live PySpark 4.1.2, `local[1]`, ANSI on — measured 2026-09-13)

| Cell | PySpark 4.1.2 | repark today (base `9efb6a65`) | Match |
|---|---|---|---|
| `replace({1: 2, 2: 3}, ["x"])` on `x int` rows 1,2,3,NULL | `[2, 3, 3, None]` int | `[3, 3, 3, None]` int | NO — sanctioned (D-3: oracle answers 2, simultaneous; the searched CASE follows Spark) |
| `replace({1.0: None}, ["x"])` on `x double` rows 1.0,2.0,NULL | `[None, 2.0, None]` double | `[None, 2.0, None]` double | yes |
| `replace({None: 5}, ["x"])` on `x int` rows 1,NULL,5 | refuses `PySparkValueError [MIXED_TYPE_REPLACEMENT]` | `[1, None, 5]` int — silent no-op | **NO** |
| `replace(None, 5, ["x"])` on `x int` rows 1,NULL | refuses `PySparkTypeError [NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE]` | `[1, None]` int — silent no-op | **NO** |
| `replace(1, None, ["x"])` on `x int` rows 1,2,NULL | `[None, 2, None]` int | `[None, 2, None]` int32 | yes |
| `replace({1: None, 2: 3}, ["x"])` on `x int` rows 1,2,3 | `[None, 3, 3]` int | `[None, 3, 3]` int32 | yes |
| `replace([1, 2], [3, 4], ["x"])` on `x int` rows 1,2,3 | `[3, 4, 3]` int | `TypeError: unhashable type: 'list'` | **NO** |
| `replace([1, 2], 9, ["x"])` on `x int` rows 1,2,3 | `[9, 9, 3]` int | `TypeError: unhashable type: 'list'` | **NO** |
| `replace([1, 2], [3], ["x"])` on `x int` | refuses `PySparkValueError [LENGTH_SHOULD_BE_THE_SAME]` | `TypeError: unhashable type: 'list'` | both refuse (different class) |
| `replace("a", "b")` no subset on `s string, y int` rows (a,1),(b,2) | `[("b", 1), ("b", 2)]` string+int | `PySparkException: Optimizer rule 'simplify_expressions' failed — Arrow Cast error: Cannot cast string 'a' to Int32` (at collect) | **NO** |
| `replace(1, 2, ["x"])` on `x boolean` rows T,F,NULL | `[True, False, None]` boolean — no match | `AnalysisException: type_coercion — Cannot infer common argument type Boolean = Int32` (at collect) | **NO** |
| `replace(True, False, ["x"])` on `x boolean` | `[False, False, None]` boolean | `[False, False, None]` bool | yes |
| `replace(True, 9, ["x"])` on `x int` rows 1,0,2 | refuses `IllegalArgumentException: Unsupported value type java.lang.Boolean` | `AnalysisException: type_coercion` (at collect) | both refuse (different class) |
| `replace(1, 2.5, ["x"])` on `x int` rows 1,2,NULL | `[2, 2, None]` int — value cast to the column type | `[2.5, 2.0, None]` double — widens, keeps 2.5 | **NO** |
| `replace({"a": 1})` on `x int, s string` | refuses `PySparkValueError [MIXED_TYPE_REPLACEMENT]` | `PySparkException: simplify_expressions — Cannot cast string 'a' to Int32` (at collect) | both refuse (different class) |
| `replace(1, 9, subset="missing")` on `x int` | refuses `AnalysisException [UNRESOLVED_COLUMN.WITH_SUGGESTION]` | `[1, 2]` int32 — silent no-op | **NO** |
| `replace(1, 9, subset=("x",))` on `x int, y int` | `[(9, 1), (2, 2)]` | `[(9, 1), (2, 2)]` | yes |
| `replace(1, 9, subset="x")` on `x int, y int` | `[(9, 1), (2, 2)]` | `[(9, 1), (2, 2)]` | yes |
| `replace(1, 9)` no subset on `x int, y int` | `[(9, 9), (2, 2)]` — both columns | `[(9, 9), (2, 2)]` | yes |
| `replace({NaN: 0.0}, ["x"])` on `x double` rows NaN,1.0,NULL | `[0.0, 1.0, None]` double — NaN matches | `[0.0, 1.0, None]` double | yes |
| `replace(1, 9, ["x"])` on `x double` rows 1.0,2.0,NULL | `[9.0, 2.0, None]` double | `[9.0, 2.0, None]` double | yes |
| `replace("a", "b", ["x"])` on `x int` rows 1,2 | `[1, 2]` int — silent no-match | `PySparkException: simplify_expressions — Cannot cast string 'a' to Int32` (at collect) | **NO** |
| `replace({})` on `x int` | `[1, 2]` — identity | `[1, 2]` int32 | yes |
| `replace(1, 9, ["a b"])` on `` `a b` `` bigint rows 1,2 | `[9, 2]` bigint | `[9, 2]` int64 | yes |

All repark errors are lazy — they raise at `.collect()`/`.to_arrow()`, never at `.replace()`.
Every repark error cell above the bool/int refusal is a `simplify_expressions`-rule cast
failure or a `type_coercion` planning refusal, i.e. the facade composes `col = lit(key)`
for every column and lets DataFusion coercion decide; Spark pre-checks key/value types
against each subset column and silently skips non-matching ones.

**D-3 verdict.** `{1: 2, 2: 3}`: PySpark answers `2` for input 1 (simultaneous
replacement) — the searched CASE is correct and the ledger records the corrected cell.
Nine other cells differ (marked **NO**): a NULL dict key and a scalar `None` key silently
no-op where Spark refuses; list `to_replace` raises `TypeError` where Spark answers; a
string key on an int column crashes the optimizer where Spark no-ops; an int key on a
boolean column refuses where Spark no-ops; `value` of another type widens to double and
keeps 2.5 where Spark casts to the column type and answers 2; a missing-column subset
silently no-ops where Spark refuses `UNRESOLVED_COLUMN`. Per the card's step-5 rule the
round HALTs for an orchestrator ruling before step 1.

### C-002 red-first memory pin

Base tree `9efb6a65` (debug native, `.venv` with the `record` extra). Worker: 10-row
`x int` frame, session warmup, then `RLIMIT_AS = VmSize_at_apply + 3 × 8 GB` (S2-8),
then `frame.replace({i: i + 1000 for i in range(N)}, subset=["x"]).collect()`, RSS delta
= `VmHWM` after minus `VmHWM` at baseline.

| N | RSS delta |
|---|---|
| 4 | 3 354 624 B (3.2 MB) |
| 8 | 3 358 720 B (3.2 MB) |
| 12 | 22 921 216 B (21.9 MB) |
| 14 | 81 321 984 B (77.6 MB) |
| 16 | 290 607 104 B (277.1 MB) |
| flat 40-col select control | 1 748 992 B (1.7 MB) |

~×3.4/entry above N≈10 — matches ABS-EXPR-1's ~×3.3 (28 MB @12, 297 MB @16). The pin
bound is `max(64 MB, 2 × flat control)` = 64 MB on this box.

Armed run (`REPARK_REPLACE_LINEAR_1_MEM=1`), red on the base tree:

```
AssertionError: replace worker produced no output: rc=1
stderr tail: ... in _from_when_pairs
    parts = _native.PyColumnParts.case_when(
MemoryError:
thread '<unnamed>' panicked at pyo3-0.29.2/src/instance.rs:347:60:
PyObject pointer is null
  File ".../spark/dataframe/core.py", line 2490, in replace
    expression = when(expression == lit_fn(old), lit_fn(new)).otherwise(expression)
pyo3_runtime.PanicException: PyObject pointer is null
FAILED test_replace_linear_1.py::test_replace_dict_depth40_memory_linear
```

The 40-entry dict never reaches `collect()` — the worker dies inside the `replace` dict
loop at `case_when` (MemoryError surfacing as the pyo3 NULL-PyObject panic — the same
cap-pressure shape ABS-EXPR-1 C-004 finding (a) filed for its own card).
