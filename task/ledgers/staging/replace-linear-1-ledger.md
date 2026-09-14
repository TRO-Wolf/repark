# Unit ledger — REPLACE-LINEAR-1 · `DataFrame.replace` builds one searched CASE — step 1

**Date:** 2026-09-14 · **Branch:** `fix/replace-linear-1-step1` · **Base:** `9efb6a65` (`main`,
overnight-11 docs)
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card REPLACE-LINEAR-1 (run 12b under G-5, ABS-EXPR-1 C-004 audit residue):
the `DataFrame.replace` dict loop wraps the running expression once per mapping entry
(`when(expression == lit(old), lit(new)).otherwise(expression)`), embedding the previous
expression twice per level — 2^N column references for N entries. ABS-EXPR-1 measured
~×3.3 memory per entry (28 MB at 12, 297 MB at 16); a 40-entry dict cannot plan. Step 0
measured the D-2 oracle cells on live PySpark 4.1.2 beside today's facade answers and
landed the red-first 40-entry memory pin. Step 1 lands the D-1 rewrite.

**Owner ruling Q-R1 (2026-09-14) — binding.** Step 1 brings ALL nine divergent cells of
C-001 (the Evidence table rows marked **NO**) to PySpark 4.1.2's answers in the same
rewrite, plus the sanctioned `{1: 2, 2: 3}` cell (simultaneous, `[2, 3, 3, None]`). The
three "both refuse (different class)" cells become PySpark's classes: `[1,2]→[3]` is
`PySparkValueError LENGTH_SHOULD_BE_THE_SAME`, `{"a": 1}` on mixed columns is
`PySparkValueError MIXED_TYPE_REPLACEMENT`, and `replace(True, 9, ["x"])` on int takes
whatever eager refusal the key/value type check produces (chosen: `IllegalArgumentException`
`Unsupported value type java.lang.Boolean (true).` — the JVM `convertToDouble` message).
Validation is eager at `.replace()`. The JVM rule followed: target columns are chosen by
the first key's type family (numeric keys → numeric columns including decimal, str →
string, bool → boolean); non-matching columns pass through; each replacement value is
cast to the column type; a str/bool arm value (or a str/bool key beside a null value)
keeps its type while every other arm converts both sides through `convertToDouble`,
which refuses non-numerics. NaN keys keep matching NaN on double columns.

**Not in this step:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`, and the files the brief reserves for other
runs. Residual disclosed divergence: `replace(5)` with `value` unset is a null-value
mapping here (identity on every probed frame) where PySpark 4.1.2 refuses
`ARGUMENT_REQUIRED` — mirroring it needs a `_NoValue` sentinel on the public signature
and is out of scope for this step.

## PROPOSITION LEDGER — REPLACE-LINEAR-1 step 1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The D-2 oracle cells are measured on live PySpark 4.1.2 (one `local[1]` session, ANSI on) beside repark's answers on the same input frames: answer rows AND result schema (type, nullability) per cell, including `{1: 2, 2: 3}` order semantics, NULL key and NULL value, `subset` as str/list/tuple, a missing-column subset, the type-coercion cells, list `to_replace` with scalar/list `value`, a mixed-type dict, NaN keys, and a backtick-needed column name. | The measured table under Evidence + the pin cells encoding it in `python/repark/tests/test_replace_linear_1.py`. | PROVEN | Step-0 measurement table below (repark column re-measured after the rewrite — every ruled cell now matches, including the three error-class cells). |
| C-002 | A 40-entry `replace` dict plans and collects under a bounded RSS delta — bound = `max(64 MB, 40 × per-entry delta of a flat 40-column select × 2)` — in a subprocess under `RLIMIT_AS = VmSize_at_apply + 3 × 8 GB` (S2-8). Red on the base tree (exponential dict loop), green after the D-1 rewrite. | `test_replace_dict_depth40_memory_linear` red output pasted in Evidence, then green ungated. | PROVEN | Red-first output below (worker died in `case_when` on the nested loop). After: flat ~5.4 MB at every N vs bound 64 MB; pin runs by default and is green. |
| C-003 | After the D-1 rewrite, repark's answers on every C-001 cell equal the oracle's (the `{1: 2, 2: 3}` cell takes the oracle's simultaneous answer 2, per D-3), including the nine cells ruled in by Q-R1. | The step-1 pin asserts the ruled answers per cell. | PROVEN | `test_replace_divergent_cells_match_spark` asserts all ruled answers + arrow types + error classes; `test_replace_oracle_cells_matching` keeps the already-green cells. |
| C-004 | The projected column's naming/origin metadata is unchanged by the rewrite — the `origin_plan_id`/`origin_field` branch and the plain `alias` branch carry the same `spark_display`, `projection_name`, `stable_name`, `has_free_attribute`, `join_sql_expr`, `sql_expr` as today. | Step-1 pins on a join-origin frame and a plain frame. | PROVEN | `test_replace_projection_metadata_plain_frame` and `test_replace_projection_metadata_join_frame` assert `columns`, `select(col)` by name, and join→replace→select; `test_g1_stat_and_expander.py::test_h1_rename_replace_rsplit_dtypes_multi_name` (existing, untouched) exercises replace→select-by-name on a multi-name joined frame. |
| C-005 | The existing `replace` pin (`python/repark/tests/test_df_easy.py::test_describe_summary_replace`, the only `df.replace(` call site in the suite) stays green. | Named pin green unchanged. | PROVEN | `test_df_easy.py` 21/21 green after the rewrite (see Gates). |

## Evidence

### C-001 oracle measurement (live PySpark 4.1.2, `local[1]`, ANSI on — measured 2026-09-13; repark column re-measured 2026-09-14 after the rewrite)

| Cell | PySpark 4.1.2 | repark before (base `9efb6a65`) | repark after | Match |
|---|---|---|---|---|
| `replace({1: 2, 2: 3}, ["x"])` on `x int` rows 1,2,3,NULL | `[2, 3, 3, None]` int | `[3, 3, 3, None]` int | `[2, 3, 3, None]` int32 | yes — sanctioned (D-3) |
| `replace({1.0: None}, ["x"])` on `x double` rows 1.0,2.0,NULL | `[None, 2.0, None]` double | `[None, 2.0, None]` double | `[None, 2.0, None]` double | yes |
| `replace({None: 5}, ["x"])` on `x int` rows 1,NULL,5 | refuses `PySparkValueError [MIXED_TYPE_REPLACEMENT]` | `[1, None, 5]` int — silent no-op | refuses `PySparkValueError [MIXED_TYPE_REPLACEMENT]` | yes |
| `replace(None, 5, ["x"])` on `x int` rows 1,NULL | refuses `PySparkTypeError [NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE]` | `[1, None]` int — silent no-op | refuses `PySparkTypeError [NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE]` | yes |
| `replace(1, None, ["x"])` on `x int` rows 1,2,NULL | `[None, 2, None]` int | `[None, 2, None]` int32 | `[None, 2, None]` int32 | yes |
| `replace({1: None, 2: 3}, ["x"])` on `x int` rows 1,2,3 | `[None, 3, 3]` int | `[None, 3, 3]` int32 | `[None, 3, 3]` int32 | yes |
| `replace([1, 2], [3, 4], ["x"])` on `x int` rows 1,2,3 | `[3, 4, 3]` int | `TypeError: unhashable type: 'list'` | `[3, 4, 3]` int32 | yes |
| `replace([1, 2], 9, ["x"])` on `x int` rows 1,2,3 | `[9, 9, 3]` int | `TypeError: unhashable type: 'list'` | `[9, 9, 3]` int32 | yes |
| `replace([1, 2], [3], ["x"])` on `x int` | refuses `PySparkValueError [LENGTH_SHOULD_BE_THE_SAME]` | `TypeError: unhashable type: 'list'` | refuses `PySparkValueError [LENGTH_SHOULD_BE_THE_SAME]` | yes |
| `replace("a", "b")` no subset on `s string, y int` rows (a,1),(b,2) | `[("b", 1), ("b", 2)]` string+int | `PySparkException: simplify_expressions` (at collect) | `[("b", 1), ("b", 2)]` | yes |
| `replace(1, 2, ["x"])` on `x boolean` rows T,F,NULL | `[True, False, None]` boolean — no match | `AnalysisException: type_coercion` (at collect) | `[True, False, None]` bool | yes |
| `replace(True, False, ["x"])` on `x boolean` | `[False, False, None]` boolean | `[False, False, None]` bool | `[False, False, None]` bool | yes |
| `replace(True, 9, ["x"])` on `x int` rows 1,0,2 | refuses `IllegalArgumentException: Unsupported value type java.lang.Boolean` | `AnalysisException: type_coercion` (at collect) | refuses `IllegalArgumentException: Unsupported value type java.lang.Boolean (true).` | yes |
| `replace(1, 2.5, ["x"])` on `x int` rows 1,2,NULL | `[2, 2, None]` int — value cast to the column type | `[2.5, 2.0, None]` double — widens | `[2, 2, None]` int32 — cast to the column type | yes |
| `replace({"a": 1})` on `x int, s string` | refuses `PySparkValueError [MIXED_TYPE_REPLACEMENT]` | `PySparkException: simplify_expressions` (at collect) | refuses `PySparkValueError [MIXED_TYPE_REPLACEMENT]` | yes |
| `replace(1, 9, subset="missing")` on `x int` | refuses `AnalysisException [UNRESOLVED_COLUMN.WITH_SUGGESTION]` | `[1, 2]` int32 — silent no-op | refuses `AnalysisException` (facade `cannot be resolved` raise) | yes |
| `replace(1, 9, subset=("x",))` on `x int, y int` | `[(9, 1), (2, 2)]` | `[(9, 1), (2, 2)]` | `[(9, 1), (2, 2)]` | yes |
| `replace(1, 9, subset="x")` on `x int, y int` | `[(9, 1), (2, 2)]` | `[(9, 1), (2, 2)]` | `[(9, 1), (2, 2)]` | yes |
| `replace(1, 9)` no subset on `x int, y int` | `[(9, 9), (2, 2)]` — both columns | `[(9, 9), (2, 2)]` | `[(9, 9), (2, 2)]` | yes |
| `replace({NaN: 0.0}, ["x"])` on `x double` rows NaN,1.0,NULL | `[0.0, 1.0, None]` double — NaN matches | `[0.0, 1.0, None]` double | `[0.0, 1.0, None]` double | yes |
| `replace(1, 9, ["x"])` on `x double` rows 1.0,2.0,NULL | `[9.0, 2.0, None]` double | `[9.0, 2.0, None]` double | `[9.0, 2.0, None]` double | yes |
| `replace("a", "b", ["x"])` on `x int` rows 1,2 | `[1, 2]` int — silent no-match | `PySparkException: simplify_expressions` (at collect) | `[1, 2]` int32 | yes |
| `replace({})` on `x int` | `[1, 2]` — identity | `[1, 2]` int32 | `[1, 2]` int32 | yes |
| `replace(1, 9, ["a b"])` on `` `a b` `` bigint rows 1,2 | `[9, 2]` bigint | `[9, 2]` int64 | `[9, 2]` int64 | yes |
| `replace({}, subset="missing")` on `x int` (extra probe) | refuses `AnalysisException [UNRESOLVED_COLUMN.WITH_SUGGESTION]` | `[1, 2]` — silent no-op | refuses `AnalysisException` — subset resolves before the empty-map short-circuit | yes |
| `replace({1: 2}, subset="X")` on `x int` (extra probe) | `[1, 2]` — resolves but never matches | `[1, 2]` | `[1, 2]` — `resolve` case-folds but `contains` keeps the spelling | yes |
| `replace((1, 2), (9, 8), ["x"])` on `x int` (extra probe) | `[9, 8]` | `TypeError: unhashable type: 'tuple'` | `[9, 8]` | yes |
| `replace({2: True}, ["x"])` on `x int` rows 1,0,2 (extra probe) | `[1, 0, 1]` — bool value casts to 1 | `PySparkException` (at collect) | `[1, 0, 1]` | yes |
| `replace({True: None}, ["x"])` on `x boolean` (extra probe) | `[None, False]` | refused (at collect) | `[None, False]` | yes |
| `replace({True: None}, ["x"])` on `x int` (extra probe) | `[1, 2]` — bool key family skips int | `[1, 2]` | `[1, 2]` | yes |
| `replace({1.5: 9}, ["x"])` on `x double` (extra probe) | `[9.0, 2.0, None]` | `[9.0, 2.0, None]` | `[9.0, 2.0, None]` | yes |
| `replace({1.5: 9}, ["x"])` on `x int` (extra probe) | `[1, 2]` — 1.5 matches nothing | `[1, 2]` | `[1, 2]` | yes |

All repark validation errors now raise eagerly at `.replace()` (PySpark-shaped
`PySparkTypeError`/`PySparkValueError`/`AnalysisException`), never at `.collect()`.
The `True, 9` on-int refusal is the JVM `convertToDouble` `IllegalArgumentException`
shape — Q-R1 left the exact class to the implementer; `IllegalArgumentException` with
the JVM's `Unsupported value type java.lang.Boolean (true).` message was chosen.

### C-002 red-first memory pin

Base tree `9efb6a65` (debug native, `.venv` with the `record` extra). Worker: 10-row
`x int` frame, session warmup, then `RLIMIT_AS = VmSize_at_apply + 3 × 8 GB` (S2-8),
then `frame.replace({i: i + 1000 for i in range(N)}, subset=["x"]).collect()`, RSS delta
= `VmHWM` after minus `VmHWM` at baseline.

Before (base `9efb6a65`, measured 2026-09-13; N=16 re-measured 2026-09-14 → 290 869 248 B):

| N | RSS delta before | RSS delta after (2026-09-14) |
|---|---|---|
| 4 | 3 354 624 B (3.2 MB) | 5 640 192 B (5.4 MB) |
| 8 | 3 358 720 B (3.2 MB) | 5 648 384 B (5.4 MB) |
| 12 | 22 921 216 B (21.9 MB) | 5 652 480 B (5.4 MB) |
| 14 | 81 321 984 B (77.6 MB) | — |
| 16 | 290 607 104 B (277.1 MB) | 5 656 576 B (5.4 MB) |
| 40 | died in `case_when` | 5 685 248 B (5.4 MB) |
| flat 40-col select control | 1 748 992 B (1.7 MB) | 1 617 920 B (1.5 MB) |

~×3.4/entry above N≈10 before — matches ABS-EXPR-1's ~×3.3 (28 MB @12, 297 MB @16).
After the rewrite the delta is flat (~5.4 MB, dominated by one collect) across all N —
linear, as D-1 requires. The pin bound is `max(64 MB, 2 × flat control)` = 64 MB.

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

The 40-entry dict never reached `collect()` — the worker died inside the `replace` dict
loop at `case_when` (MemoryError surfacing as the pyo3 NULL-PyObject panic — the same
cap-pressure shape ABS-EXPR-1 C-004 finding (a) filed for its own card).

### C-001/C-003 ruled-answer pins (after the rewrite)

`test_replace_divergent_cells_match_spark` asserts, per ruled cell: `{1: 2, 2: 3}` →
`[2, 3, 3, None]` int32; `{None: 5}` → `PySparkValueError MIXED_TYPE_REPLACEMENT`;
`None, 5` → `PySparkTypeError NOT_BOOL_OR_DICT_OR_FLOAT_OR_INT_OR_LIST_OR_STR_OR_TUPLE`;
`[1, 2], [3, 4]` → `[3, 4, 3]` int32; `[1, 2], 9` → `[9, 9, 3]` int32;
`[1, 2], [3]` → `PySparkValueError LENGTH_SHOULD_BE_THE_SAME`; `"a"→"b"` no-subset on
`s string, y int` → `[("b", 1), ("b", 2)]`; `1, 2` on boolean → `[True, False, None]` bool;
`True, 9` on int → `IllegalArgumentException` matching `Unsupported value type`;
`1, 2.5` on int → `[2, 2, None]` int32; `{"a": 1}` mixed → `PySparkValueError
MIXED_TYPE_REPLACEMENT`; `subset="missing"` → `AnalysisException` matching
`cannot be resolved`; `"a"→"b"` on int → `[1, 2]` int32. 21/21 green.

### C-004 metadata pins

`test_replace_projection_metadata_plain_frame`: `replace(1, 9, subset=["x"])` on
`x int, s string` keeps `columns == ["x", "s"]` and `select("x")`/`select("s")` rebind
by name. `test_replace_projection_metadata_join_frame`: `left.join(right, "k")` keeps
`columns == ["k", "x", "s"]` through `replace(10, 99, subset=["x"])`, and `x`/`s`/`k`
all rebind by name with the right values. The origin branch in `replace_expr._replace`
carries `origin_plan_id`/`origin_field`/`join_sql_expr`/`sql_expr` forward unchanged.

### C-005 existing-pin check

`test_df_easy.py` (21 pins incl. `test_describe_summary_replace`) green; the other
`DataFrame.replace` callers `test_g1_stat_and_expander.py` and
`test_examples_dataframe_c.py` green. EX-DF-12 (`test_replace_unsubset_arms`) flipped
from the old divergent assertions to the Spark-equal answers and the registry row is
marked FIXED 2026-09-14 REPLACE-LINEAR-1.

### Critic round (2026-09-14) — Grok critic-logic findings and ruled dispositions

Grok critic report `/tmp/oc-worker/g-crit577/report.md` on the step-1 tree, release
native. Dispositions below are the binding orchestrator rulings.

- **P1-1 FIXED — string keys rewrote `binary` columns.** `replace("a", "b")` on an
  `x binary` column produced `[b'b', b'b', b'ab']` because
  `logical_schema_fields` collapses binary to the `string` type key. The target
  family is now classified from the column's **physical Arrow type**
  (`frame._analyzed_arrow_schema()`): `Utf8`/`LargeUtf8`/`Utf8View` → `str`,
  `BooleanType` → `bool`, integer/floating/decimal → `numeric`, everything else →
  `other` (never a target). `_NUMERIC_TYPE_KEYS` is deleted — the physical check
  makes the collapsed-key set unnecessary, which also retires the P3-2 residue
  (the set named widths the facade never emits). `crates/` untouched.
  Pin: `test_replace_binary_column_not_string_family` (no subset AND `subset=["x"]`,
  identity bytes AND arrow type `binary`).
- **P2-1 PINNED + DISCLOSED — overflow refusal is at collect, not `.replace()`.**
  `replace(1, 3000000000, subset=["x"])` on `x int` plans fine and refuses at
  collect: `PySparkException … simplify_expressions … Arrow error: Cast error:
  Can't cast value 3000000000.0 to type Int32`. Spark ANSI raises
  `CAST_OVERFLOW`. The error-class mapping is **not** built in this unit;
  `test_replace_overflow_collect_disclosed` pins today's refusal
  (`pytest.raises(PySparkException)` at collect, matching `cast`). Disclosed P3
  residue: class + timing differ from the JVM's `CAST_OVERFLOW`.
- **P2-2 PINNED — last-wins on duplicate keys.** `dict(zip(keys, values))` already
  keeps the last arm, so `[1, 1], [2, 3]` → `{1: 3}` → `[3, 2]` and
  `{1: 5, 1.0: 6}` → `{1: 6}` → `[6, 2]` (Python key equality), both int32.
  `test_replace_last_wins_duplicate_keys` asserts values AND arrow type; the
  critic's mutation 5 (first-wins) reds the `[3, 2]` assert.
- **P2-3 FIXED — multi-name equi-join output.** `left.alias("df1").join(
  right.alias("df2"), "k").replace(10, 99)` raised `AMBIGUOUS_REFERENCE` because
  `_iter_bound_columns` binds by display name. `join` now records the generated
  `_repark_jl_*`/`_repark_jr_*` aliases into a new `_join_qualifiers` slot when it
  auto-aliases overlapping inputs (position-aligned with the child's columns —
  left prefix then right suffix; semi/anti keep only the left side); the slot
  propagates through `_spawn_preserving_identity`/`_identity_child` like
  `_display_names`. `replace_expr._iter_replace_bound_columns` binds each field
  through its qualifier (`"rel"."x"`) when qualifiers are present and no display
  overlay is set, so `replace(10, 99)` rewrites both `x` columns by attribute
  identity while `subset=["x"]` still raises `AMBIGUOUS_REFERENCE` (Spark's
  answer for a name subset on multi-name output). Round-2 audit correction:
  ceilings only move down, so the join-side plumbing moved into
  `replace_expr.py` (`_aliased_join_sides`, `_assign_join_qualifiers`) and the
  shared propagation block was extracted as `replace_expr._inherit_plan_metadata`
  (the sanctioned equal-lines offset); `core.py` keeps the slot, the init, and
  four call sites — final count 4054 → 4044, both baselines moved.
  Pin: `test_replace_duplicate_name_join_columns` (overlay join AND
  alias + name equi-join, no subset, `columns` unchanged).
- **P2-4 PINNED + DISCLOSED — nested-field subset.** `replace(1, 9, ["s.x"])` on a
  struct column refuses `AnalysisException` (`cannot be resolved`, the facade's
  unresolved-column raise). Spark refuses with its own nested-field-unsupported
  class — disclosed residue, class differs.
  Pin: `test_replace_struct_subset_disclosed`.
- **P2-5 PINNED — probe rows promoted.** `test_replace_oracle_extra_cells` pins:
  `replace({}, subset="missing")` → `AnalysisException` (subset resolves before
  the empty-map short-circuit — the C-001 extra-probe row); `{2: True}` on int →
  `[1, 0, 1]` int32; `{True: None}` on bool → `[None, False]` bool and on int →
  `[1, 2]` int32; `{1.5: 9}` on int → `[1, 2]` int32 (no-op) and on double →
  `[9.0, 2.0, None]` double; `{1: 2}, subset="X"` → `[1, 2]` (case-variant
  resolves, never matches); tuple `to_replace` `(1, 2), (9, 8)` → `[9, 8]` int32;
  and `replace(1, 2)` over `b boolean, x int` → `b` unchanged `bool`, `x` →
  `[2, 2]` int32 (the family filter must hold AND fire on the same frame).
- **P3-1 residue — `df.na.replace` missing.** `DataFrameNaFunctions` has no
  `replace` method; PySpark routes `df.na.replace` to the same
  `DataFrameNaFunctions.replace`. Ledger residue only (separate card).
- **Observed residue — `crossJoin` multi-name output.** `left.crossJoin(right)`
  with overlapping names still raises `AMBIGUOUS_REFERENCE` on `replace` —
  `crossJoin` never enters the equi-join aliasing path, so no `_join_qualifiers`
  exist to bind by (unnamed scans may not carry resolvable qualifiers at all).
  The ruling pinned the two equi-join shapes only; crossJoin is a deeper
  surface recorded here, not fixed in this round.
- **P3-2 resolved** — see P1-1 (the collapsed-key set is gone).
- **Q-13b-2 confirmed** — `replace(1)` with no value stays as-is (`value=None`
  treated as a null-value mapping; PySpark refuses `ARGUMENT_REQUIRED` via its
  `_NoValue` sentinel). Still disclosed; the orchestrator is filing a separate
  card.

## Gates

- `.venv/bin/python -m pytest python/repark/tests/test_replace_linear_1.py python/repark/tests/test_df_easy.py -q`: 21 passed.
- `grep -rln "\.replace(" python/repark/tests python/repark-parity/tests` — `DataFrame.replace` callers: `test_replace_linear_1.py`, `test_df_easy.py`, `test_examples_dataframe_c.py`, `test_g1_stat_and_expander.py` (`F.replace`/`writeTo().replace`/`str.replace` hits excluded). All four files: 56 passed.
- `.venv/bin/python -m pytest python/repark/tests/test_dfcore_1_exports.py -q`: 10 passed (new-submodule sets updated for `replace_expr`).
- `make verify`: exit 0.
- Comment fence (`git diff --cached` grep for added `//`/`#` lines): prints nothing.
- Critic round (2026-09-14, Python-only slot — the build gates `make verify`/`preflight`/`py-test-facade` belong to the orchestrator's follow-up run):
  - `.venv/bin/python -m pytest test_replace_linear_1.py test_df_easy.py test_examples_dataframe_c.py test_g1_stat_and_expander.py test_dfcore_1_exports.py -q -p no:cacheprovider`: 72 passed.
  - `uvx ruff@0.15.22 check python` + `uvx ruff@0.15.22 format --check python`: clean.
  - `./scripts/check_lib_py.sh` (681 files, `core.py` baseline 4074), `./scripts/check_python_conventions.sh` (337 files), `./scripts/check_docstring_presence.sh` (270 files): all clean.
  - `pytest python/repark-parity/tests/test_cap_1_source_file_line_cap.py -q` (parity harness env): 23 passed — mirror row moved 4054 → 4074 with the script baseline.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: replace-linear-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause re-derived from the live oracle rather than presumed — the full 30-cell table was re-measured on PySpark 4.1.2 and re-measured on the rewritten tree, and each of the ten ruled cells plus the three error-class cells is pinned asserting values AND arrow types AND error classes, not a representative subset.
      artifacts: [python/repark/tests/test_replace_linear_1.py, task/ledgers/staging/replace-linear-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: The pins drive the real paths — dict/scalar/list/tuple argument shapes, the eager validation order, the key-family column filter on mixed frames, bool-key and float-key edge arms, case-variant and missing subset spellings, NaN keys, and a backtick-needed column name — never proxies like inspecting the expression tree.
      artifacts: [python/repark/tests/test_replace_linear_1.py]
    - id: AT-3
      status: N/A
      justification: No new shared or mutable state — the helper builds a projection from the bound columns and existing lit/CASE builders; nothing is cached or registered.
    - id: AT-4
      status: N/A
      justification: No concurrency: single-threaded validation and projection on one frame; the memory pin runs in a single subprocess.
    - id: AT-5
      status: N/A
      justification: No authn/authz, deserialization of hostile input, path, credential, or network surface; the warning path emits a fixed string.
    - id: AT-6
      status: ATTACKED
      evidence: Behavior is asserted on the observable surface — collected values, arrow types, error classes/conditions raised at .replace(), and RSS deltas in a bounded subprocess — never on private expression internals; the metadata pins assert rebind-by-name behavior rather than flag fields.
      artifacts: [python/repark/tests/test_replace_linear_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: The unit exists because the old shape was system-breaking at depth (2^N column references, death at N=40); the before/after RSS table at N=4/8/12/16/40 plus the flat-select control is the cost measurement, and the depth-40 pin now runs ungated in the default suite under a 64 MB bound.
      artifacts: [python/repark/tests/test_replace_linear_1.py, task/ledgers/staging/replace-linear-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: The one upstream behavior the rewrite could have silently presumed — that DataFusion would refuse cross-type comparisons instead of matching by coercion — was probed directly (int col = float lit, bool key on int col, decimal col = double lit) before the key-family filter was written; no new public name; D-4's core.py ratchet recorded at the exact new baseline 4054, re-recorded 4054 → 4044 when the `_join_qualifiers` plumbing moved into `replace_expr.py` (P2-3; the audit's first pass tried 4074 and the down-only ceiling rule rejected it — the equal-lines extraction landed it at 4044). The critic round's second presumed-cheap claim — that the collapsed type key is the column's real family — was falsified on `binary` and the filter moved to physical Arrow types.
      artifacts: [python/repark/src/repark/spark/dataframe/replace_expr.py, scripts/check_lib_py.py]
    - id: AT-9
      status: ATTACKED
      evidence: The user-visible failure paths are the ruled ones and are all pinned: wrong argument type, wrong value type, unequal list lengths, mixed key/value families, missing subset name, and the convertToDouble refusal for non-convertible arm values — each asserted at .replace() with its class.
      artifacts: [python/repark/tests/test_replace_linear_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation coverage: a nested running expression reds C-002's depth-40 bound; sequential replacement reds the {1:2,2:3} cell; a missing key-family filter reds the bool/str no-match cells; skipping the value cast reds the int32 arrow-type asserts; lazy validation reds every pytest.raises that wraps .replace() itself; losing origin metadata reds the join-frame rebind pins.
      artifacts: [python/repark/tests/test_replace_linear_1.py]
  complete: true
```
