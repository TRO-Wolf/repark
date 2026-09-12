# Unit ledger — FACADE-2 · Column display strings rendered in Rust

**Date:** 2026-09-12 · **Branch:** `feat/facade-2-s1` · **Base:** `d3a20d53`
**Model:** grok-4.6 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card FACADE-2 (audit §8): Spark display strings (`spark_display`,
`projection_name`, `sql_expr`, `join_sql_expr`) move from Python string assembly in
`column.py` / `functions*` into Rust on the native `PyColumn`. Step 1 is pins only:
goldens, `isinstance`, mutation proof. No product change.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`. No JVM. Step 2/3 product moves are later
commits on later branches.

## PROPOSITION LEDGER — FACADE-2 — 2026-09-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Goldens cover every audit §4 Group-1 site: `lit` datetime/date/time, the Group-1 CAST string entry (`F.lit` numpy ndarray → `CAST(... AS ARRAY<...>)` at `functions.py:280`; there is no exported `F.cast` on this tree), `F.expr` passthrough, `F.pi`, `F.uuid`. | `test_facade_2_column_display_goldens.py` case ids `g1_*`; committed JSON. | **PROVEN** | 19 Group-1 cases. Measured `hasattr(F, "cast") is False`. Numpy CAST site recorded as `g1_cast_numpy_int32` / `int64` / `float64`. Red-first: missing JSON → `AssertionError: missing golden facade_2_column_display_goldens.json`. |
| C-002 | Goldens cover every §4 Group-2 method group: binary arithmetic and comparison including reflected forms, `__neg__`, `__ne__`, `eqNullSafe`, `substr` both signatures, `startswith`/`endswith`/`contains`/`like`/`rlike`, bitwise and/or/xor, `__invert__`, `isNull`/`isNotNull`, `when/otherwise` length 1 and 3, `alias` multi-name and metadata, `__getitem__` struct/map/array/by-column key, `getField`/`getItem`, `cast`/`try_cast` DDL and DataType, `asc`/`desc`/`asc_nulls_first`/`desc_nulls_last`, nesting, `F.sum(x) + 1`. | Case ids `g2_*` listed by family below. | **PROVEN** | 202 Group-2 cases. 221 total (≥150). Families below. |
| C-003 | Compare is byte-identical against the committed JSON; record mode exists only behind `REPARK_FACADE_2_RECORD_GOLDENS=1` and the test fails if that variable is set while CI is set. | `test_column_display_goldens_match_committed_bytes`; `test_record_mode_fails_when_ci_is_set`. | **PROVEN** | Record with `CI=true` refused (`REPARK_FACADE_2_RECORD_GOLDENS is set while CI is set`). Record with `env -u CI` wrote the JSON. Compare without the env is green. Dedicated CI-guard pin green. |
| C-004 | `isinstance(c, repark.Column)` holds. Measured: `repark.Column` is exported from `python/repark/src/repark/__init__.py` and is the same object as `repark.spark.column.Column`. | `test_isinstance_column_is_repark_column`. | **PROVEN** | Probe: `repark.Column is SparkColumn` True. Pin covers col, lit, `+`, alias, cast, when. |
| C-005 | Mutation proof: one-character change in one rendered string in `column.py` turns the golden test red; restore leaves it green. | Scratch edit of `spark/column.py`; pytest red output pasted here; `git checkout` restore. | **PROVEN** | `negative(` → `negativx(` at `column.py:399`. Red: `golden byte mismatch ... changed=['g2_unary__neg', 'g2_unary__neg_add', 'g2_unary__neg_neg']`. `git checkout -- python/repark/src/repark/spark/column.py`; 3 passed. Product tree unchanged. |
| C-006 | The audit's five pin files stay green unchanged: `test_columns.py`, `test_column_access.py`, `test_column_x1_census.py`, `test_examples_column_a.py`, `test_fnp_9_collections_json.py`. | Those files, same commit, no edits. | **PROVEN** | Combined with the new golden file: 225 passed in 5.32s. Those five files were not edited. |
| C-007 | Named gates green: golden+audit pytest, `make verify`, parity suite. | Commands and counts in Evidence. | **PROVEN** | See Evidence. |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

## Case ids by family (221 cases)

Recorded from base `d3a20d53` on 2026-09-12. Nested frame schema
`x int, y int, s string, flag boolean, st struct<a:int,b:string>, m map<string,int>, arr array<int>`.

| Family | n | Case ids |
|---|---|---|
| g1_lit_temporal | 9 | `g1_lit_date`, `g1_lit_date_leap`, `g1_lit_datetime_micro`, `g1_lit_datetime_micro_trailing`, `g1_lit_datetime_naive`, `g1_lit_datetime_tz_ny`, `g1_lit_datetime_tz_utc`, `g1_lit_time`, `g1_lit_time_micro` |
| g1_cast_numpy | 3 | `g1_cast_numpy_float64`, `g1_cast_numpy_int32`, `g1_cast_numpy_int64` |
| g1_expr | 5 | `g1_expr_cast_sql`, `g1_expr_infix`, `g1_expr_infix_mul`, `g1_expr_paren`, `g1_expr_passthrough_pi` |
| g1_pi | 1 | `g1_pi` |
| g1_uuid | 1 | `g1_uuid` |
| g2_binary_arith | 62 | `g2_binary_arith__add_chain`, `add_col`, `add_float`, `add_lit_{-1,0,1,2,4}`, `div_col`, `div_lit_{-1,1,2,4}`, `mod_col`, `mod_lit_{-1,1,2,4}`, `mul_col`, `mul_lit_{-1,0,1,2,4}`, `pow_col`, `pow_lit_{-1,1,2,4}`, `radd_lit_{-1,0,1,2,4}`, `rdiv_lit_{-1,1,2,4}`, `rmod_lit_{-1,1,2,4}`, `rmul_lit_{-1,0,1,2,4}`, `rpow_lit_{-1,1,2,4}`, `rsub_lit_{-1,0,1,2,4}`, `sub_col`, `sub_lit_{-1,0,1,2,4}` |
| g2_binary_cmp | 42 | `eq/ne/lt/gt/le/ge_col`, each with `lit_{-1,0,1}` and reflected `r*` |
| g2_unary | 3 | `g2_unary__neg`, `neg_add`, `neg_neg` |
| g2_eqnullsafe | 4 | `col`, `lit`, `none`, `zero` |
| g2_substr | 3 | `int`, `int_long`, `col` |
| g2_string_pred | 10 | startswith/endswith/contains/like/rlike × str and col |
| g2_bitwise | 6 | and/or/xor × col and lit |
| g2_invert | 2 | `flag`, `cmp` |
| g2_logical | 4 | `and`, `or`, `rand`, `ror` |
| g2_null | 4 | isnull / isnotnull and snake aliases |
| g2_when | 4 | len1, len1_open, len3, len3_open |
| g2_alias | 4 | simple, multi, metadata, add |
| g2_getitem | 8 | struct, struct_b, map, map_ab, array, array_1, map_col, array_col |
| g2_getfield | 2 | a, b |
| g2_getitem_method | 2 | 0, map |
| g2_cast | 19 | 10 DDL + 9 DataType objects |
| g2_try_cast | 15 | 10 DDL + 5 DataType objects |
| g2_sort | 6 | asc, desc, asc_nulls_first, desc_nulls_last, asc_nulls_last, desc_nulls_first |
| g2_nesting | 1 | `g2_nesting__op_cast_alias_getitem` |
| g2_agg | 1 | `g2_agg__sum_plus_one` |

Cast/try_cast of a named attribute answers `df.select(c).columns` with a
`datafusion.public.__repark_cdf_<id>.x` qualifier on this tree. The golden stores
the trailing field (`x`) only — the UUID is session-local, not a display-string
contract.

## Evidence

**Red first (base `d3a20d53`, production unchanged).**
`test_column_display_goldens_match_committed_bytes` without the JSON:
`AssertionError: missing golden facade_2_column_display_goldens.json; set REPARK_FACADE_2_RECORD_GOLDENS=1 to record`.

**C-005 mutation (scratch, restored).** One character at
`python/repark/src/repark/spark/column.py:399`: `negative(` → `negativx(`.

```
FAILED python/repark/tests/test_facade_2_column_display_goldens.py::test_column_display_goldens_match_committed_bytes
AssertionError: golden byte mismatch missing=[] extra=[] changed=['g2_unary__neg', 'g2_unary__neg_add', 'g2_unary__neg_neg']
1 failed in 0.23s
```

`git checkout -- python/repark/src/repark/spark/column.py` then 3 passed. No product
file remains in the commit.

**Change.** Tests + JSON + ledger + maps only. No product code under
`python/repark/src/` or `crates/`.

**Gates.** Named golden+audit pytest 225 passed in 5.32s.
`make verify` exit 0 (cargo test workspace 3013 passed across 55 suites; lib-py 664
files; ledger-grammar 117 live ledgers / 742 clauses). Parity suite
`PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q`:
749 passed, 1 skipped, 12 xfailed in 573.07s.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: facade-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: 221 golden cases recorded from the base tree cover every §4 Group-1 site and every named Group-2 family. Missing JSON was red; compare is byte-identical after record.
      artifacts: [python/repark/tests/test_facade_2_column_display_goldens.py, python/repark/tests/facade_2_column_display_goldens.json]
    - id: AT-2
      status: ATTACKED
      evidence: Mutation of one display character in column.py redded the three unary-neg cases; restore greened them. Audit pin files 225 passed with the new golden file.
      artifacts: [python/repark/tests/test_facade_2_column_display_goldens.py, python/repark/tests/test_columns.py]
    - id: AT-3
      status: ATTACKED
      evidence: Record mode is refused when CI or GITHUB_ACTIONS is set. Pin test_record_mode_fails_when_ci_is_set monkeypatches both.
      artifacts: [python/repark/tests/test_facade_2_column_display_goldens.py]
    - id: AT-4
      status: N/A
      justification: Pins-only step. No new shared mutable state, lock, or async spawn.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, or path handling. Goldens are committed JSON next to the test.
    - id: AT-6
      status: ATTACKED
      evidence: isinstance(c, repark.Column) holds; repark.Column is the exported spark.column.Column class. No public name added or removed.
      artifacts: [python/repark/tests/test_facade_2_column_display_goldens.py]
    - id: AT-7
      status: N/A
      justification: Pins-only. No performance claim in this step.
    - id: AT-8
      status: ATTACKED
      evidence: New test is 391 lines under the default 1000 ceiling. No baseline raised in check_lib_py.py, check_rust_file_size.py, or the CAP-1 mirror. No comment bytes in the code diff.
      artifacts: [python/repark/tests/test_facade_2_column_display_goldens.py]
    - id: AT-9
      status: N/A
      justification: No log-format or diagnosis-path change.
    - id: AT-10
      status: ATTACKED
      evidence: Golden compare failed without the JSON and after the one-character mutation; it passes on the recorded file. Audit pin list stays green.
      artifacts: [python/repark/tests/test_facade_2_column_display_goldens.py, python/repark/tests/facade_2_column_display_goldens.json]
  complete: true
```
