# Unit ledger — FACADE-2 · Column display strings rendered in Rust

**Date:** 2026-09-13 · **Branch:** `feat/facade-2-s3` · **Base:** `23bd047b`
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card FACADE-2 (audit §8): Spark display strings (`spark_display`,
`projection_name`, `sql_expr`, `join_sql_expr`) move from Python string assembly in
`column.py` / `functions*` into Rust on the native `PyColumn`. Step 2 moves every
§4 Group-2 `column.py` family into `crates/repark-python/src/column/display.rs`.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`. No JVM. Step 3 Group-1 constructors are a
later branch.

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
| C-008 | Goldens file is byte-identical and unedited vs step-1 commit `524e9edc`, and the compare pin is green. | `git diff 524e9edc -- python/repark/tests/facade_2_column_display_goldens.json` empty; `test_column_display_goldens_match_committed_bytes`. | **PROVEN** | `git diff` empty. Golden pin green on the Group-2 Rust renderer (3 passed with the isinstance and CI-guard pins). |
| C-009 | No Python text assembly remains in the Group-2 families (f-string / concat / `format` / `join` of display/SQL/join text). | `test_facade_2_group2_no_python_assembly.py`; red first on the step-1 tree. | **PROVEN** | Red on `524e9edc` (1 failed, 1 passed in 0.08s): 54 sites across `_binary`, `__neg__`, `__ne__`, `__invert__`, `eqNullSafe`, `substr`, `_string_predicate`, `_bitwise`, `is_null`, `is_not_null`, `_from_when_pairs`, `alias`, `__getitem__`, `cast`, `try_cast`. Green after the move: 2 passed. Refusals/`raise` f-strings stay allowed. `_with_sort_order` / `getItem` / `getField` had no assembly on the step-1 tree. |
| C-010 | The audit's five pin files and the whole facade suite green; counts compared with step-1. | `make py-test-facade` (or the Makefile facade target). | **PROVEN** | Five audit files + goldens + C-009: 227 passed in 4.96s (step-1 225 in 5.32s; +2 C-009 tests). Whole facade `python/repark/tests`: **5961 passed, 369 skipped** in 804.73s. |
| C-011 | Depth-100 binary+cast+alias chain (built, not executed) and depth-100 `withColumn` chain; 3+ reps after warmup; medians; no regression beyond 5%. | RELEASE `maturin develop --release` on step-1 vs this branch (R9-D-6: a debug build is not a ranking measurement). | **PROVEN** | Release re-measure after F1/F2/F5: depth-100 chain **+2.77%** (paired reps +1.45 … +4.07%), depth-500 **+2.55%**, `withColumn`-100 **−1.38%** on the stationary interleaved pass of the orchestrator's S2-21 re-check (release natives both trees). The Step-2b passes in the evidence (−34.44% / −42.31%) ran against a drifting base worker (3.55 → 6.2 ms) and are not a speedup. Bar ≤ +5%: met on every deciding case. Earlier debug-build read was +10.12%/−1.55%; the reviewer's release read on the pre-remediation branch was +8.65%/+8.48% depth-100, +4.93% depth-500, −3.65% withColumn. |
| C-012 | Gates: `cargo test -p repark-python`, `make verify`, whole parity suite. | Commands and counts in Evidence. | **PROVEN** | `cargo test -p repark-python` exit 0. `make verify` exit 0 (workspace cargo test 55 suites). Parity: **749 passed, 1 skipped, 12 xfailed** in 562.97s (step-1 749/1/12 in 573.07s). Re-run in step 2b: see Step-2b evidence. |
| C-013 | Review S2-21 findings F1–F5 dispositioned: F1 owned part tuples → borrowed `&str` on every named entry point and `needless_pass_by_value` dropped; F2 `alias` no longer round-trips `sql_expr` through Rust (Python passes the existing string through); F5 `wrap_*` builders pre-size with `String::with_capacity`; F3 `case_when` moves `.expr`; F4 generator `cast` returns the same `PyColumn` handle via `Bound`/`unbind`. | `display.rs` diff; byte-identical goldens pin; C-011 release re-measure under 5%. | **PROVEN** | All five FIXED. F1 `display.rs:252` (+ every sibling signature); F2 `display.rs:491` + `column.py:955`; F3 `display.rs:468`; F4 `display.rs:543`; F5 `display.rs:12-196`. `case_when` arm vectors stay owned `(String, String)` — `Vec<(&str, &str)>` cannot satisfy `for<'a> FromPyObject`; the Vecs are consumed by `format_case_body`, so the impl-level allow still drops. C-009 pin green on the `sql_expr` passthrough (a bare `sql_expr_part()` call is reuse, not assembly). Goldens byte-identical; release chain −34.44%. |
| C-014 | Group-1 sites no longer call `_native.PyColumn.sql`; `F.expr` is the sole allowed caller (its text is the caller's). | `test_group1_sites_only_call_pycolumn_sql_inside_expr` — AST walk of every `spark/*.py` module; red first on base `23bd047b`. | **PROVEN** | Red on base: `['functions.py:lit' ×3, 'functions.py:_lit_numpy_ndarray', 'functions.py:expr', 'functions_expr.py:pi', 'functions_session.py:uuid']` — 7 calls, 6 outside `expr`. Green on branch: sole caller `functions.py:expr`. |
| C-015 | Step-1 goldens file byte-identical and unedited; ≥30 new generic-builder `F.<fn>` cases (arities 0–3, nested, aliased) recorded from the step-2 tree before the move. | `facade_2_generic_builder_goldens.json`; `test_generic_builder_goldens_match_committed_bytes`; provenance re-record under the base release interpreter diffed byte-for-byte. | **PROVEN** | 39 cases (`gb_*`). Red-first on missing JSON: `AssertionError: missing golden facade_2_generic_builder_goldens.json`. Provenance: re-recorded with `/tmp/facade2-s3-base/.venv/bin/python` (release build of `23bd047b`) into the branch path; `diff` vs the committed file is **empty**. `git diff` on `facade_2_column_display_goldens.json` is empty. |
| C-016 | No Python text assembly left in the moved Group-1/generic helpers (`lit` temporal arms, `_lit_numpy_ndarray`, `_scalar`, `pi`, `uuid`). | `test_generic_helpers_do_not_assemble_display_sql_or_join_text` — extends the C-009 AST walk; red first on base `23bd047b`. | **PROVEN** | Red on base — 11 sites: `functions.py:lit:59/70/84`, `_lit_numpy_ndarray:277/278`, `_scalar:1232/1255/1256`. Green on branch (2 additional tests, 4 passed total in the pin file). |
| C-017 | Release measurement ≤ +5% on deciding cases: depth-100 `F.sqrt(c + 1)` chain, depth-100 generic-builder chain (`exp`/`floor`/`rint`/`signum` rotation — each probed linear-RSS on base at depth 10/40; `cbrt` excluded, superlinear like `abs`), 1000× `F.lit(datetime)`, `withColumn`-100. `F.abs` appears only at depth ≤ 6 (`abschain6`, informational). | Release natives both trees (`23bd047b` scratch clone vs branch), `prlimit --as=8589934592` + `MIMALLOC_ARENA_RESERVE=67108864` per worker (baseline VSZ 9.5 GB needs the smaller arena reserve to fit the 8 GB cap), idle-box wait before each pass, 1 warmup + 7 interleaved reps, medians + paired deltas, base-drift discard. | **PROVEN** | `chain_sqrt100` **+1.21%** (3.353 → 3.394 ms), `chain_fn100` **+1.25%** (4.154 → 4.206 ms), `litdt1000` **−98.37%** (293.279 → 4.771 ms — no engine re-parse, as predicted), `withcol100` **−0.30%** (333.528 → 332.534 ms), `abschain6` −34.69% informational (1.631 → 1.065 ms; one drifted pass discarded and rerun). Bar ≤ +5% met on every deciding case. **OOM note:** the first pass used a depth-100 `F.sqrt(F.abs(c) + 1)` chain; the base worker was OOM-killed at 84 GB anonymous RSS. Orchestrator measurement (recorded, not re-measured): chaining `F.abs` triples native memory per level (705 MB @12 → 1 963 MB @13 → 5 742 MB @14 → alloc failure) on main AND branch; `+1`/`.cast("double")`/`F.sqrt` chains stay flat at 76 MB through depth 40 — a pre-existing defect in `F.abs`, outside step-3 scope, carded separately. |
| C-018 | Step-4 inventory: remaining Python display assembly by module/helper is enumerable and intentional (bespoke display rules), or not Column render text at all. | AST sweep of `python/repark/src/repark/spark/*.py` with the C-009/C-016 detector. | **PROVEN** | Inventory table below. `functions.py`: `expr` (allowed parser caller), `coalesce`, `concat` (null-guard CASE), `abs`, aggregates `sum/count/count_distinct/avg/min/max/first/last/collect_list/collect_set`, `ntile`, `_date_fn` + `add_months`/`date_add`/`date_format`/`trunc`/`date_trunc`, `_partition_transform`/`bucket`. `functions_expr.py`: `dayname`, `monthname`, `isnull`, `struct`, `array_contains`, `overlay`, `stddev`/`stddev_pop`/`variance`/`var_pop`/`median`/`grouping`/`approx_count_distinct`/`_binary_aggregate`, `bit_and`/`bit_or`/`bit_xor`, `percentile_approx`, `explode`/`explode_outer`, `pmod`. `functions_collections.py`: `named_struct`, `create_map` (display override only). `functions_window.py`: `lag`/`lead`/`nth_value`. `functions_try.py`: `try_sum`/`try_avg`. `functions_bitwise.py`: `bitwise_not`. `functions_session.py`: `version` (foldable value text, fixed `version()` display). `column.py`: `join_sql_part` QCOL ref token, `sql_expr_without_alias`/`spark_wrap_display_part` suffix comparisons (not assembly), `__repr__`, `_normalize_type_string`/`_spark_cast_type_name` cast-arg normalization. Non-Column modules swept and excluded (`types.py` DDL trees, `merge.py`/`polars.py` join SQL, `row.py`/`storage.py` reprs, `udtf.py`, `catalog.py`, `_csv_smart.py`, `_integral.py`, `_temp_views.py`, `_idents.py` quoting helpers, `functions_lambda.py`/`functions_udf.py`/`functions_declared.py`). |
| C-019 | Gates: goldens + pins + the audit's five pin files; `cargo test -p repark-python`; `make verify`; whole facade suite; whole parity suite. | Commands and counts in Step-3 evidence. | **PROVEN** | Focused (goldens + pins + five audit files): **230 passed** in 5.36s. `cargo test -p repark-python`: **99 passed** (74 lib + 25 bindings). `make verify`: exit 0 — workspace cargo test **3061 passed across 55 suites**, fmt/clippy/lib-py/ledger-grammar clean (first run red on missing C-017/C-018/C-019 `pins:` citations; fixed in `task/ledgers/staging/map.md`, `column/map.md`, `spark/map.md`). Facade `python/repark/tests`: **5964 passed, 369 skipped** in 994.91s (baseline 5961/369; +3 = generic golden + two step-3 pins). Parity: see Step-3 evidence. |
| C-020 | P2-1 fixed exactly as step 2 fixed `binary`: operand `Expr`s move out of the extracted `PyColumn`s (`inners.into_iter().map(|c| c.expr)`) into `call_scalar_expr`'s owned `Vec<Expr>` — no second clone on top of the operational one; each part list extracts in one pass to `PyBackedStr` handles (three vecs, zero-copy); the fix is not slower than `cfa8ad2e`. | `display.rs` diff; goldens byte-identical (both files unedited); before/after release measurement on the same capped harness. | **PROVEN** | `cfa8ad2e` vs fix, `chain_sqrt100` **−17.50%** (3.369 → 2.780 ms), `chain_fn100` **−17.56%** (4.161 → 3.431 ms). Extract-clone + `expr()`-clone removed → only the `Vec<PyColumn>` extraction clone and `call_scalar_expr`'s operational clone remain. Focused goldens+pins green on the fix. |
| C-021 | Review S2-21 findings dispositioned: no P1; P2-1 fixed (C-020); P3-1/P3-2 recorded, no change. | Review report `/tmp/oc-worker/grok-rev-facade2s3/report.md` read whole; findings table below. | **PROVEN** | P3-1: numpy array literals still walk elements in Python (`_lit_numpy_ndarray`, `functions.py:252-275`) — pre-existing; a later unit could accept the numpy buffer in Rust and build one `ScalarValue::List`. P3-2: `lit_timestamp` parses through a one-element Arrow array — already −97%; optional chrono-parse micros + `"UTC"` intern is not ranking-class. Reviewer `abschain6` clean pass −4.30% vs actor's −34.69% — the actor's pass sat on a drifted base (1.154 → 2.475 ms); `abschain6` stays informational only, not a deciding case. |

VERDICT: 21 clauses, 21 PROVEN, 0 OPEN, 0 REJECTED. Step-3 release re-measure clears the C-017 bar on every deciding case; the P2-1 remediation re-measure is faster than `cfa8ad2e` on both deciding chains.

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
      status: ATTACKED
      evidence: C-011 re-measured on RELEASE natives after the step-2b string-FFI remediation. Stationary re-check pass: depth-100 op chain +2.77%, depth-500 +2.55%, withColumn-100 −1.38% — all under the 5% bar; the actor's −34.44%/−42.31% passes had a drifting base and are not a speedup.
      artifacts: [task/ledgers/staging/facade-2-ledger.md]
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

## Step 2 evidence (2026-09-12)

**Change.** Group-2 families in `column.py` call `_native.PyColumnParts.*` and unpack a
`(PyColumn, spark_display, sql_expr, join_sql_expr)` tuple. Rendering lives in
`crates/repark-python/src/column/display.rs`. `column.py` 1589 → 1549 (ratchet down in
`scripts/check_lib_py.py` and the CAP-1 mirror). Goldens JSON unedited vs `524e9edc`.

**C-009 red first** on `524e9edc` (product unchanged):

```
FAILED test_group2_methods_do_not_assemble_display_sql_or_join_text
AssertionError: Group-2 Python text assembly remains: ['__getitem__:1037', … 54 sites]
1 failed, 1 passed in 0.08s
```

Green after the move: 2 passed.

**C-011** `maturin develop` (dev/debug), 1 warmup + 5 reps, same box, rebuild each side.

| chain | step-1 median ms | step-2 median ms | delta |
|---|---|---|---|
| depth-100 `(c+1).cast("double").alias(...)` built, not executed | 19.000 | 20.924 | **+10.12%** |
| depth-100 `withColumn` built, not executed | 2894.651 | 2849.876 | −1.55% |

**Gates.** Facade `python/repark/tests`: 5961 passed, 369 skipped in 804.73s.
`make verify` exit 0. Parity: 749 passed, 1 skipped, 12 xfailed in 562.97s.
pins: facade-2/C-010, C-012

## Step 2b evidence (2026-09-12, review S2-21 remediation)

**Change.** `display.rs`: all operand part tuples take borrowed `&str` (F1 — the impl-level
`clippy::needless_pass_by_value` allow is dropped; remaining owned args are consumed);
`alias` returns `(PyColumn, spark_display)` and `column.py` keeps `sql_expr` as a
passthrough (F2); `case_when` moves `.expr` out of its arm pairs (F3); the generator arm
of `cast` takes `Bound<PyColumn>` and returns the same handle via `unbind` (F4); every
`wrap_*` builder and `format_case_body` pre-sizes with `String::with_capacity` + `push_str`
instead of `format!` (F5). `column.py` 1549 → 1548 (ratchet down in `check_lib_py.py` and
the CAP-1 mirror). Goldens JSON unedited.

**C-011 — RELEASE** (`uvx maturin@1.14.1 develop --release`; `__debug_assertions__` False on
both sides). Step-1 base `/tmp/grok-rev-facade2/base` (read-only) vs this clone. Same
interleaved-worker harness as the reviewer (`/tmp/facade2-measure/`), 1 warmup + 7 reps,
base/branch order alternated per rep, `gc.disable()` around `perf_counter`.

Superseded as the ranking measurement by the S2-21 re-check's stationary pass (depth-100 +2.77%, depth-500 +2.55%, 1000× depth-1 +3.56%, `withColumn`-100 −1.38%); the two passes below are kept as recorded.

Pass 1 (`run_interleaved.py`):

| case | base median ms | branch median ms | delta |
|---|---:|---:|---:|
| (a) depth-100 op chain | 6.490 | 3.744 | **−42.31%** |
| (a) depth-500 op chain | 75.589 | 77.890 | **+3.04%** |
| 1000× depth-1 triples | 13.611 | 13.952 | +2.51% |
| (b) depth-100 `withColumn` | 288.856 | 281.510 | **−2.54%** |

Pass 2 (`run_confirm.py`):

| case | base median ms | branch median ms | delta |
|---|---:|---:|---:|
| (a) depth-100 op chain | 6.072 | 3.981 | **−34.44%** |
| `select` of 16 plain names | 0.308 | 0.204 | −33.83% |
| `collect()` 50 000 rows | 22.996 | 20.062 | −12.76% |
| `select`+`collect` 50 000 rows | 17.597 | 18.209 | +3.48% |

Base worker drifted upward inside pass 1 (reps 3.905 → 6.951 while branch stayed ~3.7);
even against the base's fastest rep the branch is at parity or better. The deciding
depth-100 chain is **faster than step-1** on release: borrowing `&str` removed the
per-triple inbound UTF-8 copies (9 `String` extracts, up to ~1.9 KB each at depth 100)
and F2 removed the alias `sql_expr` round-trip. `ops100_alloc` tracemalloc peak is
byte-identical to the reviewer's (13 279 bytes) on both trees.

Non-deciding note: the 2 000-row `select_collect` read +48% at the ~1 ms noise floor in
pass 1; re-measured at 50 000 rows it is +3.48% — the same sub-5% noise the reviewer saw
(+0.72%). No regression on non-column paths.

**Gates.** Golden file + C-009 pin + the audit's five pin files: 227 passed in 5.62s.
`cargo test -p repark-python` 91 passed. `make verify` exit 0 (workspace cargo test 3053
across 55 suites; ledger-grammar 119 live ledgers clean). Facade `python/repark/tests`:
**5961 passed, 369 skipped** in 895.06s (round-1 5961/369). Parity:
**749 passed, 1 skipped, 12 xfailed** in 603.76s (round-1 749/1/12). `git diff
origin/main` on the goldens JSON is empty. One transient flake in the first `verify`
run — `repark-iceberg` `listing_cost_list_tables_cheaper_than_provider_rebuild`, a
wall-clock cost assertion unrelated to this diff — passed in isolation and on the
`verify` re-run.
pins: facade-2/C-011, C-013

## Step 3 evidence (2026-09-13)

**Change.** Group-1 `PyColumn.sql` entry points become typed native constructors in
`crates/repark-python/src/column/display/construct.rs` (new directory, own `map.md`):
`lit_timestamp` (Arrow-parse fold to `TimestampMicrosecond`, falling back to the
`to_timestamp(__repark_decimal_cast_nullable__(text))` analyzer shape for out-of-range or
unparsable text — measured identical to the old SQL path, including the year-9999
nanosecond-representability error at execution), `lit_date` (`Cast(lit(text), Date32)`),
`lit_time` (`Cast(lit(text), Time64(Nanosecond))`), `lit_array_cast` (native list cast with
`Field::new("item", element_type, true)` — field name measured via Arrow schema export),
`pi`, `uuid`. The generic `name(args)` render moves to `PyColumnParts.call_scalar` in
`display.rs`, which builds the `Expr` through `call_scalar_expr` and renders display / SQL /
join parts in Rust; `_scalar` keeps foldability / aggregate / window / free-attribute
bookkeeping in Python and passes borrowed `Bound<PyList>` fragment lists (no owned `String`
per operand — the S2-21 lesson). `F.expr` still parses caller SQL through `PyColumn.sql`.

**C-014 / C-016 red first** on base `23bd047b` (pin file copied into the base tests dir;
product unchanged):

```
FAILED test_group1_sites_only_call_pycolumn_sql_inside_expr
AssertionError: _native.PyColumn.sql callers outside F.expr:
['functions.py:lit', 'functions.py:lit', 'functions.py:lit',
 'functions.py:_lit_numpy_ndarray', 'functions.py:expr',
 'functions_expr.py:pi', 'functions_session.py:uuid']
FAILED test_generic_helpers_do_not_assemble_display_sql_or_join_text
AssertionError: Group-1/helper Python text assembly remains:
['functions.py:_lit_numpy_ndarray:278', 'functions.py:_lit_numpy_ndarray:277',
 'functions.py:_scalar:1256', 'functions.py:_scalar:1256',
 'functions.py:_scalar:1255', 'functions.py:_scalar:1255',
 'functions.py:_scalar:1232', 'functions.py:_scalar:1232',
 'functions.py:lit:84', 'functions.py:lit:70', 'functions.py:lit:59']
2 failed, 2 passed in 0.51s
```

Green on branch: 4 passed (`F.expr` sole caller; zero assembly sites in the moved helpers).

**C-015.** `facade_2_generic_builder_goldens.json` — 39 `gb_*` cases, arities 0–3,
variadic, nested, aliased. Red-first on missing file:
`AssertionError: missing golden facade_2_generic_builder_goldens.json; set REPARK_FACADE_2_RECORD_GOLDENS=1 to record`.
Provenance re-check (orchestrator ask): re-recorded through the base tree's release
interpreter (`/tmp/facade2-s3-base/.venv/bin/python`, release build of `23bd047b`,
`REPARK_FACADE_2_RECORD_GOLDENS=1`) writing the branch path; `diff` vs the committed
file is **empty** — the bytes are the pre-move tree's.

**C-017 — RELEASE** (`uvx maturin@1.14.1 develop --release`; `__debug_assertions__` False
both sides; base = `/tmp/facade2-s3-base` at `23bd047b`). Same interleaved-worker harness
as C-011 (`/tmp/facade2-measure/worker_s3.py` + `run_s3.py`): 1 warmup + 7 reps,
base/branch order alternated, `gc.disable()` around `perf_counter`, idle-box wait before
each pass, pass discarded when base drifts outside `[0.8, 1.25]×` its own first rep.
Workers run under `prlimit --as=8589934592` with `MIMALLOC_ARENA_RESERVE=67108864`
(baseline VSZ is ~9.5 GB from mimalloc arena reservations; the 64 MB reserve brings it to
~7.5 GB so the prescribed 8 GB cap fits; RSS growth beyond the cap is still a measurement
failure).

| case | base median ms | branch median ms | delta |
|---|---:|---:|---:|
| `chain_sqrt100` depth-100 `F.sqrt(c + 1)` | 3.353 | 3.394 | **+1.21%** |
| `chain_fn100` depth-100 `exp`/`floor`/`rint`/`signum` rotation | 4.154 | 4.206 | **+1.25%** |
| `litdt1000` 1000× `F.lit(datetime)` | 293.279 | 4.771 | **−98.37%** |
| `withcol100` depth-100 `withColumn` | 333.528 | 332.534 | **−0.30%** |
| `abschain6` depth-6 `F.abs(c + 1)` (informational) | 1.631 | 1.065 | −34.69% |

Candidate-screen RSS probes on base (depth 10 → 40, under the cap): `exp`, `floor`,
`rint`, `signum`, `expm1`, `ln`, `log10`, `degrees`, `sqrt` flat at ~50 MB; `cbrt`
exploded (444 MB, 3.6 M-char display at depth 10) and was excluded alongside `abs`.
One `abschain6` pass discarded for base drift (1.154 → 2.475) and rerun clean.
Bar ≤ +5%: met on every deciding case; `litdt1000` −98.37% is the expected win — the old
path ran `analyze_eagerly` on `TIMESTAMP '…'` text per literal.

**OOM / `F.abs` note (orchestrator measurement, recorded not re-measured).** The first
C-017 pass used a depth-100 `F.sqrt(F.abs(c) + 1)` chain; the base worker was killed by
the global OOM killer at 84 GB anonymous RSS. Orchestrator probe: chaining `F.abs`
triples native memory per nesting level (705 MB @ depth 12 → 1 963 MB @13 → 5 742 MB @14
→ allocation failure) on `main` AND on this branch; `+1`, `.cast("double")` and `F.sqrt`
chains stay flat at 76 MB through depth 40. Pre-existing `F.abs` defect, outside
step-3 scope, carded separately; `abs` appears in step-3 measurements only at depth ≤ 6.

**C-018 step-4 inventory** — remaining Column-render text assembly by module/helper
(AST sweep with the C-009 detector; counts are assembly sites, several per helper):

| module | helpers (sites) |
|---|---|
| `functions.py` | `expr` 1 (allowed parser caller), `coalesce` 6, `concat` 8, `abs` 3, `sum`/`count`/`avg`/`min`/`max` 3 each, `count_distinct` 10, `first`/`last` 4 each (`IGNORE NULLS`), `collect_list`/`collect_set` 2 each, `ntile` 3, `_date_fn` 3, `add_months`/`date_add` 4 each (CAST arg), `date_format`/`trunc`/`date_trunc` 2 each, `_partition_transform` 1, `bucket` 1 |
| `functions_expr.py` | `dayname`/`monthname`/`array_contains`/`overlay`/`explode`/`explode_outer`/`pmod` 1-2 each, `isnull` 2, `struct` 6, `stddev`/`stddev_pop`/`variance`/`var_pop`/`median`/`grouping`/`approx_count_distinct`/`_binary_aggregate`/`bit_and`/`bit_or`/`bit_xor` 2 each, `percentile_approx` 6 |
| `functions_collections.py` | `named_struct` 6, `create_map` 3 (display override into `call_scalar`) |
| `functions_window.py` | `lag`/`lead`/`nth_value` 2 each |
| `functions_try.py` | `try_sum`/`try_avg` 3 each |
| `functions_bitwise.py` | `bitwise_not` 3 |
| `functions_session.py` | `version` 1 (foldable value text; display is the fixed `version()`) |
| `column.py` | `join_sql_part` 1 (QCOL ref token), `sql_expr_without_alias`/`spark_wrap_display_part` 1 each (`endswith` comparisons, not assembly), `__repr__` 1, `_normalize_type_string`/`_spark_cast_type_name` 1 each (cast-arg normalization) |

Swept and excluded — not Column render text: `types.py` (DDL tree/`simpleString`/`toDDL`),
`merge.py`/`polars.py` (join/MERGE SQL), `row.py`/`storage.py` (reprs), `udtf.py`,
`catalog.py`, `_csv_smart.py`, `_integral.py`, `_temp_views.py`, `_idents.py` (quoting),
`functions_lambda.py`, `functions_udf.py`, `functions_declared.py`, `functions_session.py`
beyond `version`.

**Gates.** Focused goldens + pins + audit five files: **230 passed** in 5.36s
(227 at step-2b + 1 generic golden + 2 new pins). `cargo test -p repark-python`
**99 passed**. `make verify` exit 0 (workspace cargo test **3061 passed across 55
suites**; fmt/clippy/lib-py/ledger-grammar clean). Facade `python/repark/tests`:
**5964 passed, 369 skipped** in 994.91s (step-2b 5961/369; +3 new tests). Parity:
**755 passed, 1 skipped, 12 xfailed** in 561.67s (768 collected, 0 failed;
step-2b recorded 749/1/12 — same suite and skip/xfail counts).
pins: facade-2/C-014, C-015, C-016, C-017, C-018, C-019

## Step 3 remediation evidence (2026-09-13, review S2-21 of `cfa8ad2e`)

**Review.** `/tmp/oc-worker/grok-rev-facade2s3/report.md` (Grok 4.6, read-only clone at
`cfa8ad2e`). No P1 — actor C-017 deltas confirmed within ±3 pp on every deciding case;
non-column `select`/`collect` do not regress; `lit_array_cast` on 1e5 ints O(n) and
−85.70% vs base. One P2, two P3.

**Reviewer C-017 re-measure** (same harness shape, interleaved vs `/tmp/facade2-s3-base`):

| case | base median ms | branch median ms | delta | vs actor |
|---|---:|---:|---:|---|
| `chain_sqrt100` | 3.333 | 3.365 | **+0.97%** | 0.24 pp |
| `chain_fn100` | 4.137 | 4.199 | **+1.51%** | 0.26 pp |
| `litdt1000` | 285.907 | 8.237 | **−97.12%** | 1.25 pp |
| `litdate1000` (asked; not in actor table) | 271.130 | 5.588 | **−97.94%** | — |
| `withcol100` | 328.148 | 331.524 | **+1.03%** | 1.33 pp |
| `abschain6` informational | 1.289 | 1.234 | **−4.30%** | actor −34.69% did not hold (drifted base pass); informational only |

**P2-1 fix.** `call_scalar` (`display.rs`): `inners.into_iter().map(|column| column.expr)`
moves the `Expr` out of each extracted `PyColumn` — the `.iter().map(PyColumn::expr)`
clone is gone; `call_scalar_expr` already takes owned `Vec<Expr>` so no signature change.
Each part list extracts in one pass: `list.iter().map(extract::<PyBackedStr>)` — three
`Vec<PyBackedStr>` (owned zero-copy refs) replace three `Vec<Bound<PyAny>>` + three
`Vec<&str>`. `wrap_call` is now generic over `S: AsRef<str>`. Remaining clones per child:
the `Vec<PyColumn>` extraction clone and `call_scalar_expr`'s operational clone — the
borrow path (`Vec<Bound<PyColumn>>` + `borrow().expr.clone()`) would clone the same
`Expr` once, so owned extract + move is the minimal shape without touching the
`call_scalar_expr` dispatch table or its other callers (`mod.rs` `PyColumn.call_scalar`,
`call_two`). No behaviour change; both golden JSON files unedited.

**C-020 before/after** — release natives, same capped harness (`prlimit --as=8589934592`,
`MIMALLOC_ARENA_RESERVE=67108864`), before = `/tmp/facade2-s3-before` at `cfa8ad2e`,
after = this branch, 1 warmup + 7 interleaved reps, idle-box wait, drift-band discard:

| case | `cfa8ad2e` median ms | fix median ms | delta |
|---|---:|---:|---:|
| `chain_sqrt100` | 3.369 | 2.780 | **−17.50%** |
| `chain_fn100` | 4.161 | 3.431 | **−17.56%** |

Faster on both deciding chains — the removed per-level `Expr` clone and the three
eliminated vec allocations show up end-to-end. Bar "not slower": met.

**Gates.** Focused goldens + pins + the audit's five pin files on the fix: **230
passed** in 5.78s; both golden JSON files byte-identical (unedited). `cargo test -p
repark-python`: **99 passed** (74 lib + 25 bindings). `make verify`: exit 0 — workspace
cargo test **3061 passed across 55 suites**, ledger-grammar clean after citing C-021 in
`spark/map.md`. Parity `python/repark-parity/tests`: **755 passed, 1 skipped, 12
xfailed** in 566.01s. Facade suite not re-run — the Rust diff touches only
`call_scalar` and its private helpers (`str_list`, `wrap_call`), inside the brief's
scope for the focused set.
pins: facade-2/C-020, C-021
