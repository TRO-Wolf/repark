# Unit ledger — FACADE-3 · `createDataFrame` inference in Rust — step 1 (measure + pins)

**Date:** 2026-09-13 · **Branch:** `feat/facade-3-s1` · **Base:** `23bd047b`
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card FACADE-3 (audit §8): rows/tuples/dicts → Arrow inference moves to
Rust through the FACADE-1 capsule seam. Step 1 is measurement and pins only — the
release baseline table per shape, the golden corpus (schema + nullability + values +
refusal class/message), the pickle round-trip pin, and the step-2 target list. No
product code under `python/repark/src/` or `crates/` changes in this step.

**Not in this step:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`. No JVM, no `test_interchange_parity.py` live
legs (D-4). Steps 2–3 are later branches.

## PROPOSITION LEDGER — FACADE-3 step 1 — 2026-09-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Release baseline table per shape at 1e4 and 1e5 rows × 7 columns (int, float, string, bool, date, timestamp, decimal): list of tuples, list of `Row`, list of dicts, tuples + DDL, tuples + `StructType`, nested, pandas and polars controls; `createDataFrame` alone and `createDataFrame(...).count()`; warmup + 3 reps, medians; cProfile split for the three slowest shapes. | `repark._native.__debug_assertions__ is False`; `docs/perf/facade-3-cdf-baseline-2026-09-13.md` + committed runner. | **PROVEN** | `__debug_assertions__` False on `167,472,456 B` native. Full table in the doc: nested 2,852 ms > tuples_struct 2,089 ≈ tuples_ddl 2,058 > rows 1,061 > dicts 947 > tuples 736 > pandas 434 ≈ polars 424 (1e5 create medians). cProfile: `_prepare_nested_cell` / `_normalize_create_dataframe_cell` / per-cell timestamp-localize / decimal envelope are the wall on every shape. Idle-box wait per cell (`pgrep cargo/rustc/maturin`); run load 12.19 → 4.44, floor 17.45 ms. |
| C-002 | Goldens cover every dispatch shape: tuples, `Row`, dicts, pandas/polars controls, explicit DDL / `StructType` / name-list / bare-DataType schemas, nested (struct/array/map), `None` in every position, mixed-type merges, decimals at differing scales, naive + tz-aware timestamps, dates, bytes/`bytearray`/`memoryview`, `verifySchema` on/off, empty inputs with and without schema — ≥120 cases, each recording `schema.simpleString()`, field nullability, `repr()` of collected rows. | `test_facade_3_create_dataframe_goldens.py` + committed `facade_3_create_dataframe_goldens.json`; case ids by family below. | **PROVEN** | 156 cases recorded from base `23bd047b` (≥120). Red-first: missing JSON → `AssertionError: missing golden facade_3_create_dataframe_goldens.json`. Compare byte-identical after record (3 passed). |
| C-003 | Refusal cases record the exception class name and exact message for every refusal the corpus touches. | `refused` entries in the JSON golden. | **PROVEN** | 46 refusal entries: PySparkTypeError/PySparkValueError/AnalysisException plus bare `TypeError` (`verifySchema`, `samplingRatio`, non-str dict keys), bare `OverflowError` (int64 overflow), bare `InvalidOperation` (decimal envelope edge at 10^20−1 — the quantize itself overflows Python's 28-digit context). Record mode refused under `CI`/`GITHUB_ACTIONS` (dedicated pin). |
| C-004 | Pickle round-trip on collected `Row` objects and the frame schema asserts equality — pins exactly what pickling supports today (measured first). | `test_pickle_round_trip_on_inferred_frames`. | **PROVEN** | Measured on base first: `Row` round-trips via `Row.from_ordered_fields` (named AND positional `Row(1,'x')` keep `__fields__`); `StructType`/`StructField` round-trip with `==`. Pin: 7 frames (tuples, Row, dicts, temporal+decimal+bytes, nested, DDL-schema) — schema equality plus value AND `__fields__` equality on every collected row. Green on base (pins today's support; nothing changed). |
| C-005 | Mutation proof: one inference-rule change in a scratch copy turns a golden red; restore leaves it green. | Scratch edit; red output pasted; `git checkout` restore. | **PROVEN** | `_fast_column_arrow_type` `{int}` → `pa.int32()` at `create_dataframe_columns.py:219`. Red: `golden byte mismatch missing=[] extra=[] changed=['dict_all_none_then_value', 'dict_basic', 'dict_key_union', 'dict_missing_null_fill', 'dict_none_value', 'dict_schema_rename', 'dict_schema_reorder', 'dict_sorted_order', 'row_basic', 'row_in_tuple_cell', 'row_nested', 'row_none_cell']` (12 of N shown, first-12 truncation). `git checkout` restore → 3 passed. No product file in the commit. |
| C-006 | The card's named pins stay green unchanged: `test_create_dataframe_materialize.py`, `test_perf_facade_cdf_1.py`, `test_csv_infer_perf_1.py`. | Those files, same commit, no edits. | **PROVEN** | New goldens + the three named files: **145 passed, 4 skipped** in 14.58s (debug native restored via `make develop`). The three files were not edited. |
| C-007 | Step-2 target list named from C-001's numbers: which shapes are the wall and why. | Evidence section + doc §"What the table says". | **PROVEN** | Ranked 1e5 create medians: (1) nested 2,852 ms — three slow columns re-walk every cell; (2) tuples+StructType 2,089 / tuples+DDL 2,058 ms — the audit's explicit-schema wall, now per-cell normalize + prepare + localize + envelope; (3) rows 1,061 / dicts 947 ms; (4) tuples 736 ms — fast census leaves the timestamp/decimal per-cell residue; (5) pandas 434 / polars 424 ms — per-cell localize + envelope scans make even the controls Python-bound on this fixture. Step 2 should take the explicit-schema pair and nested first, then rows/dicts — all share the same per-cell helpers. |
| C-008 | Gates green: new goldens + pickle pin + the three named pins, `make verify`, whole parity suite (`python/repark-parity/tests`). | Commands and counts in Evidence. | **PROVEN** | See Evidence — all green. |

## Case ids by family (156 cases)

Recorded from base `23bd047b` on 2026-09-13 against a UTC session
(`spark.sql.session.timeZone=UTC`, `spark.sql.timestampType=TIMESTAMP_LTZ`).

| Family | n | Case ids |
|---|---|---|
| tup | 41 | `tup_all_none`, `tup_bool_int_merge`, `tup_bool_only`, `tup_bytearray_only`, `tup_bytes_only`, `tup_date_only`, `tup_date_timestamp_merge`, `tup_decimal_float_merge`, `tup_empty_row`, `tup_float_bool_merge`, `tup_float_inf`, `tup_float_int_merge`, `tup_float_nan`, `tup_float_neg_inf`, `tup_hetero_row_element`, `tup_int_bool_merge`, `tup_int_decimal_merge`, `tup_int_float_merge`, `tup_int_string_merge`, `tup_large_int`, `tup_list_rows`, `tup_memoryview_only`, `tup_mixed_bytes_bytearray`, `tup_namedtuple`, `tup_namedtuple_schema_reorder`, `tup_none_bool`, `tup_none_bytes`, `tup_none_date`, `tup_none_decimal`, `tup_none_float`, `tup_none_lead`, `tup_none_mid`, `tup_none_tail`, `tup_none_timestamp`, `tup_ragged`, `tup_scalars`, `tup_seven`, `tup_string_int_merge`, `tup_time_only`, `tup_timestamp_date_merge`, `tup_timestamp_int_merge` |
| row | 14 | `row_basic`, `row_decimal`, `row_deep_nested`, `row_hetero_element`, `row_in_tuple_cell`, `row_missing_field`, `row_nested`, `row_none_cell`, `row_positional`, `row_schema_ddl`, `row_schema_partial`, `row_schema_rename`, `row_schema_reorder`, `row_schema_struct` |
| dict | 18 | `dict_all_none_then_value`, `dict_basic`, `dict_hetero_element`, `dict_key_union`, `dict_missing_null_fill`, `dict_nested_empty`, `dict_nested_map_conf`, `dict_nested_map_conf_uniform`, `dict_nested_none_values`, `dict_nested_struct_conf`, `dict_non_str_key`, `dict_none_value`, `dict_schema_ddl`, `dict_schema_partial`, `dict_schema_rename`, `dict_schema_reorder`, `dict_schema_struct`, `dict_sorted_order` |
| nest | 24 | `nest_array_ddl`, `nest_dict_cell_map_conf`, `nest_dict_cell_map_conf_uniform`, `nest_dict_cell_struct`, `nest_list_all_empty`, `nest_list_empty_then_value`, `nest_list_float_dense`, `nest_list_int`, `nest_list_int_float_merge`, `nest_list_mixed_width`, `nest_list_none_elements`, `nest_list_of_dict`, `nest_list_of_list`, `nest_list_of_row`, `nest_list_str_int_merge`, `nest_map_ddl`, `nest_map_int_keys`, `nest_row_cell`, `nest_sparse_vector`, `nest_struct_ddl`, `nest_struct_schema`, `nest_tuple_cell`, `nest_tuple_empty`, `nest_tuple_mixed` |
| dec | 13 | `dec_explicit_ddl`, `dec_explicit_struct`, `dec_infinity`, `dec_large_in_envelope`, `dec_magnitude_over`, `dec_mixed_scales`, `dec_nan`, `dec_negative`, `dec_scale18`, `dec_scale2`, `dec_scale_over`, `dec_with_none`, `dec_zero` |
| ts | 9 | `ts_date`, `ts_explicit_ddl`, `ts_explicit_ntz`, `ts_mixed_naive_tz`, `ts_naive`, `ts_none_date`, `ts_time`, `ts_tz_ny`, `ts_tz_utc` |
| bin | 4 | `bin_bytearray`, `bin_bytes`, `bin_memoryview`, `bin_none_mix` |
| sch | 19 | `sch_bad_ddl_string`, `sch_bare_datatype`, `sch_bare_long`, `sch_bare_nonscalar_cell`, `sch_ddl`, `sch_empty_bare_datatype`, `sch_empty_ddl`, `sch_empty_names`, `sch_empty_struct`, `sch_int32_preserved`, `sch_names`, `sch_names_long`, `sch_names_nonstr`, `sch_names_short`, `sch_sampling_ratio`, `sch_schema_int`, `sch_struct`, `sch_verify_schema_false`, `sch_verify_schema_true` |
| misc | 5 | `misc_dict_input`, `misc_empty_no_schema`, `misc_scalar_list`, `misc_set_input`, `misc_str_input` |
| pd | 5 | `pd_basic`, `pd_empty`, `pd_schema_ddl`, `pd_schema_names`, `pd_schema_reorder` |
| pl | 4 | `pl_basic`, `pl_empty`, `pl_nested`, `pl_schema_names` |

Measured-on-base notes a Rust move must preserve byte-for-byte: inferred `bytes` /
`bytearray` / `memoryview` cells build a `binary` Arrow column that surfaces as
`string` in `simpleString` (`struct<_1:string>`); top-level int+str refuses
(`cannot build Arrow column`) while the same merge inside nested positions promotes;
dict cells under the default conf infer as struct (`inferNestedDictAsStruct=true`
is the repark default) and under `false` become `map<string,string>` with
stringified values; `verifySchema`/`samplingRatio` are bare `TypeError`s (the
facade signature is `(data, schema)`); `Decimal("10^20 − 1")` dies on the envelope
quantize itself as a bare `decimal.InvalidOperation`; `2**70` dies as a bare
`OverflowError`.

## Evidence

**Red first (base `23bd047b`, production unchanged).**
`test_create_dataframe_goldens_match_committed_bytes` without the JSON:
`AssertionError: missing golden facade_3_create_dataframe_goldens.json; set REPARK_FACADE_3_RECORD_GOLDENS=1 to record`.

**C-001 measure.** `uvx maturin@1.14.1 develop --release`, `__debug_assertions__`
False, then the committed runner — 32 cells (8 shapes × 2 sizes × 2 ops), medians
of 3 after 1 warmup, `wait_for_idle` per cell. Numbers and the cProfile split are
in [docs/perf/facade-3-cdf-baseline-2026-09-13.md](../../../docs/perf/facade-3-cdf-baseline-2026-09-13.md);
raw JSON `/tmp/facade-3-cdf-baseline.json` (runner writes it). `make develop`
restored the debug build afterwards.

**C-005 mutation (scratch, restored).** `pa.int64()` → `pa.int32()` in
`_fast_column_arrow_type`:

```
FAILED test_create_dataframe_goldens_match_committed_bytes
AssertionError: golden byte mismatch missing=[] extra=[] changed=['dict_all_none_then_value', 'dict_basic', 'dict_key_union', 'dict_missing_null_fill', 'dict_none_value', 'dict_schema_rename', 'dict_schema_reorder', 'dict_sorted_order', 'row_basic', 'row_in_tuple_cell', 'row_nested', 'row_none_cell']
1 failed in 0.32s
```

`git checkout -- python/repark/src/repark/spark/session/create_dataframe_columns.py`
then 3 passed. No product file remains in the commit.

**Change.** Tests + golden JSON + perf doc + runner + ledger + maps only. No
product code under `python/repark/src/` or `crates/`.

**Gates.** All green on the debug native (`make develop` restored after the release
measure).

- `.venv/bin/python -m pytest python/repark/tests/test_facade_3_create_dataframe_goldens.py
  python/repark/tests/test_create_dataframe_materialize.py
  python/repark/tests/test_perf_facade_cdf_1.py
  python/repark/tests/test_csv_infer_perf_1.py -q` → **145 passed, 4 skipped** in 14.58s.
- `make verify` → exit 0: cargo fmt, workspace clippy (all-targets and
  lib/bins panic+async ban), crate-dag, lib-rs, rust-file-size, lib-py,
  python-conventions, docstring-presence, example-coverage, manifest,
  ledger lifecycle + ledger grammar clean.
- `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project
  python -m pytest python/repark-parity/tests -q` → **755 passed, 1 skipped,
  12 xfailed** in 601.28s. No JVM started; `REPARK_PARITY_LIVE` never set.
- Staged-diff comment scan (`git diff --cached … | grep -P '^\+\s*(//|#(?!\[|!\[| noqa))'`)
  prints nothing.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: facade-3-step-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: 156 golden cases recorded from the base tree cover every dispatch shape the card names; missing JSON was red; compare is byte-identical after record.
      artifacts: [python/repark/tests/test_facade_3_create_dataframe_goldens.py, python/repark/tests/facade_3_create_dataframe_goldens.json]
    - id: AT-2
      status: ATTACKED
      evidence: Mutation of one inference rule ({int} → int32) redded the golden across every affected family; restore greened it.
      artifacts: [python/repark/tests/test_facade_3_create_dataframe_goldens.py]
    - id: AT-3
      status: ATTACKED
      evidence: Record mode is refused when CI or GITHUB_ACTIONS is set; pin monkeypatches both.
      artifacts: [python/repark/tests/test_facade_3_create_dataframe_goldens.py]
    - id: AT-4
      status: N/A
      justification: Pins-only step. No new shared mutable state, lock, or async spawn.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, or path handling. Goldens are committed JSON next to the test.
    - id: AT-6
      status: ATTACKED
      evidence: Row A2 freeze held — no public name added or removed; verifySchema/samplingRatio pinned as the TypeError refusals they are today.
      artifacts: [python/repark/tests/facade_3_create_dataframe_goldens.json]
    - id: AT-7
      status: ATTACKED
      evidence: C-001 measured on a RELEASE native (__debug_assertions__ False); debug restored via make develop before gates.
      artifacts: [docs/perf/facade-3-cdf-baseline-2026-09-13.md]
    - id: AT-8
      status: ATTACKED
      evidence: New test is 721 lines under the default 1000 ceiling; runner lives under docs/perf (outside the check_lib_py scan roots). No baseline raised in check_lib_py.py, check_rust_file_size.py, or the CAP-1 mirror. No comment bytes in the code diff.
      artifacts: [python/repark/tests/test_facade_3_create_dataframe_goldens.py]
    - id: AT-9
      status: N/A
      justification: No log-format or diagnosis-path change.
    - id: AT-10
      status: ATTACKED
      evidence: Golden compare failed without the JSON and after the inference mutation; it passes on the recorded file. The three named pin files re-run green.
      artifacts: [python/repark/tests/test_facade_3_create_dataframe_goldens.py]
  complete: true
```
