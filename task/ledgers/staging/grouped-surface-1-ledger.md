# Charter ledger — GROUPED-SURFACE-1 · GroupedData apply/applyInArrow/cogroup + state refusals

**Date:** 2026-09-14 · **Branch:** `feat/grouped-surface-1` · **Base:** `e98e899d` ·
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `GROUPED-ARROW-1`, `GROUPED-COGROUP-1`, `GROUPED-DECL-transformWithState`,
`GROUPED-DECL-transformWithStateInPandas` filed DECLARED; `GROUPED-EXPRKEY-1` filed BACKLOG.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Rulings since step 1.** R-3 (orchestrator, 2026-09-15, binding): the contiguous-key
scan in `grouped_udf.py` must not call `as_py()` per row — run boundaries are found with
`pyarrow.compute` over adjacent key values (NULL equals NULL, NaN equals NaN, as
`_apply_in_pandas_keys_equal` defines, including multi-column keys and a group spanning
a batch edge), and `as_py()` runs only on boundary rows and the key handed to a keyed
callback. A key type with no `pyarrow.compute` `not_equal` kernel (nested types —
struct, list, map) falls back to the per-row scalar compare for that column only.
Critic round 1 (Grok logic, `/tmp/oc-worker/run15b/critic-grp-report.md`) filed L-001;
the Python perf review (`/tmp/oc-worker/run15b/perf-grp-report.md`) filed P2-1, which
R-3 resolves; P2-2, P2-3, P3-1, P3-2 and the critic's UNMEASURED table were recorded
not-to-fix by the orchestrator.

**Why now.** The 1.5 PySpark-parity campaign requires every public `GroupedData` name to answer
PySpark 4.1.2 or carry a dated declared refusal. `apply`, `applyInArrow`, `cogroup`, and the
three streaming-state names were absent entirely; the oracle run-15b recorded Spark's answers
for all 25 grouped cells.

**Not in this unit:** expression group keys (`GROUPED-EXPRKEY-1`, BACKLOG — the refusal
`applyInPandas` already carries is kept); the `pandas_udf` front-door `GROUPED_MAP`
construction refusal (existing M6 seed, `functions_udf.py` is fenced); keyed
`applyInPandas` `f(key, pdf)`; Rust execution — the bridge is a Python UDF boundary by
design (per-name reasons below).

## PROPOSITION LEDGER — GROUPED-SURFACE-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `GroupedData.apply` runs a GROUPED_MAP pandas UDF through `applyInPandas` behind Spark's deprecation warning, and refuses anything else with `INVALID_UDF_EVAL_TYPE` byte-exact. | `test_grouped_surface_1.py::test_apply_grouped_map_pandas_udf`, `::test_apply_rejects_non_grouped_map_udfs`, red on the base. | **PROVEN** | Red on base: `AttributeError: 'GroupedData' object has no attribute 'apply'` (13 of 15 red, the two base-green pins are the fixture-shape and C-008 cells). Marker detection follows Spark's `isinstance(Column) or not hasattr(udf, "func") or evalType != GROUPED_MAP`; repark's marker object is `PandasUDFFunction(func, schema, function_type=PandasUDFType.GROUPED_MAP)` — the `pandas_udf` decorator keeps its existing GROUPED_MAP construction refusal (fenced file), pinned in the same test. pins: grouped-surface-1/C-001 |
| C-002 | `applyInArrow` answers Spark for `func(table)`, `func(key, table)` (key = tuple of `pyarrow.Scalar`), the `Iterator[pa.RecordBatch]` hinted form, DDL and `StructType` schemas. | `test_apply_in_arrow_table_and_key_forms`, `::test_apply_in_arrow_iterator_form`. | **PROVEN** | Cells `applyInArrow`, `applyInArrow_key`, `applyInArrow_ddl_struct` reproduced row-for-row; the hinted iterator form (Spark's `SQL_GROUPED_MAP_ARROW_ITER_UDF`) yields RecordBatches per group through the same `mapInArrow` bridge. pins: grouped-surface-1/C-002 |
| C-003 | applyInArrow result validation raises Spark's inner error classes (`UDF_RETURN_TYPE`, `RESULT_COLUMN_NAMES_MISMATCH`, `RESULT_COLUMN_TYPES_MISMATCH`), `applyInArrow(5, …)` is `NOT_CALLABLE`, and expression group keys keep the `applyInPandas` refusal. | `test_apply_in_arrow_result_validation_errors`, `::test_apply_in_arrow_not_callable`, `::test_apply_in_arrow_expression_group_key_refusal`. | **PROVEN** | Five oracle error cells (`applyInArrow_bad_schema_result`, `missing_col`, `extra_col`, `returns_not_table`, `iter`) record `PythonException` wrappers; repark raises the inner class the card names (row `GROUPED-ARROW-1`). `verify_arrow_*` order copied from the installed PySpark 4.1.2 `worker.py`. Expression keys → `AnalysisException` (BACKLOG `GROUPED-EXPRKEY-1`). pins: grouped-surface-1/C-003 |
| C-004 | `cogroup` returns `PandasCogroupedOps` whose `applyInPandas`/`applyInArrow` answer Spark for two- and three-argument callbacks, including empty-side groups. | `test_cogroup_type_and_ungrouped_side`, `::test_cogroup_apply_in_pandas`, `::test_cogroup_apply_in_arrow`. | **PROVEN** | Cells `cogroup_type`, `cogroup_applyInPandas`, `cogroup_applyInPandas_key`, `cogroup_applyInArrow`, `cogroup_applyInArrow_key`, `cogroup_one_side_empty` reproduced. Merge-walk uses the engine's ascending order (nulls first, NaN last — measured on the engine 2026-09-14); one-side-missing groups get an empty frame/table of that side's schema, Spark's shape. pins: grouped-surface-1/C-004 |
| C-005 | Cogroup errors match Spark and memory stays bounded by the largest pair of groups. | `test_cogroup_key_count_mismatch`, `::test_cogroup_never_collects_a_whole_side`. | **PROVEN** | Key-count mismatch raises `IllegalArgumentException` `requirement failed: Cogroup keys must have same size: 1 != 2` byte-exact (cell `cogroup_key_count_mismatch`); `cogroup(DataFrame)` raises `NOT_EXPECTED_TYPE` where Spark accepts silently (R-1, row `GROUPED-COGROUP-1`). The 200k-row three-key cogroup runs with `DataFrame.collect`/`to_arrow` monkeypatched to refuse non-bridge frames — both sides stream through `__arrow_c_stream__`. pins: grouped-surface-1/C-005 |
| C-006 | `applyInPandasWithState` raises Spark's batch refusal byte-exact; `transformWithState`/`transformWithStateInPandas` raise `NOT_IMPLEMENTED` with the feature name. | `test_state_api_refusals`. | **PROVEN** | `applyInPandasWithState` raises `UnsupportedOperationException` `_LEGACY_ERROR_TEMP_3176` with the cell's exact message and empty params; the two transformWithState names raise `PySparkNotImplementedError` `NOT_IMPLEMENTED` `{"feature": …}` (R-2; Spark's batch answer is an internal `CANNOT_LOAD_STATE_STORE` — rows `GROUPED-DECL-*`). pins: grouped-surface-1/C-006 |
| C-007 | Registry rows, maps, and the unchanged fixture copy land in the same commit. | The gates; `test_fixture_covers_the_named_cells`. | **PROVEN** | Fixture copied byte-identical (sha256 `168c93ff…` both sides). Four DECLARED rows + one BACKLOG row in `docs/spark-sql-iceberg-parity.md`; dataframe map + tests map updated; `_dfcore_1_expected.py` gains `cogroup`/`grouped_arrow` package submodules; `joins_columns.py` baseline ratcheted 1238 → 1169 in `check_lib_py.py` and `test_cap_1_source_file_line_cap.py`. pins: grouped-surface-1/C-007 |
| C-008 | No regression: applyInPandas/mapInArrow suites, the export freeze, and the API inventory stay green. | The gate runs. | **PROVEN** | `test_applyinpandas.py` + `test_applyinpandas_oracle.py` + `test_mapinarrow.py` + `test_mapinarrow_oracle.py` + `test_t0_df_regions_import_freeze.py` + `test_dfcore_1_exports.py` all green on the final tree; `_apply_in_pandas_arrow_batches` moved with byte-identical behavior (the same runner, the same messages). pins: grouped-surface-1/C-008 |
| C-009 | Critic round 1: L-001 — a 0-column 0-row Arrow result (`pa.table({})`, `pa.RecordBatch.from_pydict({})` in the iterator form) contributes no rows for its group on the table, keyed-table, iterator, and cogrouped `applyInArrow` paths, matching Spark's `verify_arrow_result` empty-accept; typed-empty and `applyInPandas(pd.DataFrame())` stay green. Perf P2-1 under R-3 — the grouping scan finds run boundaries with `pyarrow.compute`, `as_py` runs once per contiguous run. | `test_apply_in_arrow_zero_column_result_drops_group`, `::test_group_scan_row_key_is_per_boundary_not_per_row`, red on the step-1 tree. | **PROVEN** | Red on the step-1 tree: L-001 `KeyError: 'Field "id" does not exist in schema'` (the post-verify `select`), and the boundary pin counted 1000 `as_py` row-key calls on a 20-batch 1-group stream. Fixed: the empty shape is skipped after verify on all four paths; the scan computes a per-column adjacent-diff mask (`is_null` diff, `not_equal` with fill_null, `is_nan` masking on floats — NULL==NULL and NaN==NaN byte-identical to the old comparator) merged across key columns, `indices_nonzero` gives run starts, `as_py` runs once per run. Synthetic 1e6-row scan (perf reviewer's harness): 4.28 s → 0.02 s at 10 groups, 5.28 s → 1.32 s at 1e5 groups; `row_key` 1,000,000 → 132 / 100,098. Fallback named: struct/list/map key columns (no `not_equal` kernel) take the per-row path for that column. pins: grouped-surface-1/C-009 |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

## Per-name decisions

| Name | Disposition | One line of reason |
|---|---|---|
| `GroupedData.apply` | implemented | Marker check + delegate to `applyInPandas`; pure API plumbing, Python is correct. |
| `GroupedData.applyInArrow` | implemented | Runs the user callable over Arrow groups the engine already streams; UDF boundary stays Python (per-card Rust-first note). |
| `GroupedData.cogroup` | implemented | Argument validation + `PandasCogroupedOps` construction. |
| `PandasCogroupedOps.applyInPandas` | implemented | Merge-walk of two sorted group streams; Python callables per group. |
| `PandasCogroupedOps.applyInArrow` | implemented | Same merge walk, Arrow tables. |
| `GroupedData.applyInPandasWithState` | declared | Spark's own batch refusal `_LEGACY_ERROR_TEMP_3176`; every repark frame is batch. |
| `GroupedData.transformWithState` | declared | Structured Streaming state stores are unreachable (R-2). |
| `GroupedData.transformWithStateInPandas` | declared | Same state-store dependency (R-2). |
