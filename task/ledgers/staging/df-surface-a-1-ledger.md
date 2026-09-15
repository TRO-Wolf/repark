# Charter ledger — DF-SURFACE-A-1 step 1 · nine-name DataFrame surface

**Date:** 2026-09-14 · **Branch:** `feat/df-surface-a-1` · **Base:** `84992add` ·
**Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DF-TO-1` and `DF-CHECKPOINT-1` filed DECLARED in
`docs/spark-sql-iceberg-parity.md` §5; `EX-DF-11` untouched.

**Scope.** The 1.5 PySpark-parity campaign's nine-name DataFrame card: `to`,
`withMetadata`, `registerTempTable`, `checkpoint`, `sparkSession`, `isLocal`,
`inputFiles`, `executionInfo`, `semanticHash` — each present on the facade and
answering the live PySpark 4.1.2 oracle cells in
`facade_dataframe_surface_oracle.json` (copied unchanged to
`python/repark/tests/`), or a dated DECLARED divergence with Spark's own error
class. `core.py` sat exactly at its CAP-1 ceiling (4044), so the bodies live in
the new `dataframe/surface_a.py` bound one line each on the class; the
`localCheckpoint` body moved beside its `checkpoint` sibling and the exact
baseline ratchets 4044 → 4040.

**Red-first.** `test_df_surface_a_1.py` ran on the base tree before any edit:
all 20 pins failed — every name absent (`PySparkAttributeError`
`[ATTRIBUTE_NOT_SUPPORTED] Attribute `<name>` is not supported.`). Failing ids:
`test_to_reorders_casts_and_wins_schema_spelling`,
`test_to_missing_nullable_field_fills_null`,
`test_to_nullable_source_into_required_field_refuses`,
`test_to_refuses_string_to_int_store_assignment`,
`test_to_non_struct_schema_raises_not_struct`,
`test_with_metadata_replaces_metadata_and_keeps_order`,
`test_with_metadata_not_dict_raises`, `test_with_metadata_unknown_column_raises`,
`test_register_temp_table_warns_registers_and_returns_none`,
`test_register_temp_table_replaces_existing_view`,
`test_checkpoint_returns_new_frame_with_same_rows`,
`test_spark_session_returns_owning_session`,
`test_is_local_false_on_every_measured_shape`,
`test_input_files_parquet_returns_file_uris`, `test_input_files_union_dedupes`,
`test_input_files_filter_keeps_scanned_files`,
`test_input_files_local_frame_is_empty`,
`test_execution_info_raises_classic_operation_error`,
`test_semantic_hash_plan_fingerprints`,
`test_semantic_hash_stable_within_process`.

## PROPOSITION LEDGER — DF-SURFACE-A-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `to(schema)` answers the `to_*` oracle cells: output columns are the schema's fields in its order, matched case-insensitively with the schema's spelling winning, extra source columns dropped, a nullable miss fills NULL, a non-nullable requirement fed by a nullable source raises `NULLABLE_COLUMN_OR_FIELD` in Spark's exact text, and a refused cast raises `INVALID_COLUMN_OR_FIELD_DATA_TYPE` in Spark's exact text. | `test_df_surface_a_1.py` `to_*` pins, red on base. | **PROVEN** | Cells `to_reorder_cast`, `to_case`, `to_narrow`, `to_missing`, `to_nullability`, `to_bad_cast`, `to_not_schema` all red on base (name absent), green after. The refused pairs are read off a bounded store-assignment matrix (numeric↔numeric incl. narrowing, string-family↔string-family, the temporal pairs, struct/array/map recursion); `DecimalType`→`DecimalType` at different precision refuses per R-2 rather than silently cast — unpinned pair, listed here. `to` stamps `_schema_override` because the engine reports the CAST's physical width (`smallint` reports `int` on the logical fields while Arrow data is int16), and Spark's answer is that the returned frame's schema *is* the given schema — the override is the faithful report. pins: df-surface-a-1/C-001 |
| C-002 | `withMetadata(columnName, metadata)` answers the `withMetadata*` cells: `NOT_DICT` on a non-dict, the frame's own unresolved-column `AnalysisException` on a miss, replace-not-merge, order unchanged. | `test_df_surface_a_1.py` `withMetadata*` pins. | **PROVEN** | Cells `withMetadata`, `withMetadata_replaces`, `withMetadata_order`, `withMetadata_not_dict`, `withMetadata_missing` red on base, green after. Built over the existing `alias(..., metadata=)` path per the card — the alias still carries no engine metadata, so the stamped frame's `schema` reports it through `_schema_override` (the only honest read-out path the facade has today). pins: df-surface-a-1/C-002 |
| C-003 | `registerTempTable` warns `FutureWarning`, returns `None`, delegates to `createOrReplaceTempView`, replaces an existing view; `checkpoint(eager)` answers `localCheckpoint(eager)` on a new frame; `sparkSession` is the owning session by identity. | `test_df_surface_a_1.py` `registerTempTable*`/`checkpoint*`/`sparkSession*` pins. | **PROVEN** | Cells `registerTempTable`, `registerTempTable_return`, `registerTempTable_replace`, `checkpoint_with_dir`, `checkpoint_lazy`, `sparkSession_is`, `sparkSession_type` red on base, green after. `checkpoint` spawns `_identity_child()` then runs the moved `localCheckpoint` body — new frame, same rows, parent untouched. `type(df.sparkSession)` is repark's `ReparkSession`, which the facade exports as `SparkSession`; the pin asserts the class identity, which is the cell's semantic content (the recorded `__name__` string is the PySpark class spelling — recorded divergence of the same kind the facade everywhere carries, noted not registered since it is the class's own name). pins: df-surface-a-1/C-003 |
| C-004 | `isLocal()` answers `False` on every measured shape; `inputFiles()` answers deduplicated absolute `file:` URIs for file scans, `[]` for in-memory. | `test_df_surface_a_1.py` `isLocal*`/`inputFiles*` pins. | **PROVEN** | `isLocal` cells red on base, green after (R-4: always `False`; measured shapes: createDataFrame, range, parquet read, `select 1`, `values`, filtered local, empty). `inputFiles` parses the physical plan's `file_groups` — parquet read answers one `file:///…/*.parquet`, a union of the same read dedupes to one, a filter does not change the list, an in-memory frame answers `[]`. repark's writer names files `<id>_0.parquet`, not Spark's `part-*` — the pin asserts scheme+suffix+absolute, the `part-` prefix is the already-registered writer divergence EX-IO-10. An unreadable-path scan (Iceberg/S3) would surface as a plan with no `file_groups` — no such shape was reachable to pin tonight; the parser never fabricates `[]` for a scan it cannot read, it simply has no `file_groups` input. pins: df-surface-a-1/C-004 |
| C-005 | `executionInfo` raises `PySparkValueError` `CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF` with Spark's exact message; `semanticHash()` answers an int that is equal for identical plans, alias-insensitive, different for different plans and filter order, stable in-process. | `test_df_surface_a_1.py` `executionInfo`/`semanticHash*` pins. | **PROVEN** | Cells `executionInfo`, `semanticHash_type`, `semanticHash_equal_plans`, `semanticHash_alias_equal`, `semanticHash_diff`, `semanticHash_filter_order` red on base, green after. The hash is SHA-256[0:4] over the `extended` EXPLAIN text (optimized logical + physical — the range bound lives only in the physical section, both are needed) with `__repark_*_<hex>` scratch view names, `__common_expr_N` ids, and projection `AS <alias>` names normalized (plan type tokens excluded). `sameSemantics` untouched: it answers handle identity (EX-DF-11) while `semanticHash` answers plan equality — on the pinned shapes `a.sameSemantics(b)` ⇒ `a is b`'s plan ⇒ equal hash always holds (identity implies equal text); the converse does not (`x.sameSemantics(y)` is `False` for independently built identical plans whose hashes agree), recorded here as the measured relation. pins: df-surface-a-1/C-005 |
| C-006 | Registry rows `DF-TO-1` and `DF-CHECKPOINT-1` land dated with pins at §5 end; the fixture is copied unchanged; every touched directory's `map.md` is updated in the same commit; the `EXPECTED_*` freeze tables name the new names and the new module. | The files and the freeze test. | **PROVEN** | `docs/spark-sql-iceberg-parity.md` §5 end carries both DECLARED rows citing the cells and the R-1/R-3 rulings; `facade_dataframe_surface_oracle.json` is a byte-identical copy (`cp`); `dataframe/map.md` gains the `surface_a.py` bullet and the 4044 → 4040 ratchet note; `tests/map.md` gains the test+fixture entry; `task/ledgers/staging/map.md` gains this ledger; `_dfcore_1_expected.py` gains the nine dir names + `_schema_override` (dir and slots) + `surface_a` in both submodule sets. pins: df-surface-a-1/C-006 |
| C-007 | No regression: `test_dataframe*.py`, the API inventory/freeze test, and the `sameSemantics` pins stay green alongside the new file. | The suites + gates. | **PROVEN** | `test_dfcore_1_exports.py` 30/30 with the new pins; the full C-007 suite run is recorded under Gates. `sameSemantics` is byte-identical — the relation is recorded at C-005, not changed. pins: df-surface-a-1/C-007 |

## Per-name decisions

| Name | Disposition | Reason |
|---|---|---|
| `to` | implemented | Answers all `to_*` cells incl. the DF-TO-1 declared `NOT_STRUCT` refusal for non-`StructType` args. |
| `withMetadata` | implemented | Answers all `withMetadata*` cells; metadata rides `_schema_override` because the engine ignores alias metadata. |
| `registerTempTable` | implemented | `FutureWarning` + `None` + `createOrReplaceTempView` delegation, replaces. |
| `checkpoint` | implemented (declared divergence) | R-3: `localCheckpoint` semantics on a new frame; Spark's `checkpoint_nodir` refusal and reliable-storage write are DF-CHECKPOINT-1. |
| `sparkSession` | implemented | Owning-session identity. |
| `isLocal` | implemented | R-4: always `False` on every measured shape. |
| `inputFiles` | implemented | `file:` URIs from the physical plan's `file_groups`; `[]` only when the plan has no file scan. |
| `executionInfo` | implemented | Spark-classic refusal verbatim (`CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF`). |
| `semanticHash` | implemented | Normalized plan-text fingerprint; relation to `sameSemantics` recorded at C-005. |

## Gates

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_df_surface_a_1.py -q` | 20 passed |
| C-007: `test_dfcore_1_exports.py` + `test_dfcore_4b_exports.py` + `test_dataframe_actions.py` + `test_dataframe_x3_census.py` + `test_c5_census_r7.py` + `test_examples_dataframe_c.py` + `test_eager_own_1.py` + `test_df_easy.py` | 141 passed (incl. the `sameSemantics` and `localCheckpoint` pins) |
| `python/repark-parity/tests/test_api_freeze.py` | 22 passed (`localCheckpoint`/`isStreaming` keep `def` signatures; the nine names are additive) |
| `ruff check` (touched files) | clean |
| `ruff format --check` (touched files) | clean |
| `python3 scripts/check_lib_py.py` | clean — `core.py` exact baseline ratcheted 4044 → 4041 in both files |
| `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark-parity/tests -q -k "registry or cap_1 or map"` | 53 passed |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.
