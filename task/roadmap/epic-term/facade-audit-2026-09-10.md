# Facade audit — Half A: the Rust-backed facade facts (FACADE-AUDIT-0 step 1)

**Opened:** 2026-09-10. **Class:** reading ledger. **Step:** 1 of Card FACADE-AUDIT-0 (Half A only).
**Base:** `2fad8135`. **Branch:** `docs/facade-audit-0`.
**Retires:** this document closes when the FACADE-AUDIT-0 step-3 PR merges; Half B lands as
step 2 in this same file. **Not in this step:** Half B weighing and sequence (step 2).

Half A measures the Python facade as it stands on the base commit: one row per module,
the IPC crossing sites, and every place a `Column` renders SQL text. All counts come
from the commands named in §1, run on this tree. No estimates.

A naming correction against the card: the native extension module is `repark._native`,
not `repark._repark`. No `repark._repark` symbol exists anywhere under
`python/repark/src/repark/` (measured: `grep -rn "repark\._repark"`, zero hits
outside `__repark_` view-name prefixes). The binding surface is three pyo3 classes —
`PyReparkSession`, `PyDataFrame`, `PyColumn` — plus two module functions
(`rows_from_record_batch`, `logical_column_names`) and the native exception classes,
all registered in `crates/repark-python/src/lib.rs`.

## §1 Method

Base commit: `git rev-parse --short HEAD` → `2fad8135`.

Module list and lines: `find python/repark/src/repark -name '*.py' | sort`
(106 files) with `wc -l` per file. Total 51,930 lines.

Binding calls: a call site counts when it textually invokes the native module or a
native handle. Five grep patterns, summed per file:

- P1 `_native\.[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)* *\(` (module calls)
- P2 `\._inner\.[A-Za-z_][A-Za-z0-9_]* *\(` (handle calls on `Column`/`DataFrame`/`ReparkSession`)
- P3 `_ensure_alive\(\)\.[A-Za-z_][A-Za-z0-9_]* *\(` (session accessor; returns the native handle)
- P4 `(^|[^_A-Za-z.])native\.[A-Za-z_][A-Za-z0-9_]* *\(` (locals bound from the accessor or a constructor)
- P5 `_session\.(sql|drop_temp_view|create_or_replace_temp_view|register_ipc_stream_as_temp_view|register_arrow_stream_as_temp_view|note_local_write_root|materialize_as_temp_view|materialize_as_cache_view|declare_temp_view_sorted) *\(` in `spark/dataframe/*.py` only, where `DataFrame._session` is the native `PyReparkSession` handle (`dataframe/core.py:718`)
- P6 `inner\.(resolve_temp_view_home_ref|sql|read_parquet|read_csv|read_json|read_postgres|read_excel|excel_sheet_names|read_iceberg_table) *\(` in `session/session_core.py` only, where `inner` is the accessor result (string-valued `inner` locals excluded)

Three dynamic-dispatch sites escape static patterns and are counted with a note:
`functions.py:888` (`getattr(argument._inner, method_name)()`), `column.py:314`
(`getattr(self._inner, op_method)(...)`), `session/create_dataframe_rows.py:849`
(`register_stream(view_name, table)` via a `getattr` alias). Facade-level calls such
as `session.sql(...)` on the `ReparkSession` facade are not binding calls; they reach
the engine through one P1–P6 site each inside `session_core.py`.

pyarrow references: `grep -cE 'pyarrow|pa\.'` per file (matching-line count). 23 modules
import pyarrow at runtime; four more mention it only in prose (`dataframe/export_errors.py`,
`session/create_dataframe_schema.py`, `session/create_dataframe_values.py`,
`session/session_core.py` — docstrings or comments, no import).

Class judgement from a short AST walk (`python3` with the `ast` module, run inline,
not committed): per module it reports function count, class count, binding-shaped
`Call` nodes, `pa` name/attribute uses, and string constants containing SELECT.
The walk informs but does not decide; the class is a judgement:

- `delegate`: each entry does argument checks then one engine call (re-export-only shims file here at zero cost)
- `logic`: the work happens in Python (SQL text, plan routing, option plumbing, inference rules, display)
- `pyarrow`: the module builds or transforms Arrow tables, batches, or schemas in Python

Cross-check: the walk reproduces the card's three anchor facts exactly —
`session/create_dataframe_inference.py` 101 pyarrow-reference lines, `spark/types.py` 43,
`dataframe/core.py` 37. A miscounted column would have failed this check.

## §2 Module table

Paths are relative to `python/repark/src/repark/`. `bind` is direct binding call
sites (P1–P6 plus the noted dynamic sites); `pa` is `grep -cE 'pyarrow|pa\.'` lines.

| path | lines | bind | pa | class |
|---|---|---|---|---|
| `config.py` | 253 | 0 | 0 | logic |
| `errors.py` | 301 | 0 | 0 | logic |
| `functions.py` | 6 | 0 | 0 | delegate |
| `__init__.py` | 114 | 1 | 0 | logic |
| `spark/catalog.py` | 651 | 1 | 0 | logic |
| `spark/column.py` | 1589 | 24 | 0 | delegate |
| `spark/_csv_smart.py` | 899 | 0 | 0 | logic |
| `spark/dataframe/actions_export.py` | 290 | 3 | 0 | logic |
| `spark/dataframe/core.py` | 4487 | 68 | 37 | logic |
| `spark/dataframe/display.py` | 351 | 0 | 3 | logic |
| `spark/dataframe/eager.py` | 92 | 1 | 0 | logic |
| `spark/dataframe/explain.py` | 32 | 0 | 0 | logic |
| `spark/dataframe/export_errors.py` | 87 | 0 | 2 | logic |
| `spark/dataframe/grouped_udf.py` | 145 | 0 | 7 | pyarrow |
| `spark/dataframe/__init__.py` | 29 | 0 | 0 | delegate |
| `spark/dataframe/joins_columns.py` | 1238 | 15 | 11 | logic |
| `spark/dataframe/plan_collapse.py` | 1057 | 3 | 11 | logic |
| `spark/dataframe/polars_cells.py` | 324 | 0 | 6 | logic |
| `spark/dataframe/rows_export.py` | 241 | 1 | 31 | pyarrow |
| `spark/dataframe/sampling.py` | 288 | 11 | 0 | logic |
| `spark/dataframe/statistics.py` | 264 | 3 | 0 | logic |
| `spark/dataframe/udf_bridge.py` | 471 | 0 | 18 | pyarrow |
| `spark/dataframe/udf_projection.py` | 349 | 0 | 8 | pyarrow |
| `spark/dataframe/udf_schema.py` | 64 | 0 | 8 | pyarrow |
| `spark/dataframe/udf_window_projection.py` | 337 | 0 | 0 | logic |
| `spark/dataframe/writer_readwriter.py` | 1111 | 2 | 2 | logic |
| `spark/functions_agg.py` | 60 | 0 | 0 | logic |
| `spark/functions_bitwise.py` | 217 | 0 | 0 | logic |
| `spark/functions_collections.py` | 384 | 2 | 0 | delegate |
| `spark/functions_datetime.py` | 282 | 0 | 0 | logic |
| `spark/functions_declared.py` | 213 | 0 | 0 | logic |
| `spark/functions_expr.py` | 2255 | 16 | 0 | delegate |
| `spark/functions_json.py` | 91 | 0 | 0 | logic |
| `spark/functions_lambda.py` | 281 | 2 | 0 | delegate |
| `spark/functions_math.py` | 147 | 0 | 0 | logic |
| `spark/functions.py` | 1985 | 36 | 0 | delegate |
| `spark/functions_session.py` | 133 | 1 | 0 | delegate |
| `spark/functions_try.py` | 138 | 2 | 0 | delegate |
| `spark/functions_udf.py` | 1300 | 0 | 9 | logic |
| `spark/functions_url.py` | 174 | 0 | 0 | logic |
| `spark/functions_window.py` | 92 | 5 | 0 | delegate |
| `spark/_idents.py` | 185 | 0 | 0 | logic |
| `spark/__init__.py` | 79 | 0 | 0 | delegate |
| `spark/_integral.py` | 129 | 0 | 0 | logic |
| `spark/merge.py` | 342 | 0 | 0 | logic |
| `spark/ml/base.py` | 157 | 0 | 0 | logic |
| `spark/ml/classification.py` | 260 | 1 | 0 | logic |
| `spark/ml/clustering.py` | 252 | 1 | 0 | logic |
| `spark/ml/evaluation.py` | 620 | 0 | 0 | logic |
| `spark/ml/ext/_arrow_util.py` | 323 | 0 | 3 | pyarrow |
| `spark/ml/ext/_deps.py` | 68 | 0 | 0 | logic |
| `spark/ml/ext/__init__.py` | 63 | 0 | 0 | delegate |
| `spark/ml/ext/_lightgbm.py` | 565 | 0 | 0 | logic |
| `spark/ml/ext/_persist.py` | 341 | 0 | 4 | logic |
| `spark/ml/ext/_sklearn.py` | 310 | 0 | 0 | logic |
| `spark/ml/ext/_xgboost.py` | 636 | 0 | 0 | logic |
| `spark/ml/feature/__init__.py` | 76 | 0 | 0 | delegate |
| `spark/ml/feature/_transformers.py` | 2717 | 0 | 0 | logic |
| `spark/ml/__init__.py` | 53 | 0 | 0 | delegate |
| `spark/ml/linalg.py` | 280 | 0 | 0 | logic |
| `spark/ml/param.py` | 485 | 0 | 0 | logic |
| `spark/ml/pipeline.py` | 759 | 0 | 5 | logic |
| `spark/ml/regression.py` | 342 | 1 | 0 | logic |
| `spark/ml/tuning.py` | 449 | 0 | 0 | logic |
| `spark/ml/util.py` | 95 | 0 | 0 | logic |
| `spark/polars.py` | 549 | 0 | 0 | logic |
| `spark/row.py` | 257 | 0 | 0 | logic |
| `spark/_secrets.py` | 46 | 0 | 0 | logic |
| `spark/session/builder_conf.py` | 362 | 0 | 0 | logic |
| `spark/session/catalog.py` | 1 | 0 | 0 | delegate |
| `spark/session/catalog_resolution.py` | 294 | 0 | 0 | logic |
| `spark/session/_coerce.py` | 26 | 0 | 0 | logic |
| `spark/session/create_dataframe_arrow.py` | 407 | 0 | 34 | pyarrow |
| `spark/session/create_dataframe_columns.py` | 259 | 0 | 12 | pyarrow |
| `spark/session/create_dataframe_inference.py` | 737 | 0 | 101 | pyarrow |
| `spark/session/create_dataframe.py` | 8 | 0 | 0 | delegate |
| `spark/session/create_dataframe_rows.py` | 888 | 6 | 9 | pyarrow |
| `spark/session/create_dataframe_schema.py` | 692 | 0 | 5 | logic |
| `spark/session/create_dataframe_tuples.py` | 628 | 0 | 28 | pyarrow |
| `spark/session/create_dataframe_values.py` | 569 | 0 | 2 | logic |
| `spark/session/_funcs.py` | 484 | 0 | 0 | delegate |
| `spark/session/__init__.py` | 102 | 0 | 0 | logic |
| `spark/session/reader.py` | 1022 | 0 | 0 | logic |
| `spark/session/reader_support.py` | 477 | 0 | 0 | logic |
| `spark/session/session_configuration.py` | 540 | 1 | 0 | logic |
| `spark/session/session_core.py` | 2305 | 22 | 1 | delegate |
| `spark/session/session_state.py` | 198 | 0 | 0 | logic |
| `spark/session/session_time_zone.py` | 161 | 0 | 0 | logic |
| `spark/session/sql_relations.py` | 774 | 0 | 0 | logic |
| `spark/session/sql_udf_discovery.py` | 248 | 0 | 0 | logic |
| `spark/session/sql_udf_materialization.py` | 306 | 0 | 0 | logic |
| `spark/session/sql_udf_parsing.py` | 491 | 0 | 0 | logic |
| `spark/session/sql_udf.py` | 151 | 0 | 0 | logic |
| `spark/session/sql_udf_residual.py` | 580 | 0 | 0 | logic |
| `spark/session/sql_udf_rewrite.py` | 857 | 0 | 0 | logic |
| `spark/session/timestamp_type.py` | 97 | 0 | 5 | logic |
| `spark/sql/functions.py` | 29 | 0 | 0 | delegate |
| `spark/sql/__init__.py` | 77 | 0 | 0 | delegate |
| `spark/sql/types.py` | 27 | 0 | 0 | delegate |
| `spark/sql/window.py` | 24 | 0 | 0 | delegate |
| `spark/storage.py` | 80 | 0 | 0 | logic |
| `spark/ta.py` | 1818 | 1 | 0 | logic |
| `spark/_temp_views.py` | 78 | 0 | 0 | logic |
| `spark/types.py` | 1834 | 0 | 43 | pyarrow |
| `spark/udtf.py` | 695 | 0 | 4 | logic |
| `spark/window.py` | 344 | 0 | 0 | logic |

Binding symbols used, for every module with a nonzero `bind` count (re-export-only
delegate modules use none). `Column._inner` is the native `PyColumn`;
`DataFrame._inner` the native `PyDataFrame`; `DataFrame._session` the native session.

- `__init__.py` (1): `PyReparkSession.native` (ANSI probe).
- `spark/catalog.py` (1): `_ensure_alive().drop_temp_view`; its four `session.sql`
  calls go through the facade, not the binding.
- `spark/column.py` (24): `PyColumn.call_scalar` (8), `PyColumn.case_when`,
  `Column._inner` operator/aggregate/date methods (`alias`, `cast`, `try_cast`,
  `over`, `not_`, `ne`, `sub`, `is_null`, `is_not_null`, `display_name`,
  `contains_higher_order`) plus one dynamic `getattr(self._inner, op_method)` site.
- `spark/dataframe/actions_export.py` (3): `_inner.logical_schema_fields` (2), `_inner.alias`.
- `spark/dataframe/core.py` (68): `session.sql` (16), `session.drop_temp_view` (18),
  `session.create_or_replace_temp_view` (13), `session.register_ipc_stream_as_temp_view` (2),
  `session.register_arrow_stream_as_temp_view`, `session.materialize_as_temp_view`,
  `session.materialize_as_cache_view`, `session.declare_temp_view_sorted`,
  `PyColumn.column` (3), `_inner.logical_schema_fields` (3), `_inner.alias` (3),
  `_native.logical_column_names`, `_inner.display_name`, `_inner.analyzed_arrow_schema`.
- `spark/dataframe/eager.py` (1): `session.sql`.
- `spark/dataframe/joins_columns.py` (15): `_inner.alias` (4),
  `_inner.logical_schema_fields` (2), `_inner.display_name` (2), `_inner.aggregate` (2),
  `session.sql`, `session.drop_temp_view`, `session.create_or_replace_temp_view`,
  `PyColumn.literal`, `PyColumn.count_aggregate`.
- `spark/dataframe/plan_collapse.py` (3): `_inner.display_name` (2),
  `_inner.collapse_identity_aliases`.
- `spark/dataframe/rows_export.py` (1): `_native.rows_from_record_batch`.
- `spark/dataframe/sampling.py` (11): `session.sql` (5), `session.drop_temp_view` (3),
  `session.create_or_replace_temp_view` (3).
- `spark/dataframe/statistics.py` (3): `session.sql`, `session.drop_temp_view`,
  `session.create_or_replace_temp_view`.
- `spark/dataframe/writer_readwriter.py` (2): `session.sql`, `session.note_local_write_root`.
- `spark/functions_collections.py` (2): `PyColumn.make_struct`, `column._inner.alias`.
- `spark/functions_expr.py` (16): `_inner.aggregate` (10), `PyColumn.sql` (`pi()`),
  `PyColumn.make_struct`, `_inner.approx_percentile_list`,
  `_inner.approx_percentile_cont`, `_inner.alias`, `_inner.aggregate_binary`.
- `spark/functions_lambda.py` (2): `PyColumn.lambda_variable`, `PyColumn.call_higher_order`.
- `spark/functions.py` (36): `PyColumn.sql` (5: temporal `lit` arms, `cast`, one more),
  `PyColumn.literal` (4), `PyColumn.count_aggregate` (3), `PyColumn.column` (2),
  `PyColumn.call_scalar`, `PyColumn.coalesce`, `PyColumn.concat`,
  `PyColumn.current_timestamp`, `PyColumn.row_number`, `PyColumn.rank`,
  `PyColumn.dense_rank`, `PyColumn.ntile`, `_inner.aggregate` (8), `_inner.trunc`,
  plus one dynamic `getattr(argument._inner, method_name)` site.
- `spark/functions_session.py` (1): `PyColumn.sql` (`uuid()`).
- `spark/functions_try.py` (2): `_inner.aggregate` (`try_sum`, `try_avg`).
- `spark/functions_window.py` (5): `PyColumn.lag`, `PyColumn.lead`,
  `PyColumn.nth_value`, `PyColumn.percent_rank`, `PyColumn.cume_dist`.
- `spark/ta.py` (1): `PyColumn.ta_window`.
- `spark/ml/classification.py` (1): `_native.fit_logistic_regression`.
- `spark/ml/clustering.py` (1): `_native.fit_kmeans`.
- `spark/ml/regression.py` (1): `_native.fit_linear_regression`.
- `spark/session/create_dataframe_rows.py` (6): `native.materialize_as_temp_view`,
  `native.register_ipc_stream_as_temp_view`, `native.drop_temp_view` (2),
  `_ensure_alive().drop_temp_view`, plus one `getattr`-alias `register_stream` call.
- `spark/session/session_configuration.py` (1): `PyReparkSession.config_file_pairs`.
- `spark/session/session_core.py` (22): `PyReparkSession(...)` constructor,
  ten `_ensure_alive().<method>` sites (`register_memory_catalog`,
  `refresh_catalog_provider`, `create_namespace`, `list_iceberg_table_names`,
  `list_temp_view_names`, `list_df_schema_table_names`, `testing_*` (4)),
  nine `inner.<method>` sites (`sql` (2), `read_parquet`, `read_csv`, `read_json`,
  `read_postgres`, `read_excel`, `excel_sheet_names`, `read_iceberg_table`,
  `resolve_temp_view_home_ref`), `_inner.register_late_catalogs`.

Modules that reach the engine only through the facade (`session.sql` and friends on
`ReparkSession`, each delegating to one site above): `spark/catalog.py`,
`spark/merge.py`, `spark/polars.py`, `spark/_csv_smart.py`,
`dataframe/udf_window_projection.py`, `dataframe/writer_readwriter.py`,
`session/sql_relations.py`, `session/reader.py`, `session/create_dataframe_rows.py`
(the `session.sql` scans), `ml/tuning.py`, `ml/evaluation.py`, `ml/base.py`,
`ml/ext/_arrow_util.py`, `ml/feature/_transformers.py`.
