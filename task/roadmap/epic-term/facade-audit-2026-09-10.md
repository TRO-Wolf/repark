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
- `spark/functions.py` (36): `PyColumn.sql` (5: three temporal `lit` arms, `cast`, `expr`),
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

## §3 IPC crossing sites

Arrow crosses the pyo3 boundary as IPC bytes at the `register_ipc_stream_as_temp_view`
sites and as zero-copy C Stream capsules at the `register_arrow_stream_as_temp_view`
sites; rows come back through `rows_from_record_batch` and the C Stream export.
Every site below is measured with
`grep -rn '__arrow_c_stream__|register_ipc_stream_as_temp_view|register_arrow_stream_as_temp_view|rows_from_record_batch|RecordBatchReader|pa_ipc\.|to_batches|from_batches|ipc_bytes'`.
Comment-only mentions are marked and do not cross.

Into the engine (Python builds Arrow, engine imports it):

- `dataframe/core.py:792` — `register_arrow_stream_as_temp_view(view_name, stream_obj)`;
  the mapInArrow C Stream seam (`_register_arrow_stream_as_inner`).
- `dataframe/core.py:811` — `register_ipc_stream_as_temp_view(view_name, ipc_bytes)`;
  the IPC fallback (`_register_ipc_bytes_as_inner`).
- `dataframe/core.py:851` — `register_ipc_stream_as_temp_view(view_name, sink.getvalue())`;
  the mapInPandas IPC path.
- `dataframe/core.py:756,768,776` — `pa_ipc.new_stream` encode of the mapInArrow IPC fallback.
- `dataframe/core.py:843,846` — `pa_ipc.new_stream` encode of the mapInPandas path.
- `dataframe/core.py:626` — `pa.RecordBatchReader.from_stream(parent_for_stream)`;
  the parent frame is consumed through its own `__arrow_c_stream__` export.
- `dataframe/core.py:705,706,742` — `pa.Table.from_batches` assembly of UDF output batches.
- `session/create_dataframe_rows.py:842,849` — `register_arrow_stream_as_temp_view`
  probe and `register_stream(view_name, table)` call; the preferred `createDataFrame` seam.
- `session/create_dataframe_rows.py:856,860,861,864` — `pa_ipc.new_stream` encode
  with `table.to_batches()` and `register_ipc_stream_as_temp_view`; the version-skew fallback.
- `dataframe/joins_columns.py:133,134,139,159,164,167,168` — `pa.array` /
  `pa.RecordBatch.from_arrays` / `pa.Table.from_pandas` / `to_batches`; pandas-UDF
  result assembly re-entering through the session.
- `dataframe/grouped_udf.py:59,61` — `pa.Table.from_batches` (+ `concat_tables`
  promote) assembly of group segments.
- `dataframe/udf_bridge.py:39` — `table.to_batches()` of UDF output.
- `ml/ext/_arrow_util.py:280,298,299,305` — `pa_ipc.new_stream` encode with
  `to_batches()` and `register_ipc_stream_as_temp_view`; prediction re-entry.

Out of the engine (engine produces Arrow, Python consumes it):

- `dataframe/core.py:4047,4054` — `DataFrame.__arrow_c_stream__` delegates to
  `self._action_inner().__arrow_c_stream__`; the export door FACADE-1 standardizes.
- `dataframe/core.py:4298` — `pa.RecordBatchReader.from_stream(self)` in `to_arrow`;
  `to_arrow_batches` (4288), `to_polars` (4319), and `to_pandas` (4342) ride the same export.
- `dataframe/core.py:4094,4122` — `collect` via `table.to_pylist()`.
- `dataframe/rows_export.py:235` — `_native.rows_from_record_batch(table, supplied)`;
  the PERF-FACADE-1 native row path.
- `dataframe/rows_export.py:179,200,203` — `table.column(index).to_pylist()` cell reads.
- `dataframe/rows_export.py:223` — `__arrow_c_array__` capability probe.
- `dataframe/core.py:2436,2448` — `_inner.analyzed_arrow_schema()`; an Arrow C
  *schema* capsule (not IPC bytes) used for plan column names.

Comment-only (no crossing): `core.py:601,712,715,728,731,733,739,740,753`,
`create_dataframe_rows.py:813,815,817,852`, `grouped_udf.py:51`.

## §4 Where a `Column` renders SQL text

A `Column` holds a native `PyColumn` plus Python-kept `spark_display`,
`projection_name`, `sql_expr`, and `join_sql_expr` strings; the engine re-parses the
rendered text at plan time. Three groups of construction sites.

Group 1 — `_native.PyColumn.sql(...)` string entry points (the text goes straight
to the engine parser):

- `functions.py:62,73,87` — `lit()` temporal arms render `TIMESTAMP '...'`,
  `DATE '...'`, `TIME '...'` SQL.
- `functions.py:280` — `cast()` renders the `CAST(<expr> AS <type>)` string.
- `functions.py:358` — `expr()` passes the caller-supplied SQL string straight through.
- `functions_expr.py:1812` — `pi()` renders `pi()`.
- `functions_session.py:127` — `uuid()` renders `uuid()`.

Group 2 — `Column`-internal `sql_expr` / `join_sql_expr` assembly (`column.py`;
68 `sql_expr`-family lines). Representative sites; every operator method follows
the same shape (native call plus display/SQL bookkeeping):

- `column.py:304,327,328` — `_binary` builds both SQL parts for binary operators.
- `column.py:386,406` — `__neg__`; `column.py:426,443,444` — `__ne__`.
- `column.py:519,534,535` — `eqNullSafe` renders `<l> <=> <r>`.
- `column.py:548,595` — `substr` renders `substr(...)`.
- `column.py:625,646` — `_string_predicate`; `column.py:667,681` — `_bitwise`.
- `column.py:690,699,700` — `__invert__`; `column.py:732,741,742` — `is_null`;
  `column.py:753,762,763` — `is_not_null`.
- `column.py:775,830,831` — `_from_when_pairs` renders `CASE WHEN` chains.
- `column.py:894,932,945,953,967` — `alias` carries the SQL parts through renames.
- `column.py:983,1044,1066,1085,1104` — `__getitem__` renders
  `(<expr>)[<key>]` and `get_json_object`-style access.
- `column.py:1161,1193,1213,1214` — `cast` renders `CAST`; `column.py:1225,1249,1250` —
  `try_cast` renders `TRY_CAST`.
- `column.py:1344,1367,1373` — `_with_sort_order` carries SQL parts into sort keys.

Group 3 — session-level SQL scaffolds (query text built in Python, run through
`session.sql`, re-parsed by the engine). One line per site with its query shape:

- `dataframe/core.py:493,503` — `SELECT * FROM <cache view>` (cache materialize).
- `dataframe/core.py:795,815,853` — `SELECT * FROM <registered view>` (arrow/IPC seams).
- `dataframe/core.py:1180` — `SELECT * FROM <view>` (`declare_sorted`).
- `dataframe/core.py:2097` — `SELECT <qcol projections> FROM <view>` (`_select_via_qcol_sql`).
- `dataframe/core.py:2619` — `SELECT <projection> FROM <view>` (`selectExpr`).
- `dataframe/core.py:2646` — `SELECT * FROM <home_ref>` (`alias`).
- `dataframe/core.py:3364` — `<sql> SELECT * FROM <view>` (`_explain_text`).
- `dataframe/core.py:3523` — `SELECT * FROM <left> <op> SELECT * FROM <right>` (set ops).
- `dataframe/core.py:3590` — `SELECT * FROM <left> CROSS JOIN <right>`.
- `dataframe/sampling.py:110,112,117,178,188` — `SELECT * FROM <view> [WHERE ...]`
  (sample fractions, strata splits).
- `dataframe/statistics.py:78` — the summary query (`_compute_summary`).
- `dataframe/joins_columns.py:445,446,471,584` — `SELECT * FROM <udf/builtin/out view>`
  (pandas-UDF agg cleanups) and the grouped-agg select.
- `dataframe/eager.py:82` — `SELECT * FROM <cache view>` (lazy reroute).
- `dataframe/udf_window_projection.py:180,181,219` — `SELECT * FROM <left/agg/out view>`.
- `dataframe/writer_readwriter.py:724,1007` — `SELECT * FROM <table> LIMIT 0`
  (by-name column probe); `:836` — `SELECT * FROM <table>.partitions LIMIT 0`.
- `session/session_core.py:1018` — `SELECT * FROM <table_ref>` (`table()`).
- `session/session_core.py:494` — `MERGE INTO <target> USING <source> ON ...` rewrite.
- `session/create_dataframe_rows.py:783,868` — `SELECT * FROM <cdf view>` (VALUES/arrow scans).
- `session/create_dataframe_values.py:28+` — `_sql_literal` renders every VALUES cell literal.
- `spark/merge.py:167,196` — `MERGE INTO ... USING ... ON ...` with `THEN INSERT ... VALUES`.
- `spark/catalog.py:339,437,637,645` — `SHOW NAMESPACES IN ...`, `DESCRIBE NAMESPACE ...`.
- `spark/_csv_smart.py:836` — `SELECT 1 AS _repark_smart_empty WHERE 1 = 0` (empty probe).
- `ml/tuning.py:232` — `SELECT * FROM <fold view>`; `ml/evaluation.py:60` —
  `SELECT COUNT(*) AS n FROM <view>`; `ml/ext/_arrow_util.py:306` —
  `SELECT * FROM <prediction view>`.
- `session/sql_udf_rewrite.py` — SQL-UDF body rewrites (18 SELECT-bearing lines);
  `udf_window_projection`, `ta.py`, and `ml/feature/_transformers.py` (64
  SELECT-bearing lines) likewise generate query text in Python (D-3 out of scope
  for the sequence, in scope for the count).

## §5 Totals

Measured with `find python/repark/src/repark -name '*.py' | sort | xargs wc -l`
(51,930 total) and the §1 patterns summed per class:

| class | files | lines | share (rounded) |
|---|---|---|---|
| delegate | 22 | 10,118 | 19% |
| logic | 72 | 35,466 | 68% |
| pyarrow | 12 | 6,346 | 12% |
| **facade total** | **106** | **51,930** | 100% |

The 22 delegate files are the 13 re-export-only shims (956 lines combined),
`column.py`, the seven `functions*` builder modules, and `session_core.py`.
The 12 pyarrow files are `dataframe/rows_export.py`, `udf_bridge.py`,
`udf_projection.py`, `udf_schema.py`, `grouped_udf.py`, `spark/types.py`, five
`session/create_dataframe_*` modules (`arrow`, `columns`, `inference`, `tuples`,
`rows`), and `ml/ext/_arrow_util.py`. Direct binding call sites total 231
(§2 per-file counts). pyarrow-touching modules: 23 import pyarrow at runtime;
27 have at least one `pyarrow|pa\.` line (the four extra are prose-only —
`dataframe/export_errors.py`, `session/create_dataframe_schema.py`,
`session/create_dataframe_values.py`, `session/session_core.py`).

Largest logic masses outside the pyarrow set: `dataframe/core.py` (4,487),
`ml/feature/_transformers.py` (2,717, D-3 out of scope), `session_core.py` is
delegate, `ta.py` (1,818, D-3 out of scope), `functions_udf.py` (1,300),
`dataframe/joins_columns.py` (1,238), `dataframe/plan_collapse.py` (1,057),
`dataframe/writer_readwriter.py` (1,111), `session/reader.py` (1,022).

## Half B — sequence and weighting (step 2)

**Opened:** 2026-09-10. **Step:** 2 of Card FACADE-AUDIT-0 (Half B only: D-1 second half, D-2).
**Method:** reading only. Every wall below is quoted from the file named beside it, number
as that file states it. No benchmark ran this round; another lane owns the box's builds.
A module with no isolating measurement is marked UNMEASURED with the measurement named
that would settle it. Half A (§§1–5) is not re-measured; `§2` row citations below refer to it.

### §6 Weighing — measured cost per candidate unit

#### FACADE-1 (Arrow C Stream boundary)

Export side, measured. [facade-boundary-baseline §3](../../docs/perf/facade-boundary-baseline.md):
`export/1000000/to_arrow` median 30.61 ms, `export/100000/to_arrow` 8.51 ms,
`export/1000000/toPandas` 55.60 ms. [PERF-ANALYSIS-1
§7.3](../../docs/perf/engine-iceberg-analysis-2026-09-04.md): `to_arrow` 1e6 × 7 at 24.1 ms
against Spark `local[8]` 197 ms; `toPandas` 49.3 ms against 206 ms. The export door is
already one to two orders under the old `collect()` wall, and the out-of-engine capsule
(`dataframe/core.py:4047,4054`) already exists beside the IPC sites.

Import side, UNMEASURED. No cell isolates the `pa_ipc.new_stream` encode legs (§3:
`dataframe/core.py:756,768,776,843,846`,
`session/create_dataframe_rows.py:856,860,861,864`, `ml/ext/_arrow_util.py:280,298,299,305`)
against the `register_arrow_stream_as_temp_view` capsule seam
(`dataframe/core.py:792`, `session/create_dataframe_rows.py:842,849`). The comparison that
would settle it: one frame through `createDataFrame` via the IPC fallback versus via the
arrow-stream seam on the same build, plus the mapInPandas encode leg timed alone.

Reading: FACADE-1 buys little wall — the export numbers above are the ceiling of what it
can remove. Its prize is architectural: pyarrow becomes optional, every consumer speaks one
capsule protocol, and every later unit's pins ride the same two doors.

#### FACADE-2 (`Column` as a pyo3 class over a DataFusion `Expr`)

The chain-scale win is already banked. [facade-boundary-baseline
§2](../../docs/perf/facade-boundary-baseline.md): depth-100 `withColumn` build 2,476.08 ms before,
366.11 ms after (6.76×); the residue is 346 ms of 445 ms inside DataFusion's own
`project` (`profile_chain.py 100`), which no facade move can touch. Per-op plan build is
closed as negligible except in chains ([PERF-ANALYSIS-1
§3](../../docs/perf/engine-iceberg-analysis-2026-09-04.md): `select` 45 µs, `filter` 29 µs,
`withColumn` 131 µs on a fresh frame). The statement overhead pair is closed the same way:
Spark door 0.32 ms per statement against native `repark.sql` 0.21 ms — the pre-parse
rewrites cost about 0.1 ms per statement.

So the measured tax FACADE-2 removes is about 0.1 ms per statement plus one analyzer
pass per `columns` touch, against the §4 surface it deletes: 7 `PyColumn.sql` string entry
points, 11 `column.py` SQL-assembly method groups, 25 session-level SQL scaffolds, all
re-parsed by the engine at plan time. That is a divergence-risk removal first and a wall
second — each rendered string is a place the facade and the parser can disagree. No cell
isolates the re-parse plus re-analyze cost per expression op at chain scale, so anything
beyond the 0.1 ms per statement is UNMEASURED; the measurement that would settle it is a
depth-N expression build through native `Expr` handles versus through SQL text on the same
build.

#### FACADE-3 (`createDataFrame` inference in Rust)

The largest remaining measured Python wall in the sequence. [PERF-ANALYSIS-1 §2, row
2](../../docs/perf/engine-iceberg-analysis-2026-09-04.md) isolates 1,717 ms at 1e5 × 7 (tuples
versus pandas input, both `.count()`), with the cProfile split: per-cell normalization
(`session/create_dataframe_values.py:165`) 2.14 s cumulative, nested-cell preparation
(`session/create_dataframe_inference.py:428`) 1.22 s, tuple-loop merge checks
(`session/create_dataframe_tuples.py:107` 0.94 s, `:46` 0.43 s) and tuple-to-Arrow
(`:183`) 2.49 s cumulative. PERF-FACADE-CDF-1 already took the tuple leg in Python
([facade-boundary-baseline §4](../../docs/perf/facade-boundary-baseline.md): `create/100000/tuples_count`
1,656.62 ms before, 70.30 ms after — 23.56×, reproduced 1,620.75 to 66.65 ms the same day).

What is left, measured: the explicit-schema path at 1,273.94 ms is now the slowest
`createDataFrame` shape (same §4), and the nested leg pays a 0.96× delegation cost for the
transpose plus census before the identical conversion. The pandas-shape control sits at
3.00–4.00 ms at 1e5. The Rust delta itself is UNMEASURED — no cell runs inference in Rust
yet — but its ceiling is anchored: the pandas shape shows what a columnar handoff costs,
and the explicit path's 1.27 s is almost entirely Python looping over cells that a Rust
inference pass would never visit per cell.

#### FACADE-4 (type conversions in Rust)

UNMEASURED. No cell isolates `repark_type_to_arrow`, `struct_type_from_arrow`, or DDL
parsing (`spark/types.py`, 1,834 lines, 43 pyarrow-reference lines, §2). Type conversion
hides inside larger walls — the `to_arrow` export cells, the `createDataFrame` legs, the
`_csv_smart` reader path — and no fixture pair varies conversion alone. The measurement
that would settle it: round-trip `repark_type_to_arrow`/`struct_type_from_arrow` over a
representative schema set (nested, decimal, timestamp variants) plus DDL-parse walls, run
before the unit as its own baseline section. Expectation management: this unit is a
correctness consolidation (one conversion table instead of three) until that baseline says
otherwise.

#### FACADE-5 (display renderer in Rust)

The quoted eager walls are scan effects, not render costs. [eager-preview-baseline](../../docs/perf/eager-preview-baseline.md):
parquet `repr`/`HTML`/vertical show 0.004 s before, 0.002 s after (the `count()` goes
away); `mapInArrow` `repr` 0.821 s before, 0.031 s after (2,000,000 UDF rows computed
before, one 65,536-row batch after); the vertical-show control is 0.031 s before and
0.032 s after, unchanged by design. None of these vary the Python format path
(`spark/dataframe/display.py`, 351 lines, bodies over capped rows calling into
`plan_collapse.py` formatters, §2). That format cost is UNMEASURED. The measurement that
would settle it: `show()`/`repr` over pre-materialized capped rows with the fetch leg and
the format leg timed separately.

#### UNMEASURED roll-call

These `logic`/`pyarrow` masses (§2 lines) have no isolating cell anywhere in `docs/perf/`
and no unit in the card's sequence claims them: `dataframe/plan_collapse.py` (1,057),
`dataframe/joins_columns.py` (1,238), `dataframe/writer_readwriter.py` (1,111),
`session/reader.py` (1,022), `session/reader_support.py` (477), `functions_udf.py`
(1,300), `dataframe/udf_bridge.py` (471), `dataframe/udf_projection.py` (349),
`dataframe/udf_schema.py` (64), `dataframe/grouped_udf.py` (145), the conversion core of
`spark/types.py` (1,834; weighed under FACADE-4 above), and the session SQL-UDF modules
(`sql_udf*.py`, ~2,800 combined). They are not sequenced here: D-2 names five units, and
the audit does not invent a sixth. The UDF-bridge pyarrow modules are the most likely next
weighing target once FACADE-1 gives them a capsule door on both sides. D-3 keeps ML
transformers (2,717 lines in `ml/feature/_transformers.py` alone) and the
library-wrapping parts of `ta.py` (1,818) out of the weighing entirely.

### §7 Freeze constraints — what each unit is forbidden to change

Source: [v1-0-api-freeze.json](../../docs/design/v1-0-api-freeze.json) (2026-09-02; 35 rows,
30 frozen, 888 frozen names; additive-only within a major, breaking a frozen row needs a
major plus a one-minor deprecation shim). The `isinstance` rule — `isinstance` on `Column`,
`Row`, and the type classes must keep working — is stated by the card (D-1); the freeze
file's nearest rows are C1 and F1 below, which freeze the class names but say nothing about
their runtime identity, so each unit names its own keeping mechanism.

- **FACADE-1.** Row B2 (DataFrame actions, accessors, collect shapes) is UNFROZEN
  (decision NO): the export-door signatures may change at a minor with a changelog line —
  but the DFCORE-4b goldens still pin their bytes, so in practice the door keeps its shape
  and only the transport changes. Row B1 is frozen: `mapInArrow`/`mapInPandas` names and
  params stay. Row N1 (packaging) is frozen: the extension module stays `repark._native`,
  and pyarrow is today a hard dependency (`python/repark/pyproject.toml` line 20:
  `dependencies = ["pyarrow>=25.0.0", ...]`), so making it optional moves a name from
  `dependencies` to `optional-dependencies` on an N1-frozen surface — allowed with a
  changelog line, and every `import pyarrow` site (§2: 23 runtime importers) must degrade
  to a loud error, never a silent skew.
- **FACADE-2.** Row C1 (Column API) is frozen YES except `cast` and `eqNullSafe`: every
  other method name, and `required_params` where listed, stays. The function-builder rows
  J1/J4–J9 are frozen (J2, the aggregate/window root module, is unfrozen). The freeze-safe
  mechanism for `isinstance(c, repark.Column)` is to keep the Python `Column` class object
  as the public type and grow the native `PyColumn` handle it already holds (§2 binding
  symbols: `Column._inner` is the native `PyColumn` today) — display strings move into
  Rust methods, the class identity never moves. Making the pyo3 class itself the public
  `Column` would need a different keeping mechanism; none is decided (open question Q1).
- **FACADE-3.** Row A2 is frozen: `createDataFrame`/`create_dataframe` names and the
  `data` parameter stay, as does the dispatch over rows, tuples, dicts, pandas, and
  explicit schema. Row O1 (errors taxonomy) is frozen: inference refusals keep their
  exception classes. D-3 binds pickling: no pickling behaviour change without a pin, so
  the unit carries a pickle round-trip pin on inferred frames.
- **FACADE-4.** Row F1 (`repark.spark.types` + `Row`) is frozen YES except `VariantType`:
  every type class name stays, and the conversion members `repark_type_to_arrow` and
  `struct_type_from_arrow` are frozen by name with their `required_params`. Row K1 (DDL)
  is frozen: DDL spellings stay byte-identical. The unit keeps the Python type classes as
  the public objects and moves only the conversion tables into Rust, so `isinstance`
  holds by construction — there is no class-identity move to defend.
- **FACADE-5.** Row B2 is unfrozen, so `show`/`repr` shapes may legally change at a minor
  with a changelog line — but the DFCORE-4b golden files pin them byte-for-byte, and this
  audit treats the goldens as the binding constraint: the Rust renderer must reproduce
  them exactly, and any intended byte change is a separate owner decision, not a
  passenger. Display limits come from session settings (row L1, frozen except
  `repark.merge.*`), so the renderer reads the same keys through the same seam.
