# map — `repark.spark.dataframe`

## Layout

The Spark DataFrame facade. It builds lazy native plans and exposes Spark-compatible
actions, joins, grouping, exports, UDF bridges and writers. Engine computation stays
in Rust; Python callbacks run only where the API accepts user UDFs and receive Arrow
batches. One row per area; every module links its files. The pre-condensation
record is
[history copy](../../../../../../docs/history/map-ratchet-1/python-repark-src-repark-spark-dataframe-map-2026-11-20.md).

| area | files | charter |
|---|---|---|
| core | [core.py](core.py), [__init__.py](__init__.py) | DataFrame methods, plan glue, re-exports |
| transforms | [colregex.py](colregex.py), [replace_expr.py](replace_expr.py), [sampling.py](sampling.py), [statistics.py](statistics.py), [actions_export.py](actions_export.py), [rows_export.py](rows_export.py), [display.py](display.py), [explain.py](explain.py) | transforms, stats, display, explain, exports |
| joins | [joins_columns.py](joins_columns.py), [join_attr_tokens.py](join_attr_tokens.py), [cogroup.py](cogroup.py), [grouped_arrow.py](grouped_arrow.py), [grouped_pivot.py](grouped_pivot.py), [grouped_udf.py](grouped_udf.py), [subquery.py](subquery.py) | joins, grouping, pivot, cogroup, subquery |
| plans | [plan_collapse.py](plan_collapse.py), [plan_introspect.py](plan_introspect.py), [frame_nodes.py](frame_nodes.py), [unemitted_ids.py](unemitted_ids.py), [repartition_ops.py](repartition_ops.py), [streaming_batch.py](streaming_batch.py), [polars_cells.py](polars_cells.py) | plan rewrites, introspection, frame nodes |
| session | [eager.py](eager.py), [cache_handle.py](cache_handle.py) | eager materialization, cache-view ownership; the table-level mark (`_note_table_level`, `_aliased`) that `_bind_lazy` passes to the lazy cache, and `_held_by_table_fill`, which keeps C-009's snapshot-token drop off a lazily held table-level frame (#1394 delta 2) |
| udf | [udf_bridge.py](udf_bridge.py), [udf_projection.py](udf_projection.py), [udf_schema.py](udf_schema.py), [udf_window_projection.py](udf_window_projection.py), [export_errors.py](export_errors.py) | UDF callbacks, projections, error mapping |
| writers | [writer_readwriter.py](writer_readwriter.py), [writer_layout.py](writer_layout.py), [writer_save.py](writer_save.py), [writer_schema.py](writer_schema.py), [writer_text.py](writer_text.py), [writer_s3.py](writer_s3.py) | writers, layouts, text and S3 paths; both plain path writers call `writer_layout._refuse_dynamic_path_overwrite` first (PLAIN-DYNAMIC-OVERWRITE-GUARD-1) |
| surface | [surface_a.py](surface_a.py), [surface_b.py](surface_b.py), [io_declared.py](io_declared.py) | extra surface, declared IO |

Rules for new files: a new module joins the row of its area; a new area is a new row.

## Purpose

Python builds plans; Python never touches rows. Region modules may import helpers
from [core.py](core.py); `core.py` binds them before importing region classes.
Under CAP-1, `core.py` carries an exact exception row in `scripts/check_lib_py.py`.

## I want to...

| ...do this | go to |
|---|---|
| Call a DataFrame method or trace plan glue | the `core` row |
| Transform, sample, describe or show a frame | the `transforms` row |
| Join, group, pivot or cogroup | the `joins` row |
| Rewrite or inspect a plan | the `plans` row |
| Cache or eagerly materialize | the `session` row |
| Bridge a UDF or map an export error | the `udf` row |
| Write a frame out | the `writers` row |
| Read the pre-condensation record | the history copy under Layout |
| Run the suite | `make py-test-facade` |

## Pointers

- Up: [../map.md](../map.md)
- Rust engine contracts: [../../../../../../crates/repark-core/src/map.md](../../../../../../crates/repark-core/src/map.md)
- Facade tests: [../../../../tests/map.md](../../../../tests/map.md)
- Facade design: [../../../../../../docs/design/python-facade.md](../../../../../../docs/design/python-facade.md)
- Pre-condensation record: [history copy](../../../../../../docs/history/map-ratchet-1/python-repark-src-repark-spark-dataframe-map-2026-11-20.md)

## Debug

| Symptom | First check |
|---|---|
| Import failure | the re-export block in [core.py](core.py) and the package `__init__.py` |
| Circular import | region modules import from [core.py](core.py) only; `core.py` binds them first |
| Identity or display regression | stamped attribute ids, engine-name overlays, `_spawn` paths |
| Scratch-view failure | facade-owned views are home-qualified; engine-owned scratch registration has its own lifecycle |

First checks: `make py-test-facade`. Escalate to: [../map.md](../map.md).
