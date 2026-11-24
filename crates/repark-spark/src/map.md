# map — repark-spark/src

## Layout

Source for the Spark SQL door. `lib.rs` is the manifest; the router body lives in
`router.rs`. DDL, DML, reference, maintenance, metadata, time-travel and passthrough
handlers share the session and catalog seams. One row per area; every leaf and submap
links its files. The pre-condensation record is
[history copy](../../../docs/history/map-ratchet-1/crates-repark-spark-src-map-2026-11-20.md).

| area | files | charter |
|---|---|---|
| manifest | [lib.rs](lib.rs), [matrix.rs](matrix.rs) | crate manifest; the shipped-vs-absent surface matrix |
| router | [router/map.md](router/map.md), [router.rs](router.rs), [spark_ast.rs](spark_ast.rs), [view_dispatch.rs](view_dispatch.rs), [write_door.rs](write_door.rs) | statement routing, AST execution, write-door tags |
| normalize | [normalize/map.md](normalize/map.md), [normalize.rs](normalize.rs) | parse normalization, DML valves |
| dialect | [dialect/map.md](dialect/map.md), [dialect.rs](dialect.rs) | session and executing-parse dialects |
| dml | [dml_target.rs](dml_target.rs), [update_cast.rs](update_cast.rs), [truncate.rs](truncate.rs), [ctas.rs](ctas.rs), [create_table.rs](create_table.rs), [append_with_options.rs](append_with_options.rs), [write_options.rs](write_options.rs) | DML targets, CTAS/CREATE, write options |
| insert | [insert_by_name/map.md](insert_by_name/map.md), [insert_overwrite/map.md](insert_overwrite/map.md), [insert_by_name.rs](insert_by_name.rs), [insert_overwrite.rs](insert_overwrite.rs), [insert_arity.rs](insert_arity.rs), [insert_timestamp_ns.rs](insert_timestamp_ns.rs) | BY NAME, OVERWRITE, arity, timestamp-ns inserts |
| merge | [merge/map.md](merge/map.md), [merge_fragments/map.md](merge_fragments/map.md), [merge.rs](merge.rs), [merge_fragments.rs](merge_fragments.rs) | MERGE lowering, fragments, star sentinel |
| call | [call/map.md](call/map.md), [call.rs](call.rs), [call_args.rs](call_args.rs) | maintenance CALL procedures |
| refs | [ref_ddl/map.md](ref_ddl/map.md), [namespace_ddl/map.md](namespace_ddl/map.md), [ref_ddl.rs](ref_ddl.rs), [namespace_ddl.rs](namespace_ddl.rs), [wap.rs](wap.rs), [write_to_branch.rs](write_to_branch.rs) | branch/tag DDL, namespaces, WAP and branch writes |
| views | [view_ddl/map.md](view_ddl/map.md), [describe_show/map.md](describe_show/map.md), [describe_show.rs](describe_show.rs), [describe_column.rs](describe_column.rs), [show_create.rs](show_create.rs), [show_table_extended.rs](show_table_extended.rs), [table_props_ddl.rs](table_props_ddl.rs), [table_props_view.rs](table_props_view.rs), [use_ddl.rs](use_ddl.rs), [local_fs_ddl.rs](local_fs_ddl.rs) | views, DESCRIBE/SHOW display, USE, table properties |
| alter | [alter.rs](alter.rs), [alter_column_type.rs](alter_column_type.rs), [alter_write_order.rs](alter_write_order.rs), [column_move.rs](column_move.rs), [nested_column_ddl.rs](nested_column_ddl.rs), [replace_columns.rs](replace_columns.rs), [replace_partition_field.rs](replace_partition_field.rs), [catalog_ops.rs](catalog_ops.rs) | ALTER TABLE, column moves, catalog ops |
| literals | [spark_literals/map.md](spark_literals/map.md), [type_table/map.md](type_table/map.md), [void_type/map.md](void_type/map.md), [keyword_lower/map.md](keyword_lower/map.md), [spark_literals.rs](spark_literals.rs), [spark_literal_typing.rs](spark_literal_typing.rs), [spark_tree_string.rs](spark_tree_string.rs), [spark_typed.rs](spark_typed.rs), [spark_type_names.rs](spark_type_names.rs), [keyword_lower.rs](keyword_lower.rs), [bare_nullary.rs](bare_nullary.rs), [bare_unit.rs](bare_unit.rs), [type_table.rs](type_table.rs), [void_type.rs](void_type.rs), [uuid_cast.rs](uuid_cast.rs), [cast_gate.rs](cast_gate.rs) | literals, type names, keyword lowering, casts |
| strings | [spark_rewrites/map.md](spark_rewrites/map.md), [collation.rs](collation.rs), [sort_order_parse.rs](sort_order_parse.rs) | string rewrites, collation, sort parse |
| time | [time_travel/map.md](time_travel/map.md), [time_travel.rs](time_travel.rs), [time_window.rs](time_window.rs), [window_range.rs](window_range.rs) | time travel, window frames |
| values | [values_marker/map.md](values_marker/map.md), [values_marker.rs](values_marker.rs) | VALUES markers |
| extension | [extension/map.md](extension/map.md), [extension.rs](extension.rs), [format_version.rs](format_version.rs), [metadata_tables.rs](metadata_tables.rs) | session extension (pre-coercion Spark analyzer rules incl. `spark_extract_date_part`), format versions, metadata tables |
| tests | [tests/map.md](tests/map.md) | lib-root unit battery, by production module |

Rules for new files: a new leaf joins the row of its area; a new area is a new row
with its submap.

## Purpose

The Spark SQL door over DataFusion and the owned Iceberg fork. The MoR valve
predicate is owned by `repark_iceberg::write`. End-to-end pins live in
[tests/map.md](tests/map.md); what this door ships is [matrix.rs](matrix.rs).

## I want to...

| ...do this | go to |
|---|---|
| Trace statement routing or a refusal | the `router` row |
| Pin a MERGE behavior | the `merge` row |
| Pin an INSERT OVERWRITE or BY NAME behavior | the `insert` row |
| Pin a branch, tag, namespace or WAP behavior | the `refs` row |
| Pin a maintenance CALL | the `call` row |
| Pin a CTAS or CREATE behavior | the `dml` row |
| Pin an ALTER or column-move behavior | the `alter` row |
| Pin a temp-view, DESCRIBE or SHOW behavior | the `views` row |
| Pin a literal, type-name or cast behavior | the `literals` row |
| Pin a time-travel or window-frame behavior | the `time` row |
| Pin a router behavior end to end | the `tests` row |
| See what this door ships vs deliberately does not | [matrix.rs](matrix.rs) |
| Read the pre-condensation record | the history copy under Layout |
| Run the suite | `cargo test -p repark-spark` |

## Pointers

- Up: [../map.md](../map.md)
- Unit battery: [tests/map.md](tests/map.md)
- Surface matrix: [matrix.rs](matrix.rs)
- Pre-condensation record: [history copy](../../../docs/history/map-ratchet-1/crates-repark-spark-src-map-2026-11-20.md)

## Debug

| Symptom | First check |
|---|---|
| Statement unexpectedly passes to DataFusion | [router.rs](router.rs) arm order; `normalize::parse_single_normalized` returned `None` |
| A DML guard on a router arm is fail-open | attach it inside `spark_ast::execute_passthrough`: the router parses with `DatabricksDialect` while the executor re-parses with the session dialect |
| A `DELETE`/`UPDATE` with a subquery `WHERE` was refused | by design (G3-E8): `normalize::refuse_dml_subquery_predicate` over-refuses on purpose |
| `RANGE` window wider/narrower than Spark | [window_range.rs](window_range.rs): bare numbers over datetime keys are months, refused or restated |
| A `__repark_tt_*` name appeared in `SHOW TABLES` | from either SQL door it is a LEAK; from the reader-options path it is the DOCUMENTED RESIDUAL |
| `matrix::matrix_maps_every_surface` RED | a surface ID joined `repark_common::surfaces::ALL` with no `Tested`/`DeliberatelyAbsent` row in [matrix.rs](matrix.rs) |

First checks: `cargo test -p repark-spark <module>::`. Escalate to: [../map.md#debug](../map.md).
