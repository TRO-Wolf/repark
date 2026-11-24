# map — repark-spark/src/tests

## Layout

Lib-root unit battery for the Spark SQL door: tests, shared fixtures and the module
manifest only. Production code lives in [..](..). One row per area; every leaf links
its files. The pre-condensation record is
[history copy](../../../../docs/history/map-ratchet-1/crates-repark-spark-src-tests-map-2026-11-20.md).

| area | files | charter |
|---|---|---|
| manifest | [mod.rs](mod.rs), [common.rs](common.rs) | `mod` manifest; shared re-exports (`use super::common::*`) |
| fixtures | [fixtures/map.md](fixtures/map.md) | Spark-written fixtures behind the IO pins |
| call core | [call.rs](call.rs), [call_procedures_1.rs](call_procedures_1.rs), [call_procedures_2.rs](call_procedures_2.rs), [call_procs_route_1.rs](call_procs_route_1.rs), [call_register.rs](call_register.rs), [call_manifests.rs](call_manifests.rs) | CALL routing, procedures, registration |
| call rewrite | [call_rewrite_dangling.rs](call_rewrite_dangling.rs), [call_rewrite_options.rs](call_rewrite_options.rs), [call_rdf_branch.rs](call_rdf_branch.rs), [call_rdf_options.rs](call_rdf_options.rs), [call_rdf_sort.rs](call_rdf_sort.rs), [call_rm_deletes.rs](call_rm_deletes.rs), [call_v3.rs](call_v3.rs), [call_v3_dv.rs](call_v3_dv.rs) | CALL rewrites, RDF sort/branch, v3 calls |
| call orphans | [call_orphan.rs](call_orphan.rs), [call_orphan_ancestor.rs](call_orphan_ancestor.rs), [call_orphan_cotenancy.rs](call_orphan_cotenancy.rs), [call_orphan_scope.rs](call_orphan_scope.rs), [call_orphan_view.rs](call_orphan_view.rs) | orphan-file CALLs per scope |
| kill integrity | [kill_mid_write.rs](kill_mid_write.rs), [kill_mid_write_hold.rs](kill_mid_write_hold.rs), [kill_mid_write_release.rs](kill_mid_write_release.rs), [commit_unknown_doors.rs](commit_unknown_doors.rs), [commit_unknown_gate.rs](commit_unknown_gate.rs), [resource_bound.rs](resource_bound.rs), [resource_bound_kill.rs](resource_bound_kill.rs) | kill-mid-write, unknown-commit, resource bounds; `#[cfg(unix)]` |
| merge | [merge.rs](merge.rs), [merge_cardinality_mor.rs](merge_cardinality_mor.rs), [merge_nmbs.rs](merge_nmbs.rs), [partitioned_merge.rs](partitioned_merge.rs) | MERGE execute strictness, cardinality, partitioning |
| dml | [dml.rs](dml.rs), [update_cast.rs](update_cast.rs), [delete_granularity.rs](delete_granularity.rs), [replace_where.rs](replace_where.rs), [replace_where_oracle.rs](replace_where_oracle.rs), [replace_where_sources.rs](replace_where_sources.rs), [wrapped_subquery_dml.rs](wrapped_subquery_dml.rs) | UPDATE/DELETE doors, subquery DML, REPLACE WHERE |
| ctas create | [ctas.rs](ctas.rs), [ctas_view.rs](ctas_view.rs), [create_table.rs](create_table.rs), [create_table_options.rs](create_table_options.rs), [create_typed_partition.rs](create_typed_partition.rs), [create_format_version_one.rs](create_format_version_one.rs), [replace_table.rs](replace_table.rs), [replace_columns.rs](replace_columns.rs), [service_managed_ctas.rs](service_managed_ctas.rs), [partitioned_ctas.rs](partitioned_ctas.rs) | CTAS, CREATE/REPLACE TABLE, options, formats |
| insert | [insert_arity.rs](insert_arity.rs), [insert_overwrite.rs](insert_overwrite.rs), [dyn_by_name_overwrite.rs](dyn_by_name_overwrite.rs), [dyn_partition_overwrite.rs](dyn_partition_overwrite.rs), [overwrite_mode.rs](overwrite_mode.rs), [partition_overwrite.rs](partition_overwrite.rs), [transform_overwrite.rs](transform_overwrite.rs), [partition_append.rs](partition_append.rs), [pg_insert.rs](pg_insert.rs), [cast_overflow_insert.rs](cast_overflow_insert.rs) | INSERT doors, dynamic overwrite, overflow refusal |
| write misc | [write_door.rs](write_door.rs), [write_defaults.rs](write_defaults.rs), [write_body_carriers.rs](write_body_carriers.rs), [write_to_branch.rs](write_to_branch.rs), [router.rs](router.rs), [truncate.rs](truncate.rs), [declared_refuse.rs](declared_refuse.rs) | write doors, defaults, branch writes, TRUNCATE |
| v3 dml | [v3_cow.rs](v3_cow.rs), [v3_cow_lift.rs](v3_cow_lift.rs), [v3_mor_dml.rs](v3_mor_dml.rs), [v3_dml_scan.rs](v3_dml_scan.rs), [v3_subquery_dml.rs](v3_subquery_dml.rs), [v3_row_order.rs](v3_row_order.rs), [v3_fanout_order.rs](v3_fanout_order.rs), [v3_legacy_delete.rs](v3_legacy_delete.rs) | format-v3 copy-on-write / merge-on-read DML |
| v3 meta | [v3e3.rs](v3e3.rs), [v3e4.rs](v3e4.rs), [v3_lineage.rs](v3_lineage.rs), [v3_types.rs](v3_types.rs), [v3_timestamp_ns_door.rs](v3_timestamp_ns_door.rs), [v3_upgrade.rs](v3_upgrade.rs), [v3_upgrade_calls.rs](v3_upgrade_calls.rs) | v3 extensions, lineage, row lineage, upgrades |
| refs | [ref_ddl.rs](ref_ddl.rs), [ref_branch_on_empty.rs](ref_branch_on_empty.rs), [v1_ref_writes.rs](v1_ref_writes.rs), [refs_and_wap.rs](refs_and_wap.rs), [time_travel.rs](time_travel.rs), [branch_ops.rs](branch_ops.rs), [branch_read_schema.rs](branch_read_schema.rs), [metadata_tables_asof.rs](metadata_tables_asof.rs), [tz_asof_1.rs](tz_asof_1.rs) | branch/tag DDL, empty-branch writes, time travel |
| wap | [wap_branch.rs](wap_branch.rs), [wap_id.rs](wap_id.rs), [wap_id_sweep.rs](wap_id_sweep.rs), [wap_publish.rs](wap_publish.rs) | WAP branch writes and publish |
| views | [temp_view_alter.rs](temp_view_alter.rs), [temp_view_attr_ids.rs](temp_view_attr_ids.rs), [temp_view_errors.rs](temp_view_errors.rs), [temp_view_routing.rs](temp_view_routing.rs), [view_use_resolution.rs](view_use_resolution.rs), [viewless_catalog.rs](viewless_catalog.rs), [alter_view_routing.rs](alter_view_routing.rs), [describe_view_routing.rs](describe_view_routing.rs), [show_create_view_routing.rs](show_create_view_routing.rs) | temp-view DDL, ALTER/DESCRIBE VIEW routing |
| describe | [describe_table.rs](describe_table.rs), [describe_show.rs](describe_show.rs), [describe_column_errors.rs](describe_column_errors.rs), [describe_near_miss.rs](describe_near_miss.rs), [describe_owner.rs](describe_owner.rs), [show_create.rs](show_create.rs), [show_table_extended.rs](show_table_extended.rs), [show_table_extended_near_miss.rs](show_table_extended_near_miss.rs), [show_tblproperties.rs](show_tblproperties.rs), [show_tblproperties_routing.rs](show_tblproperties_routing.rs), [use_ddl.rs](use_ddl.rs) | DESCRIBE/SHOW display, near-miss probes, USE |
| case | [casesens_ddl.rs](casesens_ddl.rs), [casesens_scopes.rs](casesens_scopes.rs), [casesens_true.rs](casesens_true.rs), [casesens_twins.rs](casesens_twins.rs), [case_twin_columns.rs](case_twin_columns.rs), [ident_dialect.rs](ident_dialect.rs), [identifier_fields.rs](identifier_fields.rs), [input_file_name.rs](input_file_name.rs) | case sensitivity, identifier grammar |
| strings | [spark_string_literals.rs](spark_string_literals.rs), [string_literal_escape_1.rs](string_literal_escape_1.rs), [spark_dialect.rs](spark_dialect.rs), [collation.rs](collation.rs), [sort_order_parse.rs](sort_order_parse.rs), [subscript_dot_access.rs](subscript_dot_access.rs) | string literals, dialect, collation, sort parse |
| numeric | [cast_binary.rs](cast_binary.rs), [cast_binary_ansi.rs](cast_binary_ansi.rs), [decimal.rs](decimal.rs), [float_agg.rs](float_agg.rs), [nan_pushdown.rs](nan_pushdown.rs), [count_fold.rs](count_fold.rs) | binary casts, decimal, float semantics |
| nested | [nested_assign.rs](nested_assign.rs), [nested_assign_oracle.rs](nested_assign_oracle.rs), [nested_column_ddl.rs](nested_column_ddl.rs), [nested_ns_routes.rs](nested_ns_routes.rs), [map_column_panic_1.rs](map_column_panic_1.rs), [map_key_alter.rs](map_key_alter.rs), [map_key_union.rs](map_key_union.rs) | nested writes, map keys, column DDL |
| timestamps | [ntz_door.rs](ntz_door.rs), [ntz_store.rs](ntz_store.rs), [ntz_store_quotes.rs](ntz_store_quotes.rs), [ntz_values_mix.rs](ntz_values_mix.rs), [ntz_nested_refuse_routes.rs](ntz_nested_refuse_routes.rs), [ltz_store.rs](ltz_store.rs), [u9_timestamp_ltz.rs](u9_timestamp_ltz.rs), [window_temporal_range.rs](window_temporal_range.rs) | NTZ/LTZ doors, stores, temporal ranges |
| uuid | [u9_map.rs](u9_map.rs), [u9_void_uuid.rs](u9_void_uuid.rs), [u9_uuid_void_writes.rs](u9_uuid_void_writes.rs) | UUID/VOID/U9 type writes, multiset set-op MAP refusals |
| catalog | [catalog_ops.rs](catalog_ops.rs), [catalog_cache_staleness.rs](catalog_cache_staleness.rs), [session_catalog.rs](session_catalog.rs), [session_write_conf.rs](session_write_conf.rs), [session_write_conf_removals.rs](session_write_conf_removals.rs), [namespace_ddl.rs](namespace_ddl.rs), [local_fs_ddl.rs](local_fs_ddl.rs), [pa_local_fs_policy.rs](pa_local_fs_policy.rs), [hadoop_rename.rs](hadoop_rename.rs) | catalogs, namespaces, session confs, FS policy |
| ddl misc | [alter.rs](alter.rs), [alter_write_order.rs](alter_write_order.rs), [alter_write_order_transform.rs](alter_write_order_transform.rs), [column_move.rs](column_move.rs), [column_comment_ddl.rs](column_comment_ddl.rs), [rename_target.rs](rename_target.rs), [ice_ddl_clauses_1.rs](ice_ddl_clauses_1.rs), [unset_tblproperties.rs](unset_tblproperties.rs), [property_display_redaction.rs](property_display_redaction.rs) | ALTER, column moves, renames, secret redaction |
| metadata | [metadata_columns.rs](metadata_columns.rs), [metadata_columns_deleted.rs](metadata_columns_deleted.rs), [metadata_columns_reserved.rs](metadata_columns_reserved.rs), [metadata_columns_scope.rs](metadata_columns_scope.rs), [metadata_tables.rs](metadata_tables.rs) | metadata columns and tables |
| maintenance | [run_maintenance.rs](run_maintenance.rs), [apply_partitioning.rs](apply_partitioning.rs), [plan_partitioning.rs](plan_partitioning.rs), [purge.rs](purge.rs) | maintenance CALLs, partition planning, PURGE |
| joins | [join_condition_refusals.rs](join_condition_refusals.rs), [join_null_keys.rs](join_null_keys.rs), [lambda_door.rs](lambda_door.rs), [list_null_compound.rs](list_null_compound.rs) | join conditions, lambdas, null compounds |
| guards | [accept_any_naming.rs](accept_any_naming.rs), [accept_any_refusals.rs](accept_any_refusals.rs), [accept_any_routing.rs](accept_any_routing.rs), [enc_1.rs](enc_1.rs), [enc_1_fold.rs](enc_1_fold.rs), [mem_layout.rs](mem_layout.rs), [normalize.rs](normalize.rs), [parquet_dictionary.rs](parquet_dictionary.rs) | accept-any schema, encryption refusal, layout |

Rules for new files: a new test joins the row of its area; a new area is a new row.

## Purpose

Unit pins for the Spark SQL door over production-built sessions. Each row above routes to
its leaves; the per-leaf record before chunk 1 is the
[history copy](../../../../docs/history/map-ratchet-1/crates-repark-spark-src-tests-map-2026-11-20.md).

## I want to...

| ...do this | go to |
|---|---|
| Pin a CALL behavior | the `call core`, `call rewrite` or `call orphans` row |
| Pin a kill, unknown-commit or resource-bound behavior | the `kill integrity` row |
| Pin a MERGE, UPDATE or DELETE behavior | the `merge` or `dml` row |
| Pin a CTAS, CREATE or INSERT behavior | the `ctas create`, `insert` or `write misc` row |
| Pin a format-v3 behavior | the `v3 dml` or `v3 meta` row |
| Pin a branch, tag, WAP or time-travel behavior | the `refs` or `wap` row |
| Pin a temp-view or ALTER/DESCRIBE VIEW behavior | the `views` row |
| Pin a DESCRIBE or SHOW behavior | the `describe` row |
| Pin a case-sensitivity or identifier behavior | the `case` row |
| Pin a string, collation or cast behavior | the `strings` or `numeric` row |
| Pin a nested-type or map behavior | the `nested` row |
| Pin a timestamp behavior | the `timestamps` row |
| Pin a catalog, namespace or session-conf behavior | the `catalog` row |
| Pin a maintenance CALL | the `maintenance` row |
| Pin an encryption or accept-any refusal | the `guards` row |
| Read the pre-condensation record | the history copy under Layout |
| Run the suite | `cargo test -p repark-spark tests::` |

## Pointers

- Up: [../map.md](../map.md)
- Registry pins: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
- Surface matrix (also `#[cfg(test)]`): [../matrix.rs](../matrix.rs)
- Pre-condensation record: [history copy](../../../../docs/history/map-ratchet-1/crates-repark-spark-src-tests-map-2026-11-20.md)

## Debug

| Symptom | First check |
|---|---|
| Test identity doubt | `cargo test -p repark-spark --lib -- --list`; production-aligned leaf names |
| Matrix pin string red / stale | [../matrix.rs](../matrix.rs) strings are `--list` names; renames only |
| Registry pin path stale | filesystem form `crates/repark-spark/src/tests/<file>.rs::leaf` |
| Missing `TempDir` / Arrow types in a leaf | `use super::common::*;` ([common.rs](common.rs)) |
| Byte-frozen leaves red | [spark_string_literals.rs](spark_string_literals.rs) and [cast_binary.rs](cast_binary.rs) are sha256-frozen by `python/repark-parity/tests/test_pr_245_revalidation_record.py`; revert, never re-hash |
| Cross-leaf helper not found | `pub(super)` in [common.rs](common.rs), not private in one leaf |

First checks: `cargo test -p repark-spark tests::<module>::`. Escalate to: [../map.md#debug](../map.md).
