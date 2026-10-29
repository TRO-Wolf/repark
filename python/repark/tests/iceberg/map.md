# map — python/repark/tests/iceberg

## Purpose

Iceberg table format: catalogs and namespaces, DDL and schema evolution, metadata tables and columns, time travel, branches and tags, maintenance procedures, views, scans and pruning, and format v3 (deletion vectors, row lineage, upgrade).

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_alter_table.py](test_alter_table.py) — R-ALTER-TABLE — Spark ALTER TABLE schema evolution facade pins.
- [test_describe_table.py](test_describe_table.py) — Facade pins for DESCRIBE [TABLE] [EXTENDED|FORMATTED] on Iceberg tables.
- [test_empty_projection_count_1.py](test_empty_projection_count_1.py) — `count(*)` over an empty projection (changelog and v3 DV reads) answers the row count.
- [test_ice_branch_ops_1.py](test_ice_branch_ops_1.py) — ICE-BRANCH-OPS-1 pins: four Iceberg branch procedures on both doors.
- [test_ice_branch_schema_1.py](test_ice_branch_schema_1.py) — IPI-07 R-BRANCH-SCHEMA — a branch read projects the table's current schema.
- [test_ice_case_twin_1.py](test_ice_case_twin_1.py) — ICE-CASE-TWIN-1 — the RP-56 verifier fold pins against the p9/p9b Spark oracle.
- [test_ice_catalog_session_1.py](test_ice_catalog_session_1.py) — ICE-CATALOG-SESSION-1: catalog and session SQL against the recorded Spark answers.
- [test_ice_change_column_rename_1.py](test_ice_change_column_rename_1.py) — ICE-CHANGE-COLUMN-RENAME-1 — the Hive-style CHANGE COLUMN rename refusal.
- [test_ice_changelog_1.py](test_ice_changelog_1.py) — ICE-CHANGELOG-1 — incremental append reads, ``t.changes`` and ``create_changelog_view``.
- [test_ice_column_reorder_1.py](test_ice_column_reorder_1.py) — ICE-COLUMN-REORDER-1 — ALTER COLUMN FIRST/AFTER column-move pins.
- [test_ice_column_reorder_1_truth.json](test_ice_column_reorder_1_truth.json) — Recorded truth read by `test_ice_column_reorder_1.py`; re-derived by `_record_ice_column_reorder_1.py`.
- [test_ice_count_fold_1.py](test_ice_count_fold_1.py) — ICE-COUNT-FOLD-1 — count(*) folds from Iceberg statistics with names and types unchanged.
- [test_ice_ddl_alter_2.py](test_ice_ddl_alter_2.py) — Facade pins for `ALTER COLUMN … COMMENT`.
- [test_ice_ddl_clauses_1.py](test_ice_ddl_clauses_1.py) — IPI-26 + IPI-27 round 1 — the two silent rewrite bugs in the Spark door.
- [test_ice_drop_ns_1.py](test_ice_drop_ns_1.py) — ICE-DROP-NS-1 — ``DROP NAMESPACE`` on a non-empty namespace refuses like Spark.
- [test_ice_error_conditions_1.py](test_ice_error_conditions_1.py) — ICE-ERROR-CONDITIONS-1: the `[CONDITION]` and SQLSTATE parsers that attach Spark error conditions to engine errors.
- [test_ice_hadoop_vn_1.py](test_ice_hadoop_vn_1.py) — ICE-HADOOP-VN-1 — a stale Hadoop `vN` writer raises loud and loses nothing.
- [test_ice_metadata_cols_1.py](test_ice_metadata_cols_1.py) — IPI-20 PR-1 — the Spark door serves the metadata columns ``_file`` … ``_deleted``.
- [test_ice_mixed_case_1.py](test_ice_mixed_case_1.py) — ICE-MIXED-CASE-1 — the Spark door resolves mixed-case columns case-insensitively.
- [test_ice_mt_as_of_1.py](test_ice_mt_as_of_1.py) — xo55-mt R1 — the Spark door serves ``VERSION AS OF`` on Iceberg metadata tables.
- [test_ice_mt_describe_1.py](test_ice_mt_describe_1.py) — md-r1 — DESCRIBE on an Iceberg metadata table answers one row per column.
- [test_ice_mt_reader_1.py](test_ice_mt_reader_1.py) — IPI-23-MT-READER-1 — the DataFrame reader loads Iceberg metadata tables.
- [test_ice_nan_pushdown_1.py](test_ice_nan_pushdown_1.py) — ICE-NAN-PUSHDOWN-1 — NaN filter pushdown answers Spark end to end on Iceberg scans.
- [test_ice_nested_evo_1.py](test_ice_nested_evo_1.py) — ICE-NESTED-EVO-1 — adopt a Spark-evolved nested table, and nested DDL, against Spark 4.1.2.
- [test_ice_nested_evo_1_schema.py](test_ice_nested_evo_1_schema.py) — ICE-NESTED-EVO-1 round 2 — nested DDL metadata (ids, order, doc, required) against Spark 4.1.2.
- [test_ice_page_prune_1.py](test_ice_page_prune_1.py) — ICE-PAGE-PRUNE-1 — page-pruning scans answer Spark row for row, both doors.
- [test_ice_procedures_1.py](test_ice_procedures_1.py) — ICE-PROCEDURES-1 binder pins: positional and mixed CALL forms, exact refusals.
- [test_ice_procs_route_1.py](test_ice_procs_route_1.py) — ICE-PROCS-ROUTE-1 pins: four Iceberg maintenance procedures answer as Spark.
- [test_ice_promote_read_1.py](test_ice_promote_read_1.py) — ICE-PROMOTE-READ-1 — reads and DML after a legal Iceberg type promotion answer Spark.
- [test_ice_rdf_options_1.py](test_ice_rdf_options_1.py) — ICE-RDF-OPTIONS-1 options-map pins against the recorded Spark oracle.
- [test_ice_rdf_sort_parse_1.py](test_ice_rdf_sort_parse_1.py) — ICE-RDF-SORT-PARSE-1 — rewrite_data_files sort and zorder against the Spark oracle.
- [test_ice_replace_columns_1.py](test_ice_replace_columns_1.py) — ICE-REPLACE-COLUMNS-1 — ``ALTER TABLE … REPLACE COLUMNS`` drops and re-adds every column.
- [test_ice_rm_deletes_1.py](test_ice_rm_deletes_1.py) — ICE-RM-DELETES-1 rewrite_manifests pins against the recorded Spark oracle.
- [test_ice_small_parser_1.py](test_ice_small_parser_1.py) — IPI-21 + IPI-25 + IPI-42 — the three small parser shapes over finished behaviour.
- [test_ice_system_functions_1.py](test_ice_system_functions_1.py) — IPI-29 system functions rounds 1-3 — the catalog-qualified system-function pins.
- [test_ice_tt_resolve_1.py](test_ice_tt_resolve_1.py) — ICE-TT-RESOLVE-1 — Spark's time-travel resolution on every door.
- [test_ice_tt_resolve_1_tt2.py](test_ice_tt_resolve_1_tt2.py) — ICE-TT-RESOLVE-1 TT2 critic cells — offline pins on the two-snapshot shape.
- [test_ice_typed_partition_refusals.py](test_ice_typed_partition_refusals.py) — D-X-PARTITIONED-COLDEF refusals beside `test_ice_ddl_alter_2.py` (kept under the 1000-line cap).
- [test_ice_views_1.py](test_ice_views_1.py) — IPI-40 views PR1 battery — the catalog door and the read path.
- [test_ice_views_2_describe.py](test_ice_views_2_describe.py) — IPI-40 views PR2 battery — DESCRIBE on an Iceberg view.
- [test_ice_views_3_alter.py](test_ice_views_3_alter.py) — IPI-40 views PR3 battery — ALTER VIEW on an Iceberg view.
- [test_ice_views_4_showprops.py](test_ice_views_4_showprops.py) — IPI-40 views PR4 battery — SHOW TBLPROPERTIES on an Iceberg view.
- [test_ice_views_5_showcreate.py](test_ice_views_5_showcreate.py) — IPI-40 views PR5 battery — SHOW CREATE TABLE on an Iceberg view.
- [test_ice_views_6_temp.py](test_ice_views_6_temp.py) — IPI-40 views PR6a1 battery — SQL ``CREATE [OR REPLACE] TEMPORARY VIEW``.
- [test_ice_views_6_temp_errors.py](test_ice_views_6_temp_errors.py) — IPI-40 views PR6 error-path battery — every temp-view refusal with its complete text.
- [test_iceberg_hygiene.py](test_iceberg_hygiene.py) — R-ICEBERG-HYGIENE oracles — column-def CREATE + ref DDL time-travel pins.
- [test_iceberg_load_path.py](test_iceberg_load_path.py) — R-DF-LOAD-PATH / R-DF-LOAD-METADATA-JSON — ``format("iceberg").load(<path>)`` reads.
  The stale-hint pin (2026-10-28, ICE-HADOOP-HINT-WINDOW-1): a hint left one version behind a
  committed `DELETE` on a `type=hadoop` table reads the newest committed file on the path door
  and matches the catalog; it is red with the walk reverted.
  pins: ice-hadoop-hint-window-1/C-006
  The gzip-named stale-hint pin (2026-10-29, ICE-HADOOP-GZIP-METADATA-1): the same committed
  `DELETE` on a `type=hadoop` table whose newer metadata file is gzip-named
  (`v<N>.gz.metadata.json`) reads `[2, 3]` on the path door, not the deleted row. It is red on
  the test commit before the fix (`assert [1, 2, 3] == [2, 3]`).
  pins: ice-hadoop-gzip-metadata-1/C-002
  The facade pin is also cited as the merge fact that closes dfload-1 C-011 (dfload-1 C-012, 2026-10-28, merge `9ab67d7d`); the merge fact is not a pin of this file.
  pins: dfload-1/C-012
  The merge fact is dated 2026-10-28 by its correction dfload-1 C-013, cited below so the grammar gate counts it; the merge fact itself is not a test pin of this file.
  pins: dfload-1/C-013
- [test_maintenance_call.py](test_maintenance_call.py) — R-MAINTENANCE-CALL oracle — Spark ``CALL catalog.system.<proc>(…)``.
- [test_maintenance_policy_1.py](test_maintenance_policy_1.py) — `run_maintenance` policy surface: dry-run default, override kwargs, refusal without a policy.
- [test_metadata_tables.py](test_metadata_tables.py) — Oracle — Spark-style Iceberg metadata tables.
- [test_mw5_baseline_delta.py](test_mw5_baseline_delta.py) — MW-5: re-run the MW-0 merge-on-read growth demo and pin the compact delta.
- [test_mw8_runbook.py](test_mw8_runbook.py) — Run the documented maintenance runbook end to end and pin what each step changes.
- [test_partname_1.py](test_partname_1.py) — WO PARTNAME-1: partition-field names stay Spark's on each door.
- [test_rdf_schema_evo_1.py](test_rdf_schema_evo_1.py) — RDF-SCHEMA-EVO-1: rewrite_data_files after schema evolution, pinned against Spark.
- [test_ref_branch_tag_wap.py](test_ref_branch_tag_wap.py) — REF facade pins for Iceberg branches, tags and write-audit-publish.
- [test_rewrite_data_files_options.py](test_rewrite_data_files_options.py) — Rewrite_data_files where / strategy / sort_order facade pins.
- [test_rp3_c009_write_default.py](test_rp3_c009_write_default.py) — RP-3 C-009: fork fills write_default; no PRODUCT engine source sets it.
- [test_rp4_c004_to_branch.py](test_rp4_c004_to_branch.py) — RP-4 C-004: fork to_branch exists; no engine caller sets it.
- [test_show_create_table.py](test_show_create_table.py) — SHOW CREATE TABLE answers Spark's text and error contract on Iceberg tables.
- [test_sql_harden_cutover.py](test_sql_harden_cutover.py) — SQL-HARDEN-1 cutover shapes S1-S7 measured against live Spark.
- [test_tblprops_1.py](test_tblprops_1.py) — WO TBLPROPS-1 battery — ``SHOW TBLPROPERTIES`` on an Iceberg table.
- [test_time_travel.py](test_time_travel.py) — Oracle — snapshot / timestamp / branch / tag reads.
- [test_u11_edge_partition_field.py](test_u11_edge_partition_field.py) — E-CASE-PARTITION-FIELD facade pins: partition sources bind case-sensitively.
- [test_u5_alter_ddl.py](test_u5_alter_ddl.py) — Facade pins for nested struct/list/map-value TYPE promotions read the current Iceberg metadata file and the SQL DESCRIBE type.
- [test_v3_cow_dml.py](test_v3_cow_dml.py) — V3 facade `.sql()` v3 DML pins: UPDATE, sequential COW DELETE, and MERGE keep `_row_id`.
- [test_v3_create_opt_in.py](test_v3_create_opt_in.py) — V3-2: CREATE/CTAS format-version=3 behind ``repark.sql.allowCreateFormatVersion3``.
- [test_v3_dv_compaction.py](test_v3_dv_compaction.py) — V3-5: facade rewrite_data_files drops in-scope v3 deletion vectors.
- [test_v3_dv_container_close.py](test_v3_dv_container_close.py) — RP-7 — the shared-Puffin deletion-vector container close, repark against live Spark.
- [test_v3_legacy_delete_merge.py](test_v3_legacy_delete_merge.py) — V3-12 — a legacy parquet position delete merges into the DV on the next MoR write.
- [test_v3_lineage_columns.py](test_v3_lineage_columns.py) — V3-4: facade serves `_row_id` and `_last_updated_sequence_number` on v3 reads.
- [test_v3_live_file_order.py](test_v3_live_file_order.py) — V3-11 live oracle: one commit's data files take Spark's ascending-partition order.
- [test_v3_live_oracle.py](test_v3_live_oracle.py) — V3E-5 live oracle for Spark-written v3 fixtures.
- [test_v3_multiarg_1.py](test_v3_multiarg_1.py) — V3-MULTIARG-1 — multi-argument partition transforms are DECLARED out of 1.x.
- [test_v3_statement_coverage.py](test_v3_statement_coverage.py) — V3-COV — every served statement class and CALL procedure on a v3 table, repark against Spark.
- [test_v3_upgrade.py](test_v3_upgrade.py) — V3-10: in-place v2 to v3 upgrade through ALTER TABLE on the facade door.
- [test_v3e3_fixtures.py](test_v3e3_fixtures.py) — V3E-3: facade reads of Spark-written partitioned v3 DV and equality-delete fixtures.
- [test_v3e4_refs_time_travel.py](test_v3e4_refs_time_travel.py) — V3E-4: facade refs, time travel over DVs, expire/orphans on format-v3.
- [_record_branch_ops_1.py](_record_branch_ops_1.py) — Spark oracle recorder for ICE-BRANCH-OPS-1.
- [_record_ice_column_reorder_1.py](_record_ice_column_reorder_1.py) — Record the ICE-COLUMN-REORDER-1 Spark oracle into test_ice_column_reorder_1_truth.json.
- [_record_ice_drop_ns_1_oracle.py](_record_ice_drop_ns_1_oracle.py) — Record or re-check the ICE-DROP-NS-1 Spark oracle cells on live PySpark 4.1.2.
- [_record_ice_hadoop_vn_1.py](_record_ice_hadoop_vn_1.py) — Record the ICE-HADOOP-VN-1 Spark oracle and fixture.
- [_record_ice_mixed_case_1.py](_record_ice_mixed_case_1.py) — Record the ICE-MIXED-CASE-1 Spark oracle and stamp the mixed-case fixture warehouse.
- [_record_ice_nan_pushdown_1.py](_record_ice_nan_pushdown_1.py) — Record the ICE-NAN-PUSHDOWN-1 Spark oracle — truth JSON plus fixture warehouses.
- [_record_ice_nested_evo_1.py](_record_ice_nested_evo_1.py) — Record Spark 4.1.2's answers for ICE-NESTED-EVO-1 and the Spark-written nested-evolution tables.
- [_record_ice_page_prune_1.py](_record_ice_page_prune_1.py) — Record the ICE-PAGE-PRUNE-1 Spark oracle — compacted truth plus fixture warehouses.
- [_record_ice_procs_route_1_oracle.py](_record_ice_procs_route_1_oracle.py) — Record or re-check the ICE-PROCS-ROUTE-1 Spark oracle cells on live PySpark.
- [_record_ice_promote_read_1.py](_record_ice_promote_read_1.py) — Records the ICE-PROMOTE-READ-1 Spark oracle (reads and DML after a legal type promotion).
- [_record_ice_rdf_sort_parse_1_oracle.py](_record_ice_rdf_sort_parse_1_oracle.py) — Record the ICE-RDF-SORT-PARSE-1 Spark oracle — in-file row order after a sorted rewrite.
- [_record_ice_replace_columns_1_oracle.py](_record_ice_replace_columns_1_oracle.py) — Record or re-check the ICE-REPLACE-COLUMNS-1 Spark oracle cells on live PySpark 4.1.2.
- [_record_ice_tt_resolve_1_oracle.py](_record_ice_tt_resolve_1_oracle.py) — Record or re-check the ICE-TT-RESOLVE-1 Spark oracle cells on live PySpark.
- [_record_rdf_options_1_oracle.py](_record_rdf_options_1_oracle.py) — Record the Spark 4.1.2 oracle for ICE-RDF-OPTIONS-1 rewrite options cells.
- [_record_rp32_rdf_cow_bytes.py](_record_rp32_rdf_cow_bytes.py) — Record the Spark 4.1.2 oracle for the RP-32 rewrite_data_files keep-live-deletes flip.
- [_record_v3_multiarg_1.py](_record_v3_multiarg_1.py) — Record the V3-MULTIARG-1 Spark oracle — the multi-argument bucket DDL refusal.
- [_v3_statement_coverage_golden.py](_v3_statement_coverage_golden.py) — V3-COV measured matrix — the verdict per program, and the two engine halves it joins.
- [_v3_statement_coverage_programs.py](_v3_statement_coverage_programs.py) — V3-COV inventory — the 81 statement programs and the v3 seeds they run on.
- [_v3_statement_coverage_repark.py](_v3_statement_coverage_repark.py) — V3-COV measured matrix — the repark half, recorded 2026-09-03.
- [_v3_statement_coverage_spark.py](_v3_statement_coverage_spark.py) — V3-COV measured matrix — the live Spark 4.1.2 + Iceberg 1.11.0 half, recorded 2026-09-03.
- [branch_ops_1_truth.json](branch_ops_1_truth.json) — Recorded truth read by `test_ice_branch_ops_1.py`.
- [ice_catalog_session_1_oracle.json](ice_catalog_session_1_oracle.json) — Recorded Spark oracle (11 cells) read by `test_ice_catalog_session_1.py`.
- [ice_changelog_1_spark_oracle.json](ice_changelog_1_spark_oracle.json) — Recorded Spark oracle read by `test_ice_changelog_1.py`.
- [ice_drop_ns_1_spark_oracle.json](ice_drop_ns_1_spark_oracle.json) — Recorded Spark oracle read by `test_ice_drop_ns_1.py`; re-derived by `_record_ice_drop_ns_1_oracle.py`.
- [ice_hadoop_vn_1_spark_oracle.json](ice_hadoop_vn_1_spark_oracle.json) — Recorded Spark oracle read by `test_ice_hadoop_vn_1.py`; re-derived by `_record_ice_hadoop_vn_1.py`.
- [ice_mixed_case_1_spark_oracle.json](ice_mixed_case_1_spark_oracle.json) — Recorded Spark oracle (21 cells) read by `test_ice_mixed_case_1.py`; re-derived by `_record_ice_mixed_case_1.py`.
- [ice_nan_pushdown_1_oracle.json](ice_nan_pushdown_1_oracle.json) — Recorded Spark oracle read by `test_ice_nan_pushdown_1.py`; re-derived by `_record_ice_nan_pushdown_1.py`.
- [ice_procs_route_1_spark_oracle.json](ice_procs_route_1_spark_oracle.json) — Recorded Spark oracle read by `test_ice_procs_route_1.py`; re-derived by `_record_ice_procs_route_1_oracle.py`.
- [ice_rdf_options_1_spark_oracle.json](ice_rdf_options_1_spark_oracle.json) — Recorded Spark oracle read by `test_ice_rdf_options_1.py`; re-derived by `_record_rdf_options_1_oracle.py`.
- [ice_rdf_sort_parse_1_spark_oracle.json](ice_rdf_sort_parse_1_spark_oracle.json) — Recorded Spark oracle read by `test_ice_rdf_sort_parse_1.py`; re-derived by `_record_ice_rdf_sort_parse_1_oracle.py`.
- [ice_replace_columns_1_spark_oracle.json](ice_replace_columns_1_spark_oracle.json) — Recorded Spark oracle (34 cells) read by `test_ice_replace_columns_1.py`; re-derived by `_record_ice_replace_columns_1_oracle.py`.
- [ice_rm_deletes_1_spark_oracle.json](ice_rm_deletes_1_spark_oracle.json) — Recorded Spark oracle read by `test_ice_rm_deletes_1.py`.
- [ice_rm_deletes_1_spark_oracle2.json](ice_rm_deletes_1_spark_oracle2.json) — Recorded Spark oracle read by `test_ice_rm_deletes_1.py`.
- [ice_tt_resolve_1_spark_oracle.json](ice_tt_resolve_1_spark_oracle.json) — Recorded Spark oracle (115 cells) read by `test_ice_tt_resolve_1.py`, `test_ice_tt_resolve_1_tt2.py`; re-derived by `_record_ice_tt_resolve_1_oracle.py`.
- [rp32_rdf_cow_bytes_spark_oracle.json](rp32_rdf_cow_bytes_spark_oracle.json) — Recorded Spark oracle (6 cells), kept for the record (no test loads it); re-derived by `_record_rp32_rdf_cow_bytes.py`.
- [v3_multiarg_1_spark_oracle.json](v3_multiarg_1_spark_oracle.json) — Recorded Spark oracle read by `test_v3_multiarg_1.py`; re-derived by `_record_v3_multiarg_1.py`.

## Path expressions

The move put every file here one level below the old flat directory. These files had a
path expression repointed in the same unit and no other line changed:

| file | change |
|---|---|
| `_record_branch_ops_1.py` | names its own or a sibling's new path in a docstring or recorded string; puts the tests root on `sys.path` for its root-helper import when run as a script. |
| `_record_ice_column_reorder_1.py` | its usage block sets `PYTHONPATH=python/repark/tests` for the root-helper import; names its own or a sibling's new path in a docstring or recorded string. |
| `_record_ice_drop_ns_1_oracle.py` | names its own or a sibling's new path in a docstring or recorded string; puts the tests root on `sys.path` for its root-helper import when run as a script. |
| `_record_ice_hadoop_vn_1.py` | names its own or a sibling's new path in a docstring or recorded string; puts the tests root on `sys.path` for its root-helper import when run as a script; repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `_record_ice_mixed_case_1.py` | names its own or a sibling's new path in a docstring or recorded string; repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `_record_ice_nan_pushdown_1.py` | names its own or a sibling's new path in a docstring or recorded string; reads the root `fixtures/` one level up. |
| `_record_ice_nested_evo_1.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `_record_ice_page_prune_1.py` | its usage block sets `PYTHONPATH=python/repark/tests` for the root-helper import; names its own or a sibling's new path in a docstring or recorded string; reads the root `fixtures/` one level up. |
| `_record_ice_procs_route_1_oracle.py` | names its own or a sibling's new path in a docstring or recorded string; puts the tests root on `sys.path` for its root-helper import when run as a script. |
| `_record_ice_promote_read_1.py` | puts the tests root on `sys.path` for its root-helper import when run as a script; repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `_record_ice_rdf_sort_parse_1_oracle.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `_record_ice_replace_columns_1_oracle.py` | names its own or a sibling's new path in a docstring or recorded string; puts the tests root on `sys.path` for its root-helper import when run as a script. |
| `_record_ice_tt_resolve_1_oracle.py` | names its own or a sibling's new path in a docstring or recorded string; puts the tests root on `sys.path` for its root-helper import when run as a script. |
| `_record_rdf_options_1_oracle.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `_record_v3_multiarg_1.py` | names its own or a sibling's new path in a docstring or recorded string; puts the tests root on `sys.path` for its root-helper import when run as a script. |
| `test_ice_hadoop_vn_1.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_ice_mixed_case_1.py` | names its own or a sibling's new path in a docstring or recorded string; repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_ice_nan_pushdown_1.py` | reads the root `fixtures/` one level up. |
| `test_ice_page_prune_1.py` | reads the root `fixtures/` one level up. |
| `test_mw8_runbook.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_rp3_c009_write_default.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_rp4_c004_to_branch.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_sql_harden_cutover.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_v3_lineage_columns.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_v3_live_oracle.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_v3e3_fixtures.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_v3e4_refs_time_travel.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |

## Pointers

- Up: [../map.md](../map.md)
