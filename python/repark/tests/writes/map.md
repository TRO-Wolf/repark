# map — python/repark/tests/writes

## Purpose

The Iceberg write path: INSERT, INSERT OVERWRITE, CTAS/RTAS, MERGE, UPDATE and DELETE on both SQL doors, the DataFrame writers (`write`, `writeTo`), write options and session write confs, distribution and ordering, store assignment, and concurrent commits.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_a13_ctas_fallback.py](test_a13_ctas_fallback.py) — Location-less CTAS under ``register_memory_catalog`` writes under the catalog warehouse.
- [test_cast_overflow_insert_1.py](test_cast_overflow_insert_1.py) — CAST-OVERFLOW-INSERT-1: storing an overflowing value replays the recorded Spark cells on the SQL and DataFrame doors.
- [test_ctas_division_writeback.py](test_ctas_division_writeback.py) — Facade write-path pins for CTAS integer-division type derivation.
- [test_ctas_view_typed.py](test_ctas_view_typed.py) — Unpartitioned CTAS from a parquet-read temp view (Utf8View batches).
- [test_dml_b_partition_overwrite.py](test_dml_b_partition_overwrite.py) — Facade SQL INSERT OVERWRITE PARTITION pins (DML-B).
- [test_dml_c_truncate.py](test_dml_c_truncate.py) — DML-C: facade and ANSI-callable TRUNCATE TABLE.
- [test_dml_subquery_parity.py](test_dml_subquery_parity.py) — DELETE/UPDATE subquery-predicate differential rows (defect G3-E8).
- [test_enc_1.py](test_enc_1.py) — ENC-1 — the DataFrame writers refuse a table carrying ``encryption.key-id``.
- [test_ice_array_insert_1.py](test_ice_array_insert_1.py) — ICE-ARRAY-INSERT-1 — inserts into array columns answer Spark 4.1.2 on every door.
- [test_ice_avro_name_1.py](test_ice_avro_name_1.py) — ICE-AVRO-NAME-1 — partition columns whose names are not valid Avro names write and read back.
- [test_ice_dyn_overwrite_1.py](test_ice_dyn_overwrite_1.py) — Dynamic ``partitionOverwriteMode`` pins (ICE-DYN-OVERWRITE-1).
- [test_ice_dyn_overwrite_1_by_name.py](test_ice_dyn_overwrite_1_by_name.py) — ``partitionOverwriteMode`` pins for BY NAME and column-list overwrites (ICE-DYN-OVERWRITE-1).
- [test_ice_evo_dml_1.py](test_ice_evo_dml_1.py) — ICE-EVO-DML-1 — DML after ADD / RENAME COLUMN with no write since answers Spark.
- [test_ice_list_null_1.py](test_ice_list_null_1.py) — ICE-LIST-NULL-1: DELETE and UPDATE with IS NULL on nested columns answer Spark.
- [test_ice_merge_append_1.py](test_ice_merge_append_1.py) — ICE-MERGE-APPEND-1 pins: an INSERT commits through ``merge_append``.
- [test_ice_merge_schema_1.py](test_ice_merge_schema_1.py) — IPI-19 + IPI-56 + IPI-37 — schema evolution on write.
- [test_ice_meta_delete_1.py](test_ice_meta_delete_1.py) — ICE-META-DELETE-1 — a DELETE that covers whole data files deletes the files, on both modes.
- [test_ice_occ_scoped_1.py](test_ice_occ_scoped_1.py) — ICE-OCC-SCOPED-1 — concurrent Iceberg DML storms commit and refuse exactly where Spark does.
- [test_ice_orc_avro_1.py](test_ice_orc_avro_1.py) — IPI-41 — ORC and Avro data files: write, read, DELETE / UPDATE / MERGE.
- [test_ice_overwrite_mode_1.py](test_ice_overwrite_mode_1.py) — ICE-OVERWRITE-MODE-1 — Spark's overwrite partition set on every facade overwrite door.
- [test_ice_overwrite_mode_1_transform.py](test_ice_overwrite_mode_1_transform.py) — ICE-OVERWRITE-MODE-1 — ``writeTo(t).overwritePartitions()`` on specs that hold a transform.
- [test_ice_rowid_order_1.py](test_ice_rowid_order_1.py) — ICE-ROWID-ORDER-1: one statement's v3 row ids are deterministic.
- [test_ice_rtas_byname_1.py](test_ice_rtas_byname_1.py) — ICE-RTAS-BYNAME-1 — ``INSERT … BY NAME`` on the Spark door, plus the RTAS operation pins.
- [test_ice_session_write_conf_1.py](test_ice_session_write_conf_1.py) — ICE-SESSION-WRITE-CONF-1 — session ``spark.sql.iceberg.*`` write confs reach every write.
- [test_ice_session_write_conf_1_paths.py](test_ice_session_write_conf_1_paths.py) — ICE-SESSION-WRITE-CONF-1 paths — the session write confs on every remaining door.
- [test_ice_sorted_insert_1.py](test_ice_sorted_insert_1.py) — ICE-SORTED-INSERT-1 — plain INSERT sorts and stamps per the declared order.
- [test_ice_sorted_insert_2.py](test_ice_sorted_insert_2.py) — ICE-SORTED-INSERT-1 round 3 — RePark-owned rewrites sort and stamp like Spark.
- [test_ice_spark_table_1.py](test_ice_spark_table_1.py) — ICE-SPARK-TABLE-1 — RePark writes into a Spark-created Iceberg v2 CoW table.
- [test_ice_struct_pairing_1.py](test_ice_struct_pairing_1.py) — ICE-STRUCT-PAIRING-1: struct values pair with the target struct the way Spark does on UPDATE / MERGE / INSERT.
- [test_ice_struct_pairing_1_exception.py](test_ice_struct_pairing_1_exception.py) — ICE-STRUCT-PAIRING-1 exception round: every recorded update-from-subquery cell answers or refuses like Spark.
- [test_ice_struct_pairing_1_round3.py](test_ice_struct_pairing_1_round3.py) — ICE-STRUCT-PAIRING-1 round 3: the recorded groups hold every cell of the round-3 recorder and answer like Spark.
- [test_ice_v3_write_default_1.py](test_ice_v3_write_default_1.py) — ICE-V3-WRITE-DEFAULT-1 — omitted columns fill from ``write_default`` on every write path.
- [test_ice_wap_branch_1.py](test_ice_wap_branch_1.py) — ICE-WAP-BRANCH-1 — ``spark.wap.branch`` redirects writes and reads to an audit branch.
- [test_ice_write_df_1.py](test_ice_write_df_1.py) — U7 PR1 — the DataFrame writer surface: ``save(name)``, ``bucketBy`` and ``output-spec-id``.
- [test_ice_write_df_1_edges.py](test_ice_write_df_1_edges.py) — U7 PR1 round 2 — Spark's answers for the writer shapes the first review found unpinned.
- [test_ice_write_df_2.py](test_ice_write_df_2.py) — U7 PR2 — DataFrame writer semantics: saveAsTable overwrite, the branch option, overwrite().
- [test_ice_write_df_2_doors.py](test_ice_write_df_2_doors.py) — U7 PR2 slice-2 round 3 — `isolation-level` door by door, and nested binding residues.
- [test_ice_write_df_2_overwrite.py](test_ice_write_df_2_overwrite.py) — U7 PR2 slice 2 — ``DataFrameWriterV2.overwrite(condition)`` is Spark's overwrite by filter.
- [test_ice_write_options_1.py](test_ice_write_options_1.py) — ICE-WRITE-OPTIONS-1 — DataFrame write options on Iceberg writes.
- [test_ice_write_options_1_rebase.py](test_ice_write_options_1_rebase.py) — ICE-WRITE-OPTIONS-1 run 22b — write options across the paths main added under them.
- [test_ice_write_options_rp_1.py](test_ice_write_options_rp_1.py) — ICE-WRITE-OPTIONS-RP-1 — a caller ``replace-partitions`` summary value wins.
- [test_ice_write_refusals_1.py](test_ice_write_refusals_1.py) — U6 WRITE-REFUSALS — refusal parity on `write.spark.accept-any-schema` tables.
- [test_ice_write_sql_1.py](test_ice_write_sql_1.py) — U8 WRITE-SQL PR1 and PR2 — REPLACE WHERE, INSERT PARTITION, bucketed INSERT, nested SET.
- [test_insert_store_assign.py](test_insert_store_assign.py) — Non-MERGE write-path ANSI store-assignment gate (WI-1) — the `INSERT OVERWRITE` door.
- [test_io_bucket_cluster_1.py](test_io_bucket_cluster_1.py) — io-bucket-cluster-1 pins: the bucketBy / sortBy / clusterBy writer surface.
- [test_merge_differential_parity.py](test_merge_differential_parity.py) — MERGE INTO differential rows (gap G3, record-side) — post-MERGE table vs live Spark.
- [test_merge_insert_scope.py](test_merge_insert_scope.py) — MERGE ``WHEN NOT MATCHED`` source-only resolution scope.
- [test_merge_into.py](test_merge_into.py) — R-MERGEINTO — ``DataFrame.mergeInto`` builder lowers to existing SQL MERGE.
- [test_merge_scan_prune_semantics.py](test_merge_scan_prune_semantics.py) — MERGE scan-prune / residual-probe hardening — M1, M5, M6, M7 (r1 / r2 / r3 / r11).
- [test_merge_semantics_audit.py](test_merge_semantics_audit.py) — MERGE-audit corpus — green pins for gap-map holes verified in the MERGE audit.
- [test_merge_store_assign.py](test_merge_store_assign.py) — MERGE INSERT/UPDATE ANSI store-assignment gate (audit M9 / BL-4, repros r13b/r13c).
- [test_mw9_delete_granularity.py](test_mw9_delete_granularity.py) — MW-9: facade Spark `.sql()` honors `write.delete.granularity`.
- [test_partition_value_audit.py](test_partition_value_audit.py) — Write-path partition-key VALUE audit — CTAS + INSERT vs live Spark 4.1.2 + Iceberg.
- [test_profiles1_table_properties.py](test_profiles1_table_properties.py) — `write.*` table-property pins — REVIEW-FIX-8 (RF-4, Q-40).
- [test_repark_sql_update_subquery_where_1.py](test_repark_sql_update_subquery_where_1.py) — REPARK-SQL-UPDATE-SUBQUERY-WHERE-1: ``repark.sql()`` refuses a subquery in a DML WHERE.
- [test_repark_sql_update_subquery_where_1_spark_door.py](test_repark_sql_update_subquery_where_1_spark_door.py) — REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 on the Spark SQL door: a wrapped subquery DML refuses.
- [test_update_set_subquery_walk_1.py](test_update_set_subquery_walk_1.py) — UPDATE-SET-SUBQUERY-WALK-1: ``repark.sql()`` refuses a scalar subquery anywhere in SET.
- [test_write_body_carriers_1.py](test_write_body_carriers_1.py) — WRITE-BODY-CARRIERS-1 build battery — a write body inside CTAS refuses at parse.
- [test_write_distribution_1.py](test_write_distribution_1.py) — WRITE-DISTRIBUTION-1 — the hash distribution rule before a partitioned Iceberg write.
- [test_write_distribution_2.py](test_write_distribution_2.py) — WRITE-DISTRIBUTION-2 — the hash distribution rule on the stream write paths.
- [test_write_order_dist_1.py](test_write_order_dist_1.py) — WRITE-ORDER-DIST-1 — ALTER TABLE WRITE ORDERED/DISTRIBUTED BY and the writes they shape.
- [test_writer_v2.py](test_writer_v2.py) — Facade tests for DataFrameWriterV2 (writeTo), path parquet, sortWithinPartitions, partition transforms, and F.weekday.
- [_record_dml_subquery_goldens.py](_record_dml_subquery_goldens.py) — Record mode for the G3-E8 DML-subquery corpus — re-derive every Spark half from live Iceberg.
- [_record_ice_append_storm_1.py](_record_ice_append_storm_1.py) — Record the ICE-APPEND-RETRY-1 insert-storm Spark oracle.
- [_record_ice_array_insert_1.py](_record_ice_array_insert_1.py) — Record Spark 4.1.2's answers for ICE-ARRAY-INSERT-1 — inserts into array columns.
- [_record_ice_avro_name_1_oracle.py](_record_ice_avro_name_1_oracle.py) — Record or re-check the ICE-AVRO-NAME-1 Spark oracle cells on live PySpark 4.1.2.
- [_record_ice_evo_dml_1.py](_record_ice_evo_dml_1.py) — Records the ICE-EVO-DML-1 Spark oracle (DML after ADD / RENAME COLUMN).
- [_record_ice_list_null_1.py](_record_ice_list_null_1.py) — Record Spark 4.1.2's answers for ICE-LIST-NULL-1: DELETE and UPDATE with IS NULL.
- [_record_ice_meta_delete_1.py](_record_ice_meta_delete_1.py) — Record Spark 4.1.2's answers for ICE-META-DELETE-1 — DELETE that covers whole data files.
- [_record_ice_overwrite_mode_1_oracle.py](_record_ice_overwrite_mode_1_oracle.py) — Record or re-check the ICE-OVERWRITE-MODE-1 Spark oracle cells on live PySpark.
- [_record_ice_overwrite_mode_1_transform.py](_record_ice_overwrite_mode_1_transform.py) — Record or re-check the ICE-OVERWRITE-MODE-1 transform-spec cells on live PySpark 4.1.2.
- [_record_ice_rowid_order_1.py](_record_ice_rowid_order_1.py) — Record Spark 4.1.2's answers for ICE-ROWID-ORDER-1: v3 row-id assignment order.
- [_record_ice_rtas_byname_1_oracle.py](_record_ice_rtas_byname_1_oracle.py) — Record or re-check the ICE-RTAS-BYNAME-1 Spark oracle cells on live PySpark 4.1.2.
- [_record_ice_session_write_conf_1_oracle.py](_record_ice_session_write_conf_1_oracle.py) — Record or re-check the ICE-SESSION-WRITE-CONF-1 Spark oracle cells on live PySpark.
- [_record_ice_session_write_conf_1_paths.py](_record_ice_session_write_conf_1_paths.py) — Record the ICE-SESSION-WRITE-CONF-1 path cells (QS / QZ / QR) on live PySpark.
- [_record_ice_session_write_conf_1_rounds.py](_record_ice_session_write_conf_1_rounds.py) — Record the ICE-SESSION-WRITE-CONF-1 round-3 and round-4 Spark cells.
- [_record_ice_sorted_insert_1_oracle.py](_record_ice_sorted_insert_1_oracle.py) — Record the ICE-SORTED-INSERT-1 Spark oracle — per-file sortedness truth JSON.
- [_record_ice_sorted_insert_2_oracle.py](_record_ice_sorted_insert_2_oracle.py) — Record the ICE-SORTED-INSERT-2 Spark oracle — rewrite, binpack and NaN sort cells.
- [_record_ice_struct_pairing_1.py](_record_ice_struct_pairing_1.py) — Record the ICE-STRUCT-PAIRING-1 door x shape matrix on Spark 4.1.2 or on a RePark module.
- [_record_ice_struct_pairing_1_exception.py](_record_ice_struct_pairing_1_exception.py) — Records the ICE-STRUCT-PAIRING-1 exception round on Spark.
- [_record_ice_struct_pairing_1_round3.py](_record_ice_struct_pairing_1_round3.py) — Record the ICE-STRUCT-PAIRING-1 build-round-3 cells on Spark 4.1.2 or on a RePark module.
- [_record_ice_wap_branch_1_oracle.py](_record_ice_wap_branch_1_oracle.py) — Record or re-check the ICE-WAP-BRANCH-1 Spark oracle cells on live PySpark 4.1.2.
- [_record_ice_write_options_1_oracle.py](_record_ice_write_options_1_oracle.py) — Record the Spark oracle for ICE-WRITE-OPTIONS-1 (DataFrame write options).
- [_record_ice_write_options_2_oracle.py](_record_ice_write_options_2_oracle.py) — Follow-up Spark probes for ICE-WRITE-OPTIONS-1 (appends cells P2-* to the fixture).
- [_record_ice_write_options_3_oracle.py](_record_ice_write_options_3_oracle.py) — Snapshot-property collision cells for ICE-WRITE-OPTIONS-1 (appends COLL-* to the fixture).
- [_record_ice_write_options_rp_1.py](_record_ice_write_options_rp_1.py) — Record Spark 4.1.2's answers for ICE-WRITE-OPTIONS-RP-1 — caller summary on replace commits.
- [_record_merge_differential_goldens.py](_record_merge_differential_goldens.py) — Record mode for the MERGE INTO differential corpus — re-derive Spark halves from live Iceberg.
- [_record_partition_value_goldens.py](_record_partition_value_goldens.py) — Record mode for the V-4 partition-value corpus — re-derive every Spark half from live Iceberg.
- [_record_write_body_carriers_1_main.py](_record_write_body_carriers_1_main.py) — Record main's answers for CTAS statements whose body is a write, and the wrappers.
- [_record_write_body_carriers_1_spark.py](_record_write_body_carriers_1_spark.py) — Record live Spark 4.1.2 answers for CTAS statements whose body is a write.
- [cast_overflow_insert_1_spark_oracle.json](cast_overflow_insert_1_spark_oracle.json) — Recorded Spark oracle (98 cells) read by `test_cast_overflow_insert_1.py`.
- [ice_avro_name_1_spark_oracle.json](ice_avro_name_1_spark_oracle.json) — Recorded Spark oracle (20 cells) read by `test_ice_avro_name_1.py`; re-derived by `_record_ice_avro_name_1_oracle.py`.
- [ice_merge_append_1_truth.json](ice_merge_append_1_truth.json) — Recorded truth read by `test_ice_merge_append_1.py`.
- [ice_meta_delete_1_spark_oracle.json](ice_meta_delete_1_spark_oracle.json) — Recorded Spark oracle (72 cells) read by `test_ice_meta_delete_1.py`; re-derived by `_record_ice_meta_delete_1.py`.
- [ice_overwrite_mode_1_spark_oracle.json](ice_overwrite_mode_1_spark_oracle.json) — Recorded Spark oracle (96 cells) read by `test_ice_overwrite_mode_1.py`; re-derived by `_record_ice_overwrite_mode_1_oracle.py`.
- [ice_overwrite_mode_1_transform_spark_oracle.json](ice_overwrite_mode_1_transform_spark_oracle.json) — Recorded Spark oracle (16 cells) read by `test_ice_overwrite_mode_1_transform.py`; re-derived by `_record_ice_overwrite_mode_1_transform.py`.
- [ice_rtas_byname_1_spark_oracle.json](ice_rtas_byname_1_spark_oracle.json) — Recorded Spark oracle read by `test_ice_rtas_byname_1.py`; re-derived by `_record_ice_rtas_byname_1_oracle.py`.
- [ice_session_write_conf_1_spark_oracle.json](ice_session_write_conf_1_spark_oracle.json) — Recorded Spark oracle (106 cells) read by `test_ice_session_write_conf_1.py`, `test_ice_session_write_conf_1_paths.py`; re-derived by `_record_ice_session_write_conf_1_oracle.py`.
- [ice_sorted_insert_1_spark_oracle.json](ice_sorted_insert_1_spark_oracle.json) — Recorded Spark oracle (8 cells) read by `test_ice_sorted_insert_1.py`; re-derived by `_record_ice_sorted_insert_1_oracle.py`.
- [ice_sorted_insert_2_spark_oracle.json](ice_sorted_insert_2_spark_oracle.json) — Recorded Spark oracle (15 cells) read by `test_ice_sorted_insert_2.py`; re-derived by `_record_ice_sorted_insert_2_oracle.py`.
- [ice_struct_pairing_1_main.json](ice_struct_pairing_1_main.json) — RePark recording read by the three `test_ice_struct_pairing_1*.py` files; re-derived by `_record_ice_struct_pairing_1.py` and its round-3 twin.
- [ice_struct_pairing_1_spark_oracle.json](ice_struct_pairing_1_spark_oracle.json) — Recorded Spark oracle read by the three `test_ice_struct_pairing_1*.py` files; re-derived by `_record_ice_struct_pairing_1.py` and its round-3 twin.
- [ice_wap_branch_1_spark_oracle.json](ice_wap_branch_1_spark_oracle.json) — Recorded Spark oracle (27 cells) read by `test_ice_wap_branch_1.py`; re-derived by `_record_ice_wap_branch_1_oracle.py`.
- [ice_write_df_1_spark_oracle.json](ice_write_df_1_spark_oracle.json) — The U7 writer oracle: recorded scoreboard observations and each probe shape's measured error or state; read by the `test_ice_write_df_*.py` files and `test_io_bucket_cluster_1.py`.
- [ice_write_options_1_spark_oracle.json](ice_write_options_1_spark_oracle.json) — Recorded Spark oracle (52 cells) read by `test_ice_write_options_1.py`; re-derived by the three `_record_ice_write_options_*_oracle.py` recorders.
- [u8_write_sql_nested_spark_oracle.json](u8_write_sql_nested_spark_oracle.json) — Recorded Spark oracle read by `test_ice_write_sql_1.py`.
- [u8_write_sql_spark_oracle.json](u8_write_sql_spark_oracle.json) — Recorded Spark oracle read by `test_ice_write_sql_1.py`.

## Pointers

- Up: [../map.md](../map.md)
