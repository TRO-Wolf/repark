# ice-procs-route-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/iceberg/test_ice_procs_route_1.py
C-002: python/repark/tests/iceberg/test_ice_procs_route_1.py
C-003: python/repark/tests/iceberg/test_ice_procs_route_1.py::test_live_oracle_matches_recorded
C-004: crates/repark-spark/src/tests/call_procs_route_1.rs::call_ancestors_of_honors_older_snapshot_id, crates/repark-spark/src/tests/call_procs_route_1.rs::call_ancestors_of_walks_newest_first_with_snapshot_timestamps, python/repark/tests/iceberg/test_ice_procs_route_1.py
C-005: crates/repark-spark/src/tests/call_procs_route_1.rs::call_ancestors_of_names_missing_snapshots_like_spark, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_ancestors_empty, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_ancestors_missing_id
C-006: crates/repark-spark/src/tests/call_procs_route_1.rs::call_compute_table_stats_registers_older_snapshot_id, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_default, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_snapshot, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_twice
C-007: crates/repark-spark/src/tests/call_procs_route_1.rs::call_compute_table_stats_registers_blobs_in_caller_order, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_columns, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_columns_two, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_duplicate
C-008: python/repark/tests/iceberg/test_ice_procs_route_1.py::check_refusal, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_empty_array, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_struct_arg, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_unknown_column
C-009: python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_nested, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_types
C-010: python/repark/tests/iceberg/test_ice_procs_route_1.py::test_table_stats_empty
C-011: crates/repark-spark/src/tests/call_procs_route_1.rs::call_compute_partition_stats_registers_older_snapshot_id, python/repark/tests/iceberg/test_ice_procs_route_1.py
C-012: python/repark/tests/iceberg/test_ice_procs_route_1.py::test_partition_stats_unpartitioned
C-013: crates/repark-spark/src/tests/call_procs_route_1.rs::call_rewrite_table_path_stages_manifests_lists_and_answers_sparks_counts, python/repark/tests/iceberg/test_ice_procs_route_1.py
C-014: python/repark/tests/iceberg/test_ice_procs_route_1.py::test_rewrite_path_no_file_list
C-015: python/repark/tests/iceberg/test_ice_procs_route_1.py::test_rewrite_path_missing_prefix
C-016: crates/repark-spark/src/tests/call_procs_route_1.rs::call_rewrite_table_path_version_range_refuses_naming_the_fork_gap, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_rewrite_path_end_version, python/repark/tests/iceberg/test_ice_procs_route_1.py::test_rewrite_path_start_version
