# rp-7-f18-repin pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: none (records)
C-002: crates/repark-iceberg/src/write/merge/tests/occ_partitions.rs, crates/repark-iceberg/src/write/merge/tests/partition_sink.rs::three_concurrent_target_scan_executes_plan_data_manifests_once
C-003: crates/repark-spark/src/tests/v3e4.rs::a_later_single_row_delete_writes_one_blob_not_the_whole_container, crates/repark-spark/src/tests/v3e4.rs::measure_later_single_row_delete_bytes, python/repark/tests/iceberg/test_v3_dv_container_close.py::_dv_entries
C-004: crates/repark-spark/src/tests/v3e4.rs::a_later_single_row_delete_writes_one_blob_not_the_whole_container, crates/repark-spark/src/tests/v3e4.rs::measure_later_single_row_delete_bytes
C-005: crates/repark-spark/src/tests/v3_dml_scan.rs::subquery_dml_matrix_matches_spark_with_the_residual_pushed, python/repark/tests/iceberg/test_v3_dv_container_close.py::_dv_entries, scripts/check_rust_file_size.py
C-006: python/repark/tests/iceberg/test_v3_dv_container_close.py::_dv_entries, python/repark/tests/iceberg/test_v3_live_oracle.py
