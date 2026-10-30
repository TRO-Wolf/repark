# v3-12-legacy-delete-merge pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/v3_upgrade.rs
C-002: crates/repark-spark/src/tests/v3_legacy_delete.rs::measure_pure_dv_close_cost
C-003: crates/repark-spark/src/tests/v3_legacy_delete.rs::measure_pure_dv_close_cost
C-004: crates/repark-spark/src/tests/v3_legacy_delete.rs::measure_pure_dv_close_cost, python/repark/tests/iceberg/test_v3_legacy_delete_merge.py::_live_session, python/repark/tests/iceberg/test_v3_legacy_delete_merge.py::_repark_legacy_merge_shape, python/repark/tests/iceberg/test_v3_legacy_delete_merge.py::_spark_legacy_merge_shape, python/repark/tests/iceberg/test_v3_legacy_delete_merge.py::test_partition_scoped_legacy_delete_matches_spark, python/repark/tests/iceberg/test_v3_legacy_delete_merge.py::test_plain_where_mor_delete_over_a_legacy_parquet_delete_matches_spark
C-005: crates/repark-spark/src/tests/v3_legacy_delete.rs::measure_pure_dv_close_cost
C-006: crates/repark-spark/src/tests/v3_legacy_delete.rs::a_legacy_parquet_delete_that_exists_only_on_a_branch_merges_on_that_branch, crates/repark-spark/src/tests/v3_legacy_delete.rs::a_second_merge_on_read_delete_on_a_diverged_branch_merges_the_branch_only_dv, crates/repark-spark/src/tests/v3_legacy_delete.rs::measure_pure_dv_close_cost
C-007: crates/repark-spark/src/tests/v3_legacy_delete.rs::measure_pure_dv_close_cost
