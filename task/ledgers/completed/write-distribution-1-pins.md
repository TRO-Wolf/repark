# write-distribution-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-iceberg/src/write/distribution/tests.rs::one_partition_value_lands_in_exactly_one_writer, python/repark/tests/writes/test_write_distribution_1.py::test_partitioned_ctas_writes_one_data_file_per_partition_value
C-002: crates/repark-iceberg/src/write/distribution/tests.rs::the_distribution_is_deterministic_across_runs
C-003: crates/repark-iceberg/src/write/distribution/tests.rs::input_partitions_without_rows_do_not_split_a_value
C-004: crates/repark-iceberg/src/write/distribution/tests.rs::null_partition_values_share_one_writer, python/repark/tests/writes/test_write_distribution_1.py::test_null_partition_value_lands_in_one_data_file
C-005: crates/repark-iceberg/src/write/distribution/tests.rs::bucket_and_day_transforms_key_on_the_transformed_value, python/repark/tests/writes/test_write_distribution_1.py::test_bucket_partitioned_ctas_writes_one_data_file_per_bucket
C-006: crates/repark-iceberg/src/write/distribution/tests.rs::an_unpartitioned_table_keeps_one_file_per_input_partition, python/repark/tests/perf/test_perf_ice_writepath_1.py::test_ctas_writes_one_data_file_per_plan_partition
C-007: python/repark/tests/writes/test_write_distribution_1.py::test_partitioned_ctas_file_count_matches_spark
C-008: crates/repark-iceberg/src/write/distribution/tests.rs
C-009: python/repark/tests/perf/test_perf_ice_writepath_1.py::test_partitioned_ctas_files_ascend_by_partition_value, python/repark/tests/writes/test_write_distribution_1.py::test_partitioned_ctas_writes_one_data_file_per_partition_value
