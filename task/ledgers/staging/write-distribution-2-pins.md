# write-distribution-2 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-iceberg/src/write/distribution/tests.rs::stream_path_lands_one_partition_value_in_one_writer, python/repark/tests/writes/test_write_distribution_2.py::test_stream_write_commits_one_data_file_per_partition_value, scripts/check_rust_file_size.py
C-002: crates/repark-iceberg/src/write/distribution/tests.rs::truncate_over_a_view_typed_string_keys_on_the_cast_value, python/repark/tests/writes/test_write_distribution_2.py
C-003: python/repark/tests/writes/test_write_distribution_2.py
C-004: crates/repark-iceberg/src/write/distribution/tests.rs::merge_inserts_into_a_partitioned_table_route_one_value_to_one_writer, crates/repark-iceberg/src/write/distribution/tests.rs::stream_path_distribution_is_deterministic_across_runs
C-005: crates/repark-iceberg/src/write/distribution/tests.rs::stream_path_null_partition_values_share_one_writer, crates/repark-iceberg/src/write/distribution/tests.rs::stream_path_two_field_spec_keys_on_the_transform_value
C-006: crates/repark-iceberg/src/write/distribution/tests.rs::a_late_failure_into_a_partitioned_table_leaves_no_data_file
C-007: none (records)
C-008: crates/repark-iceberg/src/write/distribution/tests.rs
