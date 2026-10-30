# row-lineage-order-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-iceberg/src/write/fanout_order_tests.rs::java_struct_hash_matches_java_goldens
C-002: crates/repark-iceberg/src/write/fanout_order_tests.rs::spark_murmur3_matches_spark_hash
C-003: crates/repark-iceberg/src/write/fanout_order_tests.rs::order_matches_recorded_spark_at_shuffle_4
C-004: crates/repark-iceberg/src/write/fanout_order_tests.rs::ties_follow_the_shuffle_reducer
C-005: crates/repark-iceberg/src/write/fanout_order_tests.rs::shuffle_1_matches_the_registry_rows
C-006: crates/repark-iceberg/src/write/fanout_order_tests.rs::falls_back_to_input_order
C-007: crates/repark-iceberg/src/write/fanout_order_tests.rs::shuffle_partitions_conf
C-008: crates/repark-spark/src/tests/v3_fanout_order.rs::row_id_v3_cell_answers_spark
C-009: crates/repark-spark/src/tests/v3_fanout_order.rs::seven_keys_take_spark_file_order
C-010: crates/repark-spark/src/tests/v3_fanout_order.rs::bucket_tie_follows_the_configured_reducer
C-011: crates/repark-spark/src/tests/v3_fanout_order.rs::unpartitioned_insert_keeps_its_order
C-012: crates/repark-spark/src/tests/v3_fanout_order.rs::sorted_table_keeps_ascending
C-013: python/repark/tests/writes/test_ice_rowid_order_1.py::test_eight_category_mapping_is_one_and_matches_spark
