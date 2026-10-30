# mw-9-delete-granularity pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/call.rs::call_mor2_merge_writes_one_position_delete_per_data_file_by_default, python/repark/tests/writes/test_mw9_delete_granularity.py::test_unset_granularity_writes_one_delete_file_per_data_file
C-002: crates/repark-spark/src/tests/delete_granularity.rs::explicit_file_granularity_matches_the_unset_default
C-003: crates/repark-spark/src/tests/delete_granularity.rs::explicit_partition_granularity_writes_one_delete_file
C-004: crates/repark-iceberg/src/write/predicate_dml/tests/update.rs, crates/repark-spark/src/tests/delete_granularity.rs::unknown_delete_granularity_refuses_before_any_write
C-005: crates/repark-spark/src/tests/delete_granularity.rs::fork_table_provider_delete_is_not_this_writer
C-006: crates/repark-spark/src/tests/call.rs, crates/repark-spark/src/tests/delete_granularity.rs
C-007: crates/repark-spark/src/tests/delete_granularity.rs::alter_set_granularity_is_honored_on_the_next_merge
C-008: crates/repark-iceberg/src/write/merge/tests/streaming_scan.rs::mor_props, python/repark/tests/iceberg/test_mw5_baseline_delta.py::MOR_PROPERTIES
C-009: crates/repark-spark/src/tests/call.rs, crates/repark-spark/src/tests/delete_granularity.rs
C-010: python/repark/tests/writes/test_mw9_delete_granularity.py
C-011: crates/repark-spark/src/tests/delete_granularity.rs
