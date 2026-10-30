# dml-c-truncate pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/truncate.rs, crates/repark-sql/src/truncate_tests.rs
C-002: crates/repark-spark/src/tests/truncate.rs::truncate_table_wipes_rows_stamps_delete_and_preserves_history
C-003: crates/repark-sql/src/truncate_tests.rs::truncate_table_wipes_rows_stamps_delete_and_preserves_history
C-004: python/repark/tests/writes/test_dml_c_truncate.py
C-005: crates/repark-spark/src/tests/truncate.rs::empty_insert_overwrite_still_wipes_and_stamps_delete, crates/repark-spark/src/tests/truncate.rs::truncate_and_empty_overwrite_stamp_equal_wipe_summary_keys
C-006: crates/repark-spark/src/tests/truncate.rs, crates/repark-sql/src/truncate_tests.rs, python/repark/tests/writes/test_dml_c_truncate.py
C-007: crates/repark-spark/src/tests/truncate.rs, crates/repark-sql/src/truncate_tests.rs, python/repark/tests/writes/test_dml_c_truncate.py
C-008: none (records)
