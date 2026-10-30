# dml-b-insert-overwrite pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/insert_overwrite.rs, crates/repark-spark/src/tests/partition_overwrite.rs, python/repark/tests/writes/test_dml_b_partition_overwrite.py
C-002: crates/repark-spark/src/tests/partition_overwrite.rs, python/repark/tests/writes/test_dml_b_partition_overwrite.py, python/repark/tests/writes/test_writer_v2.py
C-003: python/repark/tests/writes/test_writer_v2.py
C-004: crates/repark-spark/src/tests/insert_overwrite.rs, crates/repark-spark/src/tests/partition_overwrite.rs, python/repark/tests/writes/test_dml_b_partition_overwrite.py, python/repark/tests/writes/test_writer_v2.py
C-005: crates/repark-spark/src/tests/insert_overwrite.rs::empty_insert_overwrite_partition_drops_only_named_partition, crates/repark-spark/src/tests/insert_overwrite.rs::insert_overwrite_partition_nonempty_replaces_only_named_partition, crates/repark-spark/src/tests/partition_overwrite.rs, python/repark/tests/writes/test_dml_b_partition_overwrite.py
C-006: none (records)
