# v3-2-create-v3-opt-in pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: none (records)
C-002: crates/repark-spark/src/tests/create_table.rs, crates/repark-spark/src/tests/ctas.rs
C-003: crates/repark-spark/src/tests/ctas.rs::ctas_format_version_two_consumed_others_rejected, crates/repark-sql/src/properties/tests.rs, crates/repark-sql/src/tests.rs::with_format_version_sets_the_table_format_version
C-004: crates/repark-spark/src/tests/call_v3.rs::the_engine_still_cannot_produce_a_v3_table, crates/repark-spark/src/tests/create_table.rs, crates/repark-spark/src/tests/ctas.rs
C-005: crates/repark-spark/src/tests/call_v3.rs, crates/repark-spark/src/tests/create_table.rs, crates/repark-spark/src/tests/ctas.rs
C-006: crates/repark-sql/src/properties/tests.rs
C-007: crates/repark-spark/src/tests/create_format_version_one.rs::merge_on_read_row_level_writes_on_a_v1_table_refuse_like_spark, crates/repark-spark/src/tests/ctas.rs::ctas_format_version_two_consumed_others_rejected, crates/repark-sql/src/tests.rs::with_format_version_sets_the_table_format_version
C-008: crates/repark-spark/src/tests/call_v3.rs::the_engine_still_cannot_produce_a_v3_table
C-010: python/repark/tests/iceberg/test_v3_create_opt_in.py
C-011: crates/repark-spark/src/tests/call_v3.rs, python/repark/tests/iceberg/test_v3_create_opt_in.py
C-012: none (records)
C-013: none (records)
C-015: crates/repark-spark/src/tests/call_v3.rs
