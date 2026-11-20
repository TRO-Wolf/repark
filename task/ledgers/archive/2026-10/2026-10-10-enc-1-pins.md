# enc-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/enc_1.rs
C-002: crates/repark-spark/src/tests/enc_1.rs
C-003: crates/repark-spark/src/tests/enc_1.rs
C-004: crates/repark-spark/src/tests/enc_1.rs
C-005: crates/repark-iceberg/src/write/encryption_tests.rs, crates/repark-spark/src/tests/enc_1.rs, crates/repark-spark/src/tests/v3_lineage.rs, python/repark/tests/writes/test_enc_1.py
C-006: none (records)
C-007: crates/repark-core/src/microbatch/run_tests.rs::a_keyed_sink_refuses_the_batch_and_stages_no_file, crates/repark-spark/src/tests/enc_1_fold.rs, python/repark/tests/writes/test_enc_1.py
C-008: crates/repark-iceberg/src/catalog/tests/encryption_guard.rs, crates/repark-spark/src/tests/enc_1_fold.rs
C-009: crates/repark-iceberg/src/tests/enc_1_gate.rs, crates/repark-spark/src/tests/enc_1_fold.rs
C-010: crates/repark-iceberg/src/catalog/tests/encryption_guard.rs, crates/repark-spark/src/tests/enc_1_fold.rs::keyed_table_with_hostile_metadata_text_still_alters_expires_and_unsets
