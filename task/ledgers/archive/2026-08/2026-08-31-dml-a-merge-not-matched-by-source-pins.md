# dml-a-merge-not-matched-by-source pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/merge_nmbs.rs
C-002: crates/repark-iceberg/src/write/merge/tests/nmbs.rs, crates/repark-spark/src/tests/merge_nmbs.rs::cow_nmbs_delete_only_keeps_matched_target, crates/repark-spark/src/tests/merge_nmbs.rs::mor_nmbs_delete_only_writes_position_deletes, crates/repark-sql/src/merge/nmbs_tests.rs::ansi_cow_and_mor_nmbs_delete_keeps_matched, python/repark/tests/writes/test_merge_differential_parity.py, python/repark/tests/writes/test_merge_into.py::test_merge_into_not_matched_by_source_deletes_unmatched
C-003: crates/repark-iceberg/src/write/merge/tests/nmbs.rs, crates/repark-spark/src/tests/merge_nmbs.rs::cow_and_mor_nmbs_update_rewrites_unmatched, crates/repark-spark/src/tests/merge_nmbs.rs::nmbs_update_store_assignment_reuses_merge_gate, crates/repark-sql/src/merge/nmbs_tests.rs::ansi_nmbs_update_and_three_arms, python/repark/tests/writes/test_merge_into.py
C-004: crates/repark-iceberg/src/write/merge/tests/nmbs.rs, crates/repark-spark/src/tests/merge_nmbs.rs::nmbs_plus_matched_update_dup_source_raises_cardinality, crates/repark-sql/src/merge/nmbs_tests.rs::ansi_source_empty_wipe_and_cardinality
C-005: crates/repark-iceberg/src/write/merge/tests/nmbs.rs, crates/repark-spark/src/tests/merge_nmbs.rs::nmbs_first_match_update_then_delete, crates/repark-spark/src/tests/merge_nmbs.rs::nmbs_unconditional_not_last_raises_spark_class, crates/repark-spark/src/tests/merge_nmbs.rs::three_arms_cow_and_mor_match_spark, crates/repark-sql/src/merge/nmbs_tests.rs
C-006: crates/repark-spark/src/tests/merge_nmbs.rs::source_empty_nmbs_delete_wipes_cow_and_mor, crates/repark-sql/src/merge/nmbs_tests.rs::ansi_source_empty_wipe_and_cardinality
C-007: crates/repark-spark/src/tests/merge_nmbs.rs::mor_nmbs_delete_only_writes_position_deletes
C-008: none (records)
