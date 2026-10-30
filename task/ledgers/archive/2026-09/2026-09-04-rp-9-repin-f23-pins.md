# rp-9-repin-f23 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-iceberg/src/write/merge/tests/dv_close.rs::closing_a_covered_v3_delete_reads_the_data_manifest_for_sequence_numbers
C-002: crates/repark-iceberg/src/write/merge/tests/dv_close.rs::a_legacy_delete_fills_data_sequence_numbers_even_with_a_complete_partition_map, crates/repark-iceberg/src/write/merge/tests/dv_close.rs::a_supplied_partition_map_closes_a_fresh_partitioned_delete_with_no_data_manifest
C-003: crates/repark-spark/src/tests/v3_legacy_delete.rs::measure_pure_dv_close_cost
C-004: none (records)
C-005: crates/repark-iceberg/src/write/merge/tests/dv_close.rs::a_plain_identity_delete_closes_with_no_data_manifest, crates/repark-iceberg/src/write/predicate_dml/tests/plain.rs, crates/repark-iceberg/src/write/predicate_dml/tests/predicate_dml.rs, crates/repark-spark/src/tests/list_null_compound.rs, crates/repark-spark/src/tests/v3_mor_dml.rs
