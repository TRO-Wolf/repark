# ice-merge-append-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark-parity/tests/test_cap_1_source_file_line_cap.py, scripts/check_rust_file_size.py
C-002: crates/repark-iceberg/src/tests/merge_append_series.rs::defaults_series_matches_spark, python/repark/tests/writes/test_ice_merge_append_1.py::test_defaults_collapse_to_one_manifest_at_the_hundredth_append
C-003: crates/repark-iceberg/src/tests/merge_append_series.rs::min_count_to_merge_series_matches_spark, python/repark/tests/writes/test_ice_merge_append_1.py
C-004: crates/repark-iceberg/src/tests/merge_append_series.rs::merge_disabled_series_matches_spark, python/repark/tests/writes/test_ice_merge_append_1.py::test_merge_disabled_is_a_real_escape_hatch
C-005: crates/repark-iceberg/src/tests/merge_append_series.rs::branch_merging_append_moves_only_the_branch, crates/repark-iceberg/src/tests/merge_append_series.rs::delete_manifests_carry_forward_through_a_merge, crates/repark-iceberg/src/tests/merge_append_series.rs::merge_never_mixes_partition_spec_ids, crates/repark-iceberg/src/tests/merge_append_series.rs::sequence_numbers_and_file_provenance_survive_a_merge, python/repark/tests/writes/test_ice_merge_append_1.py::test_v3_row_lineage_survives_a_merging_append, python/repark/tests/writes/test_ice_occ_scoped_1.py
C-006: python/repark/tests/writes/test_ice_merge_append_1.py::test_merging_commit_stamps_the_manifests_summary_keys
C-007: none (records)
C-008: crates/repark-iceberg/src/tests/merge_append_series.rs::merge_disabled_series_matches_spark, crates/repark-iceberg/src/tests/merge_append_series.rs::min_count_to_merge_series_matches_spark
C-009: python/repark/tests/writes/test_ice_merge_append_1.py
C-010: none (records)
