# ap-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/plan_partitioning.rs::plan_days_ts_ranks_first_on_ninety_day_fixture
C-002: crates/repark-spark/src/tests/plan_partitioning.rs::plan_frame_carries_d1_columns_with_arrow_types, crates/repark-spark/src/tests/plan_partitioning.rs::plan_rows
C-003: crates/repark-spark/src/tests/plan_partitioning.rs::plan_boundless_column_is_excluded_and_named_on_last_row
C-004: crates/repark-spark/src/tests/plan_partitioning.rs::plan_unpartitioned_is_a_candidate
C-005: crates/repark-spark/src/call/plan_partitioning/tests.rs::plan_id_is_stable_per_pair_and_unique_per_candidate, crates/repark-spark/src/tests/plan_partitioning.rs::plan_id_repeats_per_candidate_and_differs_across_candidates
C-006: crates/repark-spark/src/tests/plan_partitioning.rs::plan_every_row_carries_the_r001_file_count_caveat
C-007: crates/repark-spark/src/tests/plan_partitioning.rs::plan_branch_besides_main_refuses
C-008: crates/repark-spark/src/tests/plan_partitioning.rs::plan_multi_spec_table_reports_the_single_spec_rewrite
C-009: crates/repark-spark/src/tests/plan_partitioning.rs::plan_identity_region_ranks_first_at_twice_target
C-010: crates/repark-spark/src/tests/plan_partitioning.rs::plan_byte_ratio_is_measured_from_parquet_footers
C-011: crates/repark-spark/src/tests/plan_partitioning.rs::plan_byte_ratio_falls_back_when_a_footer_is_unreadable
C-012: none (records)
C-013: none (records)
