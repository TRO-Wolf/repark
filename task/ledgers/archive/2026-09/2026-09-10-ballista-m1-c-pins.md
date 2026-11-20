# ballista-m1-c pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-distributed/tests/multi_stage.rs::hash_aggregate_over_four_partitions_matches_local_executor, crates/repark-distributed/tests/multi_stage.rs::hash_join_of_two_tables_matches_local_executor, crates/repark-distributed/tests/multi_stage.rs::join_named, crates/repark-distributed/tests/multi_stage.rs::sort_merge_join_with_prefer_hash_join_false_matches_local_executor
C-003: crates/repark-distributed/tests/multi_stage.rs::completed_two_stage_hash_aggregate_reports_shuffle_bytes
C-004: crates/repark-distributed/tests/multi_stage.rs::session_spill_dir_has_no_shuffle_files_after_complete_and_after_cancel
