# mb-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: none (records)
C-002: none (records)
C-003: none (records)
C-004: none (records)
C-005: none (records)
C-006: none (records)
C-007: none (records)
C-008: crates/repark-iceberg/src/microbatch/window_tests.rs::after_snapshot_marks_named_snapshot_consumed, crates/repark-iceberg/src/microbatch/window_tests.rs::after_snapshot_with_unknown_id_refuses_naming_oldest, crates/repark-iceberg/src/microbatch/window_tests.rs::earliest_offset_points_at_oldest_ancestor, crates/repark-iceberg/src/microbatch/window_tests.rs::from_timestamp_matches_second_commit_inclusively, crates/repark-iceberg/src/microbatch/window_tests.rs::from_timestamp_zero_starts_at_oldest, crates/repark-iceberg/src/microbatch/window_tests.rs::initial_offset_on_empty_table_reads_none_on_every_start
C-009: crates/repark-iceberg/src/microbatch/window_tests.rs::max_files_splits_snapshot_into_single_file_windows, crates/repark-iceberg/src/microbatch/window_tests.rs::max_rows_keeps_first_file_whole, crates/repark-iceberg/src/microbatch/window_tests.rs::position_resumes_mid_snapshot, crates/repark-iceberg/src/microbatch/window_tests.rs::replaced_table_refuses_before_any_scan, crates/repark-iceberg/src/microbatch/window_tests.rs::unbounded_limit_ignores_caps, crates/repark-iceberg/src/microbatch/window_tests.rs::unknown_from_snapshot_refuses_naming_oldest, crates/repark-iceberg/src/microbatch/window_tests.rs::window_returns_none_when_from_is_consumed_head, crates/repark-iceberg/src/microbatch/window_tests.rs::window_streams_appends_oldest_first
C-010: crates/repark-iceberg/src/microbatch/window_tests.rs::delete_inside_window_refuses, crates/repark-iceberg/src/microbatch/window_tests.rs::overwrite_inside_window_refuses
C-011: crates/repark-iceberg/src/microbatch/window_tests.rs::replace_inside_window_skipped_silently
C-012: none (records)
C-013: none (records)
C-014: crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_defaults_to_unbounded_earliest, crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_parses_caps, crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_parses_from_timestamp, crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_parses_start_after_snapshot_id, crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_passes_unprefixed_keys, crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_refuses_both_skip_keys, crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_refuses_malformed_values, crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_refuses_two_start_keys, crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_refuses_unknown_keys_under_interpreted_prefixes
C-015: crates/repark-core/src/time_travel/microbatch_source_tests.rs::capped_batches_agree_with_their_plans_row_for_row, crates/repark-core/src/time_travel/microbatch_source_tests.rs::initial_offset_on_an_empty_table_reads_none, crates/repark-core/src/time_travel/microbatch_source_tests.rs::open_refuses_unknown_tables_and_malformed_identifiers, crates/repark-core/src/time_travel/microbatch_source_tests.rs::open_streams_two_appends_then_reports_none
C-016: none (records)
C-017: none (records)
C-018: none (records)
C-019: crates/repark-iceberg/src/microbatch/window_tests.rs::delete_inside_window_refuses, crates/repark-iceberg/src/microbatch/window_tests.rs::overwrite_inside_window_refuses
C-020: none (records)
C-021: none (records)
C-022: none (records)
C-023: none (records)
C-024: none (records)
C-025: none (records)
C-026: crates/repark-core/src/time_travel/microbatch_source_tests.rs::the_same_source_refuses_a_table_replaced_under_its_name, crates/repark-core/src/time_travel/microbatch_source_tests.rs::the_same_source_sees_appends_committed_after_open
C-027: crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_matches_keys_ascii_case_insensitively, crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_refuses_case_twins_with_different_values
C-028: crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_accepts_skip_keys_set_to_false_as_a_no_op, crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_refuses_both_skip_keys
C-029: crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_refuses_recognised_spark_streaming_keys_it_does_not_support
C-030: crates/repark-core/src/time_travel/microbatch_source_tests.rs::from_options_refuses_caps_above_the_spark_int_range
C-031: python/repark/tests/functions/test_lrs3_registered_divergences.py, python/repark/tests/parity/test_parity_live.py, python/repark/tests/session/test_dropin_disclosure.py, scripts/check_docs_links.py
C-032: none (records)
C-033: crates/repark-core/src/time_travel/microbatch_source_fold2_tests.rs::a_drop_and_re_add_never_shows_the_dropped_field, crates/repark-core/src/time_travel/microbatch_source_fold2_tests.rs::a_rename_before_start_reads_every_batch_under_the_new_name, crates/repark-core/src/time_travel/microbatch_source_fold2_tests.rs::a_schema_change_after_open_leaves_the_batch_schema_alone, crates/repark-core/src/time_travel/microbatch_source_fold2_tests.rs::a_type_promotion_before_start_reads_every_batch_promoted
C-034: none (records)
C-035: none (records)
C-036: none (records)
C-037: none (records)
C-038: crates/repark-core/src/time_travel/microbatch_source_fold2_tests.rs::a_key_that_folds_to_a_streaming_prefix_only_under_unicode_refuses, crates/repark-core/src/time_travel/microbatch_source_fold2_tests.rs::boolean_twins_compare_by_meaning
C-039: crates/repark-core/src/time_travel/microbatch_source_fold2_tests.rs::planner_errors_name_the_table_as_the_source_was_opened
