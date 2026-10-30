# fnp-6d pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-functions/src/bitmap_agg/tests.rs::construct_agg_sets_bits_zero_one_two_and_last_and_ignores_null, python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py::test_construct_agg_sets_bits_and_ignores_null
C-002: crates/repark-functions/src/bitmap_agg/tests.rs::or_and_agg_fold_grouped_bitmaps, python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py::test_or_and_agg_fold_grouped_bitmaps
C-003: crates/repark-functions/src/bitmap_agg/tests.rs::empty_input_construct_and_or_are_zeros_and_is_ones, python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py::test_empty_input_identities_are_non_null_binary
C-004: crates/repark-functions/src/bitmap_agg/tests.rs::and_agg_of_one_bitmap_has_length_4096, python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py::test_and_agg_length_is_4096_int
C-005: python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py
C-006: python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py::test_sliding_frame_answers_spark_via_win_slide_rescan
C-007: none (records)
C-008: python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py
C-009: python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py
C-010: none (records)
C-011: crates/repark-functions/src/bitmap_agg/tests.rs::short_binary_or_and_normalize_to_4096, python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py::test_or_and_short_empty_and_long_normalize_to_4096
C-012: python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py::test_construct_agg_coerces_string_like_spark_bigint
C-013: crates/repark-functions/src/bitmap_agg/tests.rs::out_of_range_position_names_invalid_bitmap_position, python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py::test_construct_agg_refuses_out_of_range_with_spark_class
C-014: crates/repark-functions/src/bitmap_agg/tests.rs, python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py::test_all_null_and_empty_identities_and_group_schema
C-015: crates/repark-functions/src/bitmap_agg/tests.rs::unbounded_partition_window_answers, python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py::test_sliding_frame_answers_spark_via_win_slide_rescan, python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py::test_unbounded_partition_window_answers_spark
C-016: none (records)
C-017: none (records)
