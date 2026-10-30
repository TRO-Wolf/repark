# fnp-6d-followup-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-functions/src/bitmap_agg/tests.rs::and_agg_refuses_non_binary_with_spark_class, crates/repark-functions/src/bitmap_agg/tests.rs::or_agg_refuses_non_binary_with_spark_class, python/repark/tests/functions/test_fnp_6d_followup_1.py::test_or_and_agg_refuses_non_binary_payload
C-002: crates/repark-functions/src/bitmap_agg/tests.rs::construct_agg_refuses_non_bigint_with_spark_class, python/repark/tests/functions/test_fnp_6d_followup_1.py::test_construct_agg_refuses_non_bigint_payload
C-003: crates/repark-functions/src/bitmap_agg/tests.rs::construct_agg_malformed_string_raises_cast_invalid_input, python/repark/tests/functions/test_fnp_6d_followup_1.py::test_construct_agg_malformed_string_raises_cast_invalid_input
C-004: crates/repark-functions/src/bitmap_agg/tests.rs::construct_agg_answers_numeric_trimmed_and_null, crates/repark-functions/src/bitmap_agg/tests.rs::grouped_construct_agg_answers_trimmed_strings, python/repark/tests/functions/test_fnp_6d_followup_1.py::test_construct_agg_answers_numeric_trimmed_and_null
C-005: python/repark/tests/functions/test_fnp_6d_followup_1.py
C-006: python/repark/tests/functions/test_fnp_6d_bitmap_aggregates.py
C-007: python/repark/tests/functions/test_fnp_6d_followup_1.py
