# fnp-4b pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/spark_dialect.rs, crates/repark-sql/tests/cross_door.rs, python/repark/tests/functions/test_fnp_4b_spark_dialect.py
C-002: crates/repark-iceberg/src/write/merge/tests/merge.rs, crates/repark-sql/tests/cross_door.rs
C-003: python/repark/tests/functions/test_fnp_4b_spark_dialect.py
C-004: crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/functions/test_fnp_4b_literals.py
C-005: crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/functions/test_fnp_4b_literals.py
C-006: crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/functions/test_fnp_4b_literals.py
C-007: crates/repark-spark/src/tests/spark_dialect.rs
C-008: none (records)
C-009: python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark/tests/dataframe/test_filter_predicate_rewrite.py, scripts/check_rust_file_size.py
C-010: crates/repark-spark/src/tests/spark_dialect.rs::d_suffix_is_double, python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark/tests/functions/test_fnp_4b_literals.py, scripts/check_rust_file_size.py
C-011: crates/repark-spark/src/tests/spark_dialect.rs::bd_literals_take_precision_from_digits, python/repark/tests/functions/test_fnp_4b_literals.py::test_decimal_suffix, python/repark/tests/functions/test_fnp_4b_literals.py::test_decimal_suffix_precision_from_digits
C-012: crates/repark-spark/src/tests/spark_dialect.rs::typed_numeric_literals_are_non_null, python/repark/tests/functions/test_fnp_4b_literals.py::test_typed_numeric_literals_are_non_null
C-013: crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/functions/test_fnp_4b_literals.py::test_exponent_l_and_zero_x_are_unresolved_identifiers
C-014: crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/functions/test_fnp_4b_literals.py::test_struct_field_access_on_call_result, python/repark/tests/functions/test_fnp_4b_spark_dialect.py::test_select_expr_struct_dot_display
C-015: crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/functions/test_fnp_4b_spark_dialect.py::test_expr_backtick_column_reference_binds_the_frame_column, python/repark/tests/functions/test_fnp_4b_spark_dialect.py::test_select_expr_backtick_ident
C-016: crates/repark-spark/src/tests/spark_dialect.rs::chained_struct_field_access_on_call_result, python/repark/tests/functions/test_fnp_4b_literals.py::test_chained_struct_field_access
C-017: crates/repark-spark/src/tests/lambda_door.rs::hof_ctx
C-018: python/repark/tests/dataframe/test_filter_predicate_rewrite.py::test_explicitly_double_quoted_span_is_a_string_literal
C-019: none (records)
C-020: none (records)
C-021: none (records)
C-022: python/repark/tests/functions/test_fnp_4b_spark_dialect.py::test_select_expr_backtick_ident, python/repark/tests/functions/test_fnp_bitmap_facade_1.py::test_sql_door_qualifier_leak_is_expected_divergence
C-023: crates/repark-spark/src/tests/spark_dialect.rs::other_suffixes_keep_spark_types
C-024: crates/repark-iceberg/src/write/merge/tests/merge_dialect.rs, python/repark/tests/writes/test_merge_into.py
C-025: none (records)
C-026: python/repark/tests/functions/test_fnp_4b_hof_display.py
C-029: python/repark/tests/functions/test_fnp_4b_spark_dialect.py
