# tz-asof-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/tz_asof_1.rs, python/repark/tests/timestamps/test_tz_asof_1.py
C-002: crates/repark-spark/src/tests/tz_asof_1.rs, python/repark/tests/timestamps/test_tz_asof_1.py
C-003: crates/repark-spark/src/tests/tz_asof_1.rs::arithmetic_projection_names_the_paren_form, crates/repark-spark/src/tests/tz_asof_1.rs::function_projection_names_the_call_form, python/repark/tests/timestamps/test_tz_asof_1.py
C-004: crates/repark-spark/src/tests/tz_asof_1.rs::two_casts_over_two_keys_answer_like_spark, python/repark/tests/timestamps/test_tz_asof_1.py
C-005: crates/repark-spark/src/tests/tz_asof_1.rs::distinct_cast_with_unprojected_key_answers, python/repark/tests/timestamps/test_tz_asof_1.py
C-006: crates/repark-spark/src/tests/tz_asof_1.rs::cast_of_aggregate_names_the_full_cast, python/repark/tests/timestamps/test_tz_asof_1.py
C-007: crates/repark-spark/src/tests/tz_asof_1.rs::snapshots_cast_orders_and_trims_like_spark, python/repark/tests/timestamps/test_tz_asof_1.py::test_snapshots_cast_orders_and_trims_like_spark, python/repark/tests/timestamps/test_tz_asof_1.py::test_timestamp_rendering_trims_like_spark
C-008: python/repark/tests/timestamps/test_tz_asof_1.py::test_timestamp_as_of_cell_shape_answers_first_snapshot
C-009: crates/repark-spark/src/tests/tz_asof_1.rs::arithmetic_projection_names_the_paren_form, crates/repark-spark/src/tests/tz_asof_1.rs::projected_keys_stars_and_unions_keep_todays_names, crates/repark-spark/src/tests/tz_asof_1.rs::unqualified_key_binds_a_qualified_items_last_segment, crates/repark-spark/src/tests/u9_map.rs::a_map_operand_refuses_comparison_ordering_and_distinct_as_spark_does, python/repark/tests/timestamps/test_tz_asof_1.py
C-010: crates/repark-spark/src/tests/tz_asof_1.rs::struct_field_key_never_binds_a_same_named_column, python/repark/tests/timestamps/test_tz_asof_1.py::test_struct_field_key_never_binds_a_same_named_column
C-011: crates/repark-spark/src/tests/tz_asof_1.rs::bare_key_matching_the_display_name_sorts_the_output_column, crates/repark-spark/src/tests/tz_asof_1.rs::qualified_key_sorts_by_the_column, python/repark/tests/timestamps/test_tz_asof_1.py
C-012: crates/repark-spark/src/tests/tz_asof_1.rs::distinct_with_an_unbindable_key_keeps_the_refusal, python/repark/tests/timestamps/test_tz_asof_1.py
