# attr-id-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-core/src/session/tests/attr_id.rs
C-002: crates/repark-core/src/session/tests/attr_id.rs::stamp_gives_a_union_the_first_input_ids, crates/repark-core/src/session/tests/attr_id.rs::stamp_keeps_aggregate_keys_and_mints_values_and_window_outputs, crates/repark-core/src/session/tests/attr_id.rs::stamp_keeps_column_ids_and_mints_for_cast_arithmetic_and_literals, crates/repark-core/src/session/tests/attr_id.rs::stamp_leaves_pass_through_nodes_unchanged, crates/repark-core/src/session/tests/attr_id.rs::stamp_mints_a_fresh_id_per_source_field_and_is_idempotent, crates/repark-core/src/session/tests/attr_id.rs::stamp_mints_where_the_first_union_input_has_no_id_to_give
C-003: crates/repark-core/src/session/tests/attr_id.rs::the_join_re_mint_keeps_the_left_and_renames_each_colliding_right_id_once, crates/repark-core/src/session/tests/attr_id.rs::the_join_re_mint_leaves_a_join_of_distinct_attributes_unchanged
C-004: crates/repark-core/src/session/tests/attr_id.rs, crates/repark-core/src/session/tests/attr_id_resolve.rs::resolve_binds_a_single_hit_under_each_rule, crates/repark-core/src/session/tests/attr_id_resolve.rs::resolve_binds_every_twin_of_one_attribute, crates/repark-core/src/session/tests/attr_id_resolve.rs::resolve_narrows_by_a_written_qualifier, crates/repark-core/src/session/tests/attr_id_resolve.rs::resolve_refuses_a_cast_twin_that_reuses_the_name, crates/repark-core/src/session/tests/attr_id_resolve.rs::resolve_refuses_two_attributes_under_one_name, crates/repark-core/src/session/tests/attr_id_resolve.rs::resolve_treats_a_missing_id_as_a_bug_and_checks_the_display_count
C-005: crates/repark-python/src/tests.rs::binding_re_mints_lateral_join_collisions_of_a_self_join, crates/repark-python/src/tests.rs::binding_re_mints_using_join_collisions_of_a_self_join, crates/repark-python/src/tests.rs::binding_stamps_resolves_and_re_mints_a_self_join
C-006: crates/repark-core/src/session/tests/attr_id.rs
C-007: python/repark/tests/resolution/test_attr_id_1_s2.py
C-008: python/repark/tests/resolution/test_attr_id_1_s2.py
C-009: python/repark/tests/resolution/test_attr_id_1_s2.py
C-010: python/repark/tests/resolution/test_attr_id_1_s2.py
C-011: crates/repark-python/src/tests.rs::binding_re_mints_lateral_join_collisions_of_a_self_join, crates/repark-python/src/tests.rs::binding_re_mints_using_join_collisions_of_a_self_join, python/repark/tests/resolution/test_attr_id_1_s2.py
C-014: python/repark/tests/resolution/test_attr_id_1_s2.py
C-015: crates/repark-core/src/session/tests/attr_id_seam.rs, crates/repark-core/src/session/tests/df_guard.rs
C-016: crates/repark-core/src/session/tests/attr_id_seam.rs, python/repark/tests/io/test_e2_readwriter.py
C-017: crates/repark-core/src/session/tests/attr_id_fresh.rs::cast_negated_and_arithmetic_group_keys_get_fresh_ids, crates/repark-core/src/session/tests/attr_id_fresh.rs::every_aggregate_value_the_facade_emits_gets_a_fresh_id, crates/repark-core/src/session/tests/attr_id_fresh.rs::every_window_output_gets_a_fresh_id_and_the_input_passes_through, crates/repark-core/src/session/tests/attr_id_verify.rs::va_cast_and_negative_group_keys_get_fresh_ids, crates/repark-core/src/session/tests/attr_id_verify.rs::va_first_and_last_value_aggregates_get_fresh_ids, crates/repark-core/src/session/tests/attr_id_verify.rs::va_lag_and_lead_window_roots_get_fresh_ids
C-018: crates/repark-core/src/session/tests/attr_id_fresh.rs::a_projection_over_a_same_op_window_or_aggregate_mints_what_it_computes, crates/repark-core/src/session/tests/attr_id_fresh.rs::assert_idempotent, crates/repark-core/src/session/tests/attr_id_fresh.rs::sql_group_by_and_having_over_a_view_mint_the_computed_outputs, crates/repark-core/src/session/tests/attr_id_verify.rs::va_alias_over_an_aliased_cast_that_carries_an_id_is_re_minted_once, crates/repark-core/src/session/tests/attr_id_verify.rs::va_first_value_through_sql_over_a_view_gets_a_fresh_id, crates/repark-core/src/session/tests/attr_id_verify.rs::va_lag_through_select_is_ambiguous_with_its_source, crates/repark-core/src/session/tests/attr_id_verify.rs::va_union_of_unions_takes_the_outermost_first_input
C-019: crates/repark-core/src/session/tests/attr_id_fresh.rs::a_source_id_minted_by_another_process_is_re_minted_once
C-020: crates/repark-core/src/session/tests/attr_id.rs::the_join_re_mint_leaves_a_join_of_distinct_attributes_unchanged, crates/repark-python/src/tests.rs::binding_re_mints_lateral_join_collisions_of_a_self_join
C-021: crates/repark-core/src/session/tests/attr_id_fresh.rs::datafusion_copies_the_argument_id_onto_these_computed_outputs
C-023: crates/repark-core/src/session/tests/attr_id.rs::plan_is_stamped_matches_what_stamp_would_change, crates/repark-python/src/tests.rs
C-024: crates/repark-core/src/session/tests/attr_id.rs::union_reads_inner_alias_ids_through_plain_machinery_aliases, python/repark/tests/resolution/test_attr_id_1_s3a.py, scripts/check_lib_py.py
C-025: crates/repark-core/src/session/tests/attr_id_s3b.rs, python/repark/tests/dataframe/test_filter_predicate_rewrite.py, python/repark/tests/resolution/test_attr_id_1_s3b.py, scripts/check_lib_py.py
C-026: python/repark/tests/dataframe/test_dfcore_1_exports.py, python/repark/tests/dataframe/test_filter_predicate_rewrite.py
C-027: python/repark/tests/resolution/test_attr_id_1_s3b.py
C-028: python/repark/tests/resolution/test_attr_id_1_s3b.py
C-029: python/repark/tests/resolution/test_attr_id_1_s3b.py
C-030: python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark/tests/resolution/test_attr_id_1_s3c.py
C-031: python/repark/tests/resolution/test_attr_id_1_s3c.py
C-032: python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark/tests/resolution/test_attr_id_1_s3d.py
C-033: python/repark/tests/resolution/test_attr_id_1_s3d.py
C-034: python/repark/tests/resolution/test_attr_id_1_s3d.py
C-035: none (records)
C-036: none (records)
C-037: none (records)
C-038: none (records)
C-039: crates/repark-core/src/session/tests/attr_id_resolve.rs::source, crates/repark-core/src/session/tests/attr_id_resolve.rs::stamped, crates/repark-core/src/session/tests/attr_id_resolve.rs::strings, crates/repark-core/src/session/tests/attr_id_s3e.rs, crates/repark-core/src/session/tests/case_bind.rs, crates/repark-python/src/tests.rs, python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark/tests/resolution/test_attr_id_1_s3b.py::test_filter_facade_held_qualifier_binds_single_id, python/repark/tests/resolution/test_attr_id_1_s3e.py, scripts/check_lib_py.py
C-040: python/repark/tests/resolution/test_attr_id_1_s4.py
C-041: python/repark/tests/resolution/test_attr_id_1_s4.py, scripts/check_lib_py.py
C-042: python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark/tests/resolution/test_attr_id_1_s4.py::test_s4_aliased_side_ref_binds_own_side, scripts/check_lib_py.py
C-044: python/repark/tests/resolution/test_attr_id_1_v456.py
C-045: crates/repark-core/src/session/tests/attr_id.rs::the_id_carry_copies_source_ids_by_position_and_keeps_a_carried_plan, python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark/tests/resolution/test_attr_id_1_s2.py::test_cache_keeps_frames_bindable, python/repark/tests/resolution/test_attr_id_1_v456.py, scripts/check_lib_py.py
C-046: python/repark/tests/resolution/test_attr_id_1_sj3.py, python/repark/tests/resolution/test_attr_id_1_sj4.py, python/repark/tests/resolution/test_attr_id_1_sj5.py
C-047: python/repark/tests/resolution/test_attr_id_1_sj2.py, python/repark/tests/resolution/test_attr_id_1_sj3.py, python/repark/tests/resolution/test_attr_id_1_sj4.py, python/repark/tests/resolution/test_attr_id_1_sj5.py
C-048: none (records)
C-049: python/repark/tests/resolution/test_attr_id_1_sj3.py, python/repark/tests/resolution/test_attr_id_1_sj4.py
C-050: crates/repark-python/src/executable_twin_tests.rs::the_twin_is_built_once_per_handle_and_carries_no_id, python/repark/tests/resolution/test_perf_attr_stamp_2_o1.py::test_executing_doors_leave_the_stamped_handle_bindable
C-051: crates/repark-python/src/executable_twin_tests.rs
C-052: crates/repark-core/src/session/tests/attr_id_seam.rs::a_stamped_plan_reaches_the_optimizer_backstop_and_its_collapsed_twin_plans_the_same, python/repark/tests/resolution/test_perf_attr_stamp_2_o1.py::test_export_schema_is_clean_over_sql_defined_views
C-053: python/repark/tests/resolution/test_perf_attr_stamp_2_o1.py::test_export_schema_is_clean_over_sql_defined_views, python/repark/tests/resolution/test_perf_attr_stamp_2_o1.py::test_no_new_reader_of_the_attribute_key, python/repark/tests/resolution/test_perf_attr_stamp_2_o1.py::test_the_native_export_schema_is_clean_without_a_facade_strip
C-054: crates/repark-spark/src/tests/temp_view_attr_ids.rs, python/repark/tests/resolution/test_attr_view_semantics_1.py
C-055: python/repark/tests/resolution/test_attr_id_1_s4.py::test_s4_unheld_sort_marker_over_same_name_twins_refuses
C-056: crates/repark-ta/tests/parallel_window.rs::stamped_plan_through_optimizer_reaches_executors_stripped
C-060: python/repark/tests/resolution/test_attr_id_1_sm2_r1.py::test_nested_write_ordered_by_replaces_the_sort_key
C-061: python/repark/tests/resolution/test_attr_id_1_sm2_dupwrites.py
C-062: python/repark/tests/resolution/test_attr_id_1_sm2_dupexport.py
C-063: python/repark/tests/resolution/test_attr_id_1_sm2_duppandas.py
C-064: python/repark/tests/resolution/test_attr_id_1_sm2_dupviews.py
C-065: python/repark/tests/resolution/test_attr_id_1_sm2_r7.py
C-066: python/repark/tests/resolution/test_attr_id_1_sm2_r6.py
C-067: python/repark/tests/resolution/test_attr_id_1_sm2c_leak.py
C-068: python/repark/tests/resolution/test_attr_id_1_sm2c_leak.py
C-069: crates/repark-core/src/session/tests/attr_id_s3b.rs, python/repark/tests/resolution/test_attr_id_1_s3b.py
C-070: crates/repark-core/src/session/tests/attr_id_s3b.rs, python/repark/tests/resolution/test_casesens_2.py::test_port_sort_twins_from_one_join_refuse_as_spark
C-071: crates/repark-core/src/session/tests/attr_id_s3b.rs, python/repark/tests/resolution/test_attr_id_1_s3b.py::test_orderby_case_twin_ambiguous_names_written_spelling, python/repark/tests/resolution/test_attr_id_1_s3b.py::test_orderby_key_missing_past_hidden_intermediate_refuses, python/repark/tests/resolution/test_attr_id_1_s3b.py::test_orderby_missing_past_redefined_intermediate_refuses, python/repark/tests/resolution/test_attr_id_1_s3b.py::test_orderby_missing_without_intermediate_sorts_by_source, python/repark/tests/resolution/test_attr_id_1_s3b.py::test_orderby_self_join_projection_duplicate_is_unresolved
