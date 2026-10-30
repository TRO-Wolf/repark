# source-url-redact-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-common/src/redaction/tests.rs::every_url_in_a_value_is_masked, crates/repark-common/src/redaction/tests.rs::lone_userinfo_is_masked_whole, crates/repark-common/src/redaction/tests.rs::url_userinfo_is_masked_for_every_scheme, crates/repark-common/src/redaction/tests.rs::url_userinfo_password_is_masked_keeping_user_host_and_database
C-002: crates/repark-common/src/redaction/tests.rs::unclean_userinfo_inside_the_authority_fails_closed
C-003: crates/repark-common/src/redaction/tests.rs::secret_query_parameters_are_masked
C-004: crates/repark-common/src/redaction/tests.rs::keyword_dsn_secrets_are_masked
C-005: crates/repark-common/src/redaction/tests.rs::non_secret_values_are_untouched
C-006: crates/repark-common/src/redaction/tests.rs::prop_key_is_secret_is_unchanged_by_the_move, crates/repark-common/src/redaction/tests.rs::redact_value_keeps_the_key_rule_and_adds_the_value_rule
C-007: none (records)
C-008: crates/repark-core/src/session/tests/conf_dump_redaction.rs::conf_dump_masks_url_userinfo_while_the_endpoint_resolves_raw
C-009: none (records)
C-010: crates/repark-spark/src/tests/describe_show.rs::describe_namespace_extended_masks_url_userinfo_spark_would_show, crates/repark-spark/src/tests/describe_show.rs::describe_namespace_extended_redaction_truth_table
C-011: python/repark/tests/io/test_source_url_redaction_1.py::test_config_dump_never_carries_the_password
C-012: python/repark/tests/io/test_source_url_redaction_1.py::test_config_dump_never_carries_the_password, python/repark/tests/io/test_source_url_redaction_1.py::test_sources_rows_never_carry_the_password
C-013: python/repark/tests/io/test_source_url_redaction_1.py
C-014: none (records)
C-015: crates/repark-common/src/redaction/tests.rs::userinfo_is_read_only_inside_the_authority
C-016: crates/repark-spark/src/tests/property_display_redaction.rs
C-017: python/repark/tests/io/test_source_url_redaction_1.py::test_knob_refusals_never_carry_the_password, python/repark/tests/io/test_source_url_redaction_1.py::test_set_redaction_matches_the_spark_4_1_2_default_regexes
C-018: crates/repark-iceberg/src/catalog/tests/memory_props_span.rs::memory_catalog_span_masks_a_password_in_the_warehouse
C-019: python/repark/tests/io/test_source_url_redaction_1.py::test_knob_refusals_never_carry_the_password, python/repark/tests/io/test_source_url_redaction_1.py::test_set_redaction_matches_the_spark_4_1_2_default_regexes
C-020: none (records)
C-021: none (records)
C-022: none (records)
C-023: none (records)
C-024: none (records)
C-025: none (records)
C-026: none (records)
C-027: none (records)
C-028: none (records)
C-029: crates/repark-spark/src/tests/describe_show.rs::describe_namespace_masks_comment_location_and_owner_credentials
C-030: none (records)
C-031: none (records)
C-032: python/repark-parity/tests/torture/test_torture_secrets.py
C-033: none (records)
C-034: none (records)
C-035: none (records)
C-036: none (records)
C-037: none (records)
C-038: none (records)
C-039: none (records)
C-040: python/repark/tests/io/test_source_url_redaction_1.py::test_get_database_returns_the_stored_location_and_comment
C-041: crates/repark-spark/src/tests/property_display_redaction.rs::a_credential_free_storage_location_is_shown_as_spark_shows_it
C-042: crates/repark-spark/src/tests/property_display_redaction.rs::a_credential_free_storage_location_is_shown_as_spark_shows_it
C-043: none (records)
C-044: none (records)
C-045: none (records)
C-046: none (records)
C-047: none (records)
C-048: none (records)
C-049: none (records)
C-050: none (records)
C-051: none (records)
C-052: none (records)
C-053: none (records)
C-054: none (records)
C-055: crates/repark-spark/src/extension/tests.rs::configure_defaults_ansi_enabled_true, crates/repark-spark/src/extension/tests.rs::configure_defaults_parse_float_as_decimal, crates/repark-spark/src/extension/tests.rs::configure_honors_ansi_enabled_false, crates/repark-spark/src/extension/tests.rs::configure_makes_bare_1_23_decimal128_3_2, crates/repark-spark/src/extension/tests.rs::configure_masks_a_url_password_in_the_ansi_refusal, crates/repark-spark/src/extension/tests.rs::configure_masks_a_url_password_in_the_case_sensitive_refusal, crates/repark-spark/src/extension/tests.rs::configure_masks_a_url_password_in_the_merge_schema_refusal, crates/repark-spark/src/extension/tests.rs::configure_refuses_ansi_notabool, crates/repark-spark/src/extension/tests.rs::masking_a_configuration_message_changes_only_the_url_password, python/repark/tests/io/test_source_url_redact_1_fn.py
C-056: crates/repark-common/src/redaction/tests.rs::mask_url_userinfo_masks_a_url_password_and_leaves_other_text
C-057: none (records)
C-058: none (records)
C-059: none (records)
C-060: none (records)
C-061: none (records)
C-062: none (records)
C-063: python/repark/tests/io/test_source_url_redact_2.py
C-064: python/repark/tests/io/test_source_url_redact_2.py
C-065: none (records)
C-066: none (records)
C-067: none (records)
C-068: none (records)
C-069: none (records)
C-071: none (records)
C-073: none (records)
C-074: crates/repark-common/src/redaction/tests.rs::registered_values_keep_letter_or_digit_neighbours, crates/repark-common/src/redaction/tests.rs::registered_values_mask_at_punctuation_boundaries, crates/repark-common/src/redaction/tests.rs::registered_values_mask_when_their_own_edge_is_punctuation
C-075: crates/repark-common/src/redaction/tests.rs::registered_values_mask_after_an_ansi_csi_sequence
C-076: python/repark/tests/io/test_source_url_redact_2_doors.py::eval, python/repark/tests/io/test_source_url_redact_2_doors.py::simpleString, python/repark/tests/io/test_source_url_redact_2_doors.py::start, python/repark/tests/io/test_source_url_redact_2_doors.py::terminate, python/repark/tests/io/test_source_url_redact_2_doors.py::test_user_callback_door_formatted_traceback_is_masked, scripts/check_lib_py.py
C-077: python/repark/tests/io/test_source_url_redact_2_doors.py::eval, python/repark/tests/io/test_source_url_redact_2_doors.py::test_dataframe_udf_door_scrubs_a_user_raised_pyspark_exception, python/repark/tests/io/test_source_url_redact_2_doors.py::test_user_raised_pyspark_exception_is_scrubbed, python/repark/tests/io/test_source_url_redact_2_doors.py::test_user_raised_pyspark_exception_without_a_secret_keeps_identity, scripts/check_lib_py.py
C-078: python/repark/tests/io/test_source_url_redact_2_doors.py::test_scalar_iter_consume_door_formatted_traceback_is_masked, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scalar_iter_consume_site_masks_its_own_detail
C-079: python/repark/tests/io/test_source_url_redact_2_doors.py::__str__, python/repark/tests/io/test_source_url_redact_2_doors.py::args, python/repark/tests/io/test_source_url_redact_2_doors.py::test_dataframe_udf_door_masks_groups_and_notes, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_keeps_a_clean_group_and_note, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_masks_groups_and_notes, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_walks_a_deep_chain_in_linear_time
C-080: python/repark/tests/io/test_source_url_redact_2_doors.py::__new__, python/repark/tests/io/test_source_url_redact_2_doors.py::__slots__, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_base_only_stand_in_is_never_an_exception, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_exception_stand_in_is_a_pyspark_exception, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_new_copy_carries_keyword_only_attributes_masked, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_new_copy_carries_slots_masked, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_unbuildable_group_keeps_the_exception_split
C-081: crates/repark-common/src/redaction/tests.rs::registered_values_mask_after_an_ecma_48_csi_sequence
C-082: python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark/tests/io/test_source_url_redact_2_doors.py::test_mask_entry_points_are_total_on_a_lone_surrogate, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_keeps_a_clean_surrogate_message_identity, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_masks_a_lone_surrogate_message, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_masks_an_os_error_with_a_surrogate_filename, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_returns_a_masked_stand_in_when_the_walk_raises, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_user_failure_masks_a_lone_surrogate, python/repark/tests/io/test_source_url_redact_2_doors.py::test_user_callback_door_masks_a_lone_surrogate, scripts/check_lib_py.py
C-083: python/repark/tests/io/test_source_url_redact_2_doors.py::__str__, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_masks_non_str_note_carriers
C-084: python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_masks_tuple_notes
C-085: python/repark/tests/io/test_source_url_redact_2_doors.py::__init__, python/repark/tests/io/test_source_url_redact_2_doors.py::__new__, python/repark/tests/io/test_source_url_redact_2_doors.py::test_scrub_exception_never_reruns_init_with_raw_args
