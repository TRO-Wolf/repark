# source-url-redact-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-002: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-003: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-004: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-005: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-006: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs, crates/repark-core/src/catalog_config/map.md, crates/repark-core/src/config_file/redact.rs::redact_config
C-007: crates/repark-core/src/named_sources/tests.rs::sources_listing_masks_a_password_inside_a_url_shaped_value
C-008: crates/repark-core/src/config_file/tests/wiring.rs::file_dump_masks_a_password_inside_a_catalog_uri, crates/repark-core/src/config_file/wiring.rs::conf_dump_rows, crates/repark-core/src/config_file/wiring.rs::raw_conf_dump_rows, crates/repark-core/src/config_file/wiring.rs::redact_dump_rows, crates/repark-core/src/session.rs::conf_dump, crates/repark-core/src/session/tests/conf_dump_redaction.rs
C-009: crates/repark-core/src/catalog_config/map.md
C-010: crates/repark-spark/src/describe_show/map.md, crates/repark-spark/src/tests/describe_show.rs::describe_namespace_extended_masks_url_userinfo_spark_would_show
C-011: crates/repark-python/src/session_sources.rs, python/repark/src/repark/spark/session/builder_conf.py::get, python/repark/src/repark/spark/session/builder_conf.py::getAll, python/repark/src/repark/spark/session/sql_set_statements.py::_REDACTION_RE, python/repark/src/repark/spark/session/sql_set_statements.py::_redact_conf_value, python/repark/src/repark/spark/session/sql_set_statements.py::_requirement_error, python/repark/src/repark/spark/session/sql_set_statements.py::_type_mismatch_error, python/repark/tests/conftest.py
C-012: python/repark/tests/conftest.py
C-013: python/repark/tests/conftest.py
C-014: crates/repark-core/src/lib.rs::error_map, crates/repark-core/src/lib.rs::session
C-015: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-016: crates/repark-spark/src/table_props_view.rs, crates/repark-spark/src/tests/property_display_redaction.rs, crates/repark-spark/src/view_ddl/execute.rs::show_tblproperties_rows
C-017: crates/repark-core/src/config_file/maintenance.rs::parse_duration, crates/repark-core/src/config_file/maintenance.rs::parse_maintenance_policy, crates/repark-core/src/config_file/maintenance.rs::resolve, crates/repark-core/src/config_file/tests/wiring.rs::typed_knob_refusals_mask_a_password_in_the_echoed_value, crates/repark-core/src/config_file/wiring.rs, crates/repark-core/src/object_store_s3.rs::encode_s3_key_for_url, crates/repark-core/src/object_store_s3.rs::split_s3_url_raw, crates/repark-core/src/partition_overwrite_mode.rs, crates/repark-core/src/session.rs, crates/repark-core/src/session/df_guards.rs::dead_datafusion_54_1_refusal, crates/repark-core/src/session/df_guards.rs::invalid_datafusion_config, crates/repark-core/src/session_time_zone.rs::TimeParserPolicyConfig, crates/repark-core/src/session_time_zone.rs::java_display_zone_id, crates/repark-spark/src/spark_literals/unescape.rs, python/repark/src/repark/spark/_secrets.py, python/repark/src/repark/spark/session/session_configuration.py::TIME_PARSER_POLICY_KEY, python/repark/src/repark/spark/session/session_configuration.py::_SQLCONF_DEFAULTS, python/repark/src/repark/spark/session/session_configuration.py::_forward_datafusion_conf, python/repark/src/repark/spark/session/session_configuration.py::_normalize_display_int, python/repark/src/repark/spark/session/session_configuration.py::is_iceberg_session_write_key, python/repark/src/repark/spark/session/session_configuration.py::lookup_int_entry, python/repark/src/repark/spark/session/session_configuration.py::normalize_display_style, python/repark/src/repark/spark/session/session_state.py::_config_value_error, python/repark/src/repark/spark/session/sql_set_statements.py::_REDACTION_RE, python/repark/src/repark/spark/session/sql_set_statements.py::_requirement_error, python/repark/src/repark/spark/session/sql_set_statements.py::_type_mismatch_error, python/repark/src/repark/spark/session/timestamp_type.py, python/repark/tests/conftest.py, python/repark/tests/types/test_integer_overflow_parity.py
C-018: crates/repark-iceberg/src/catalog/builders.rs, crates/repark-iceberg/src/catalog/tests/memory_props_span.rs::memory_catalog_span_masks_a_password_in_the_warehouse
C-019: python/repark/src/repark/spark/session/sql_set_statements.py::_REDACTION_RE, python/repark/src/repark/spark/session/sql_set_statements.py::_requirement_error, python/repark/src/repark/spark/session/sql_set_statements.py::_type_mismatch_error, python/repark/tests/conftest.py
C-020: crates/repark-distributed/src/iceberg_provider.rs::catalog_spec_from_debug
C-021: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-022: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-023: crates/repark-common/src/redaction/tests.rs
C-024: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-025: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-026: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-027: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs, crates/repark-core/src/read_options.rs
C-028: crates/repark-common/src/redaction/tests.rs
C-029: crates/repark-spark/src/describe_show/map.md, crates/repark-spark/src/tests/describe_show.rs::describe_namespace_masks_comment_location_and_owner_credentials
C-030: crates/repark-core/src/object_store_s3.rs::an_invalid_endpoint_refusal_masks_its_userinfo
C-031: crates/repark-core/src/session/text_write_format/select.rs::build_text_write_copy_parts, crates/repark-core/src/session/text_write_format/select.rs::merge_spec_options, crates/repark-core/src/session/text_write_format/select.rs::text_write_copy_parts
C-032: python/repark-parity/datasets/secrets/datagen.py::fake_secret, python/repark-parity/datasets/secrets/datagen.py::generate, python/repark-parity/datasets/secrets/datagen.py::read_parquet, python/repark-parity/datasets/secrets/datagen.py::small, python/repark-parity/datasets/secrets/datagen.py::write_files, python/repark-parity/tests/test_datasets_secrets.py, python/repark-parity/tests/torture/test_torture_secrets.py, python/repark/src/repark/spark/_secrets.py::mask_user_visible, python/repark/src/repark/spark/_secrets.py::scrub_exception, python/repark/src/repark/spark/session/_funcs.py, python/repark/tests/dataframe/_dfcore_1_expected.py, python/repark/tests/types/test_integer_overflow_parity.py
C-033: crates/repark-distributed/src/iceberg_provider.rs::debug_quoted_values, crates/repark-distributed/src/iceberg_provider.rs::tests
C-034: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-035: crates/repark-functions/src/case_sensitive.rs
C-036: crates/repark-core/src/read_options.rs, crates/repark-core/src/session/path_write/rollback.rs, crates/repark-spark/src/write_options.rs
C-037: crates/repark-common/src/redaction/corpus.rs
C-038: crates/repark-common/src/redaction/corpus.rs
C-039: crates/repark-common/src/redaction/corpus.rs
C-040: crates/repark-python/src/catalog_census.rs, crates/repark-spark/src/describe_show/map.md, python/repark/src/repark/spark/catalog.py, python/repark/tests/conftest.py
C-041: crates/repark-common/src/redaction/corpus.rs, crates/repark-common/src/redaction/map.md, crates/repark-spark/src/tests/property_display_redaction.rs::a_credential_free_storage_location_is_shown_as_spark_shows_it
C-042: crates/repark-spark/src/describe_show/map.md, crates/repark-spark/src/table_props_view.rs, crates/repark-spark/src/tests/property_display_redaction.rs::a_credential_free_storage_location_is_shown_as_spark_shows_it, crates/repark-spark/src/view_ddl/show_tblproperties.rs
C-043: crates/repark-common/src/redaction/corpus.rs, crates/repark-common/src/redaction/map.md
C-044: crates/repark-common/src/redaction/corpus.rs, crates/repark-common/src/redaction/map.md
C-045: crates/repark-common/src/redaction/corpus.rs, crates/repark-common/src/redaction/map.md
C-046: crates/repark-common/src/redaction/corpus.rs, crates/repark-common/src/redaction/map.md
C-047: crates/repark-common/src/redaction/corpus.rs
C-048: crates/repark-common/src/redaction/corpus.rs
C-049: crates/repark-common/src/redaction/corpus.rs
C-050: crates/repark-common/src/redaction/corpus.rs
C-051: crates/repark-common/src/redaction/corpus.rs
C-052: crates/repark-common/src/redaction/corpus.rs, crates/repark-common/src/redaction/map.md
C-053: crates/repark-common/src/redaction/corpus.rs
C-054: crates/repark-common/src/redaction/corpus.rs
C-055: crates/repark-functions/src/case_sensitive.rs, crates/repark-python/src/session_runtime.rs::refused_boolean_knob, crates/repark-spark/src/extension/map.md, crates/repark-spark/src/extension/tests.rs::configure_defaults_ansi_enabled_true, crates/repark-spark/src/extension/tests.rs::configure_defaults_parse_float_as_decimal, crates/repark-spark/src/extension/tests.rs::configure_honors_ansi_enabled_false, crates/repark-spark/src/extension/tests.rs::configure_makes_bare_1_23_decimal128_3_2, crates/repark-spark/src/extension/tests.rs::configure_masks_a_url_password_in_the_ansi_refusal, crates/repark-spark/src/extension/tests.rs::configure_masks_a_url_password_in_the_case_sensitive_refusal, crates/repark-spark/src/extension/tests.rs::configure_masks_a_url_password_in_the_merge_schema_refusal, crates/repark-spark/src/extension/tests.rs::configure_refuses_ansi_notabool, crates/repark-spark/src/extension/tests.rs::masking_a_configuration_message_changes_only_the_url_password, python/repark/tests/functions/test_map_column_panic_1.py
C-056: crates/repark-common/src/redaction/tests.rs::mask_url_userinfo_masks_a_url_password_and_leaves_other_text, crates/repark-python/src/lib.rs::datafusion_to_py_err, crates/repark-python/src/lib.rs::temp_view_names, crates/repark-python/src/lib.rs::to_py_err
C-057: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs
C-058: crates/repark-core/src/session.rs::build, crates/repark-core/src/session/spill.rs::maybe_apply_runtime_set, crates/repark-python/src/session_runtime.rs::apply_runtime_config, crates/repark-python/src/session_runtime.rs::restore_runtime_config, crates/repark-python/src/session_runtime.rs::set_runtime_config, crates/repark-python/src/session_sources.rs::mask_url_userinfo, crates/repark-python/src/session_sources.rs::mask_user_visible, crates/repark-python/src/session_sources.rs::register_config_value
C-059: crates/repark-python/src/session_sources.rs::mask_url_userinfo, crates/repark-python/src/session_sources.rs::mask_user_visible, crates/repark-python/src/session_sources.rs::register_config_value, python/repark/src/repark/spark/_secrets.py::mask_credentials, python/repark/src/repark/spark/_secrets.py::register_config_value, python/repark/src/repark/spark/session/builder_conf.py::set, python/repark/src/repark/spark/session/builder_conf.py::unset, python/repark/src/repark/spark/session/session_configuration.py::_forward_datafusion_conf, python/repark/src/repark/spark/session/session_configuration.py::lookup_int_entry
C-060: crates/repark-python/src/exceptions.rs::mask_user_visible, crates/repark-python/src/exceptions.rs::masked_message_params
C-061: python/repark/src/repark/spark/session/session_configuration.py::_forward_datafusion_conf, python/repark/src/repark/spark/session/session_configuration.py::lookup_int_entry
C-062: python/repark/src/repark/spark/session/sql_set_statements.py::_requirement_error
C-063: python/repark/tests/conftest.py
C-064: python/repark/tests/conftest.py
C-065: python/repark/src/repark/spark/session/session_configuration.py::lookup_int_entry
C-066: crates/repark-python/src/dataframe_stats.rs, crates/repark-python/src/exceptions.rs::mask_user_visible, crates/repark-python/src/exceptions.rs::masked_message_params, crates/repark-python/src/orc_io.rs, crates/repark-python/src/writer_layout.rs
C-067: python/repark/src/repark/spark/_secrets.py, python/repark/src/repark/spark/catalog.py, python/repark/src/repark/spark/dataframe/eager.py::_count_rows, python/repark/src/repark/spark/dataframe/eager.py::_eager_materialize, python/repark/src/repark/spark/dataframe/eager.py::_resolve_cache_budgets, python/repark/src/repark/spark/dataframe/eager.py::_to_lazy, python/repark/src/repark/spark/dataframe/export_errors.py::_export_engine_error, python/repark/src/repark/spark/dataframe/writer_readwriter.py::DataFrameWriter, python/repark/src/repark/spark/dataframe/writer_readwriter.py::DataFrameWriterV2, python/repark/src/repark/spark/dataframe/writer_readwriter.py::option, python/repark/src/repark/spark/dataframe/writer_text.py, python/repark/src/repark/spark/session/sql_udf_materialization.py::_sql_udf_clean_exception
C-068: python/repark/src/repark/spark/dataframe/writer_layout.py, python/repark/src/repark/spark/dataframe/writer_readwriter.py::DataFrameWriter, python/repark/src/repark/spark/dataframe/writer_readwriter.py::DataFrameWriterV2, python/repark/src/repark/spark/dataframe/writer_readwriter.py::option, python/repark/src/repark/spark/session/reader.py::_reject_csv_json_parse_options, python/repark/src/repark/spark/session/reader.py::option, python/repark/src/repark/spark/session/reader.py::options, python/repark/src/repark/spark/session/reader.py::smartCsv
C-069: python/repark/tests/conftest.py
C-071: python/repark/src/repark/spark/catalog.py, python/repark/src/repark/spark/dataframe/cache_handle.py::_register_cache_frame, python/repark/src/repark/spark/dataframe/core.py, python/repark/src/repark/spark/dataframe/eager.py, python/repark/src/repark/spark/dataframe/writer_readwriter.py::_materialize_empty_path_write, python/repark/src/repark/spark/dataframe/writer_text.py, python/repark/src/repark/spark/session/session_core.py, python/repark/tests/conftest.py
C-073: python/repark/src/repark/spark/dataframe/udf_bridge.py, python/repark/tests/conftest.py
C-074: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs::registered_values_mask_when_their_own_edge_is_punctuation
C-075: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs::registered_values_mask_after_an_ansi_csi_sequence
C-076: python/repark/src/repark/spark/_secrets.py, python/repark/src/repark/spark/dataframe/frame_nodes.py, python/repark/src/repark/spark/dataframe/grouped_arrow.py::_call_grouped_user_func, python/repark/src/repark/spark/dataframe/grouped_arrow.py::_iter_apply_in_arrow_results, python/repark/src/repark/spark/dataframe/grouped_udf.py::_grouped_agg_pandas, python/repark/src/repark/spark/dataframe/joins_columns.py, python/repark/src/repark/spark/dataframe/udf_bridge.py, python/repark/src/repark/spark/functions_udf.py, python/repark/src/repark/spark/table_arg.py, python/repark/src/repark/spark/udtf.py::__call__, python/repark/src/repark/spark/udtf.py::_map_arrow_udtf_batches, python/repark/src/repark/spark/udtf.py::_refuse_udtf_as_scalar_udf, python/repark/src/repark/spark/udtf.py::eval, python/repark/src/repark/spark/udtf.py::try_sql_registered_udtf, python/repark/tests/conftest.py, scripts/check_lib_py.py
C-077: python/repark/src/repark/spark/dataframe/grouped_udf.py::_grouped_agg_pandas, python/repark/src/repark/spark/dataframe/udf_bridge.py, python/repark/src/repark/spark/udtf.py::eval, python/repark/tests/conftest.py, scripts/check_lib_py.py
C-078: python/repark/tests/conftest.py
C-079: python/repark/src/repark/spark/_secrets.py::scrub_exception, python/repark/tests/conftest.py
C-080: python/repark/src/repark/spark/_secrets.py, python/repark/tests/conftest.py
C-081: crates/repark-common/src/redaction/map.md, crates/repark-common/src/redaction/tests.rs::registered_values_mask_after_an_ecma_48_csi_sequence
C-082: python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark/src/repark/spark/_secrets.py::mask_credentials, python/repark/src/repark/spark/_secrets.py::mask_url_userinfo, python/repark/src/repark/spark/_secrets.py::scrub_exception, python/repark/src/repark/spark/_secrets.py::scrub_user_failure, python/repark/src/repark/spark/dataframe/udf_bridge.py, python/repark/tests/conftest.py, scripts/check_lib_py.py
C-083: python/repark/src/repark/spark/_secrets.py, python/repark/tests/conftest.py
C-084: python/repark/tests/conftest.py
C-085: python/repark/src/repark/spark/_secrets.py, python/repark/tests/conftest.py
