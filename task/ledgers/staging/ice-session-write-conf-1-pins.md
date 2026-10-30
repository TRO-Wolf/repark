# ice-session-write-conf-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/writes/test_ice_session_write_conf_1.py
C-002: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_live_oracle_fixture_reproduces
C-003: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_insert_stamps_session_team
C-004: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_two_keys_stamps_both
C-005: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_df_append_stamps_session_team
C-006: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_df_saveastable_stamps_session_team
C-007: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_df_option_wins_over_conf
C-008: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_delete_cow_stamps_overwrite
C-009: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_delete_mor_stamps_delete
C-010: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_update_stamps_overwrite
C-011: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_merge_stamps_overwrite
C-012: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_overwrite_stamps_overwrite
C-013: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_ctas_stamps_session_team
C-014: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_call_rdf_keeps_stamps
C-015: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_rollback_makes_no_snapshot
C-016: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_reserved_operation_untouched
C-017: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_set_sql_applies_team
C-018: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_empty_value_stamps_empty
C-019: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_insert_v3_stamps_session_team
C-020: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_df_append_v3_stamps_session_team
C-021: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_delete_cow_v3_stamps_overwrite
C-022: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_delete_mor_v3_stamps_delete
C-023: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_cz_conf_gzip_df_writes_gzip
C-024: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_cz_conf_gzip_sql_writes_gzip
C-025: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_cz_conf_snappy_sql_writes_snappy
C-026: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_cz_conf_upper_writes_gzip
C-027: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_cz_conf_beats_table_property
C-028: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_cz_option_beats_conf
C-029: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_cz_conf_bogus_refuses_naming_codec
C-030: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_cz_conf_delete_cow_rewrites_gzip
C-031: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_cz_conf_rdf_writes_gzip
C-032: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_live_oracle_fixture_reproduces
C-033: crates/repark-iceberg/src/tests/session_write_conf.rs::bogus_session_codec_refuses_naming_the_codec, crates/repark-iceberg/src/tests/session_write_conf.rs::builder_map_installs_carrier, crates/repark-iceberg/src/tests/session_write_conf.rs::statement_snapshot_wins_over_session, crates/repark-iceberg/src/tests/session_write_conf.rs::unset_clears_each_shape
C-034: crates/repark-spark/src/tests/session_write_conf.rs::bogus_session_codec_refuses_naming_the_codec, crates/repark-spark/src/tests/session_write_conf.rs::session_team_stamps_cow_delete_overwrite, crates/repark-spark/src/tests/session_write_conf.rs::session_team_stamps_plain_insert, crates/repark-spark/src/tests/session_write_conf.rs::unset_session_conf_restores_unstamped_writes
C-035: python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_update_stamps_overwrite
C-036: python/repark/tests/writes/test_ice_session_write_conf_1_paths.py
C-037: python/repark/tests/writes/test_ice_session_write_conf_1_paths.py
C-038: crates/repark-spark/src/tests/write_to_branch.rs, python/repark/tests/writes/test_ice_session_write_conf_1.py::test_sp_update_stamps_overwrite, python/repark/tests/writes/test_ice_session_write_conf_1_paths.py::test_session_property_path_matches_spark
C-039: python/repark/tests/writes/test_ice_session_write_conf_1_paths.py::test_branch_dataframe_append_stamps_session_team
C-040: python/repark-parity/tests/test_cap_1_source_file_line_cap.py::test_cap_1_exception_tables_equal_the_measured_debt, python/repark/tests/writes/test_ice_session_write_conf_1_paths.py::test_session_codec_path_matches_spark
C-041: python/repark/tests/writes/test_ice_session_write_conf_1_paths.py::test_summary_key_collision_matches_spark
C-042: none (records)
C-043: crates/repark-iceberg/src/write/predicate_dml/tests/plain.rs::branch_selector_delete_is_identity_dml_on_the_branch, crates/repark-iceberg/src/write/predicate_dml/tests/plain.rs::empty_branch_selector_delete_is_not_plain_identity, crates/repark-iceberg/src/write/predicate_dml/tests/plain.rs::tag_selector_delete_is_not_plain_identity
C-044: crates/repark-spark/src/tests/session_write_conf.rs::session_team_stamps_cow_delete_overwrite
C-045: crates/repark-spark/src/tests/session_write_conf.rs, crates/repark-spark/src/tests/v3_subquery_dml.rs
C-046: none (records)
C-047: crates/repark-spark/src/tests/session_write_conf.rs::replace_partitions_into_a_new_partition_stamps_deleted_records, crates/repark-spark/src/tests/session_write_conf.rs::replace_partitions_into_an_existing_partition_names_the_engine_value
C-048: crates/repark-iceberg/src/tests/session_write_conf.rs::a_mixed_case_prefix_is_not_a_session_write_key, crates/repark-iceberg/src/tests/session_write_conf.rs::snapshot_suffix_keeps_its_case_and_last_write_wins
C-049: crates/repark-spark/src/tests/session_write_conf.rs::rewrite_manifests_does_not_refuse_a_colliding_session_key, crates/repark-spark/src/tests/session_write_conf.rs::rewrite_manifests_does_not_stamp_the_session_snapshot_property
C-050: none (records)
C-051: crates/repark-spark/src/tests/session_write_conf.rs::a_branch_delete_stamps_the_session_snapshot_property, crates/repark-spark/src/tests/session_write_conf.rs::a_branch_insert_stamps_the_session_snapshot_property, crates/repark-spark/src/tests/session_write_conf.rs::truncate_does_not_stamp_and_does_not_refuse_a_colliding_key
C-052: crates/repark-spark/src/tests/session_write_conf.rs::truncate_does_not_stamp_and_does_not_refuse_a_colliding_key
C-053: none (records)
C-054: crates/repark-spark/src/tests/session_write_conf.rs::static_partition_overwrite_names_the_total_it_would_have_written, crates/repark-spark/src/tests/session_write_conf.rs::static_partition_overwrite_of_a_live_partition_names_the_engine_value, crates/repark-spark/src/tests/session_write_conf.rs::static_partition_overwrite_of_a_new_partition_stamps_deleted_records
C-055: crates/repark-spark/src/tests/session_write_conf.rs::a_session_conf_keeps_a_compound_null_insert, crates/repark-spark/src/tests/session_write_conf.rs::a_session_conf_keeps_the_default_keyword_refusal, crates/repark-spark/src/tests/session_write_conf.rs::a_session_conf_keeps_the_values_list_typing
C-056: crates/repark-spark/src/tests/session_write_conf.rs::a_mixed_case_metric_suffix_is_a_different_key_and_stamps, crates/repark-spark/src/tests/session_write_conf.rs::the_exact_metric_suffix_still_refuses
C-057: crates/repark-spark/src/tests/session_write_conf.rs::a_mixed_case_metric_suffix_is_a_different_key_and_stamps, crates/repark-spark/src/tests/session_write_conf.rs::a_session_conf_keeps_a_plain_update_layout, crates/repark-spark/src/tests/session_write_conf.rs::assert_spark_layout, crates/repark-spark/src/tests/session_write_conf.rs::the_exact_metric_suffix_still_refuses
C-058: none (records)
C-059: crates/repark-spark/src/tests/session_write_conf_removals.rs::a_merge_on_read_overwrite_names_the_delete_file_it_removes
C-060: crates/repark-spark/src/tests/session_write_conf_removals.rs::an_identity_decimal_partition_overwrite_names_the_engine_value
C-061: crates/repark-spark/src/tests/session_write_conf_removals.rs::an_empty_source_static_overwrite_names_the_partition_it_clears
C-062: none (records)
C-063: crates/repark-spark/src/tests/session_write_conf.rs::an_owned_plain_update_resolves_a_mixed_case_set_target
C-064: crates/repark-iceberg/src/tests/session_write_conf.rs, crates/repark-spark/src/tests/parquet_dictionary.rs::a_table_without_the_dictionary_property_writes_dictionary_pages, crates/repark-spark/src/tests/parquet_dictionary.rs::parquet_enable_dictionary_false_turns_the_dictionary_pages_off, crates/repark-spark/src/tests/parquet_dictionary.rs::parquet_enable_dictionary_true_keeps_the_dictionary_pages_on, crates/repark-spark/src/tests/parquet_dictionary.rs::the_owned_plain_insert_takes_the_fork_insert_exec_dictionary_rule, crates/repark-spark/src/tests/session_write_conf.rs::a_session_conf_keeps_a_plain_insert_layout
