# maint-policy-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-core/src/config_file/tests/mod.rs::maintenance_policy_parses_every_documented_key
C-002: crates/repark-core/src/config_file/tests/mod.rs::an_unknown_maintenance_key_refuses_naming_the_key_path
C-003: crates/repark-core/src/config_file/tests/mod.rs::durations_parse_days_hours_and_minutes
C-004: crates/repark-core/src/config_file/tests/mod.rs::a_malformed_duration_refuses_naming_the_key
C-005: crates/repark-core/src/config_file/tests/mod.rs::a_table_entry_overrides_the_profile_policy
C-006: crates/repark-core/src/config_file/tests/mod.rs::the_adaptive_partitioning_key_refuses_as_not_yet_supported
C-007: crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan, crates/repark-spark/src/tests/run_maintenance.rs::run_maintenance_dry_run_plans_every_step_with_planned_status
C-008: crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan, crates/repark-spark/src/tests/run_maintenance.rs::run_maintenance_inline_wins_over_table
C-009: crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan, crates/repark-spark/src/tests/run_maintenance.rs::run_maintenance_dry_run_defaults_to_true
C-010: crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan, crates/repark-spark/src/tests/run_maintenance.rs::run_maintenance_delete_ratio_gate_below_skips_step_1
C-011: crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan, crates/repark-spark/src/tests/run_maintenance.rs::run_maintenance_delete_ratio_gate_at_runs_step_1
C-012: crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan, crates/repark-spark/src/tests/run_maintenance.rs::run_maintenance_no_policy_refuses
C-013: crates/repark-spark/src/tests/run_maintenance.rs::apply_binpacks_to_expected_count, crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan
C-014: crates/repark-spark/src/tests/run_maintenance.rs::apply_expires_to_retain_last, crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan
C-015: crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan
C-016: crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan, crates/repark-spark/src/tests/run_maintenance.rs::failed_step_stops_chain
C-017: crates/repark-spark/src/tests/run_maintenance.rs::apply_omits_gate_skipped_steps_like_dry_run, crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan
C-018: crates/repark-spark/src/tests/run_maintenance.rs::a_repark_toml_policy_reaches_run_maintenance, crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan
C-019: crates/repark-spark/src/tests/run_maintenance.rs::apply_removes_orphan, crates/repark-spark/src/tests/run_maintenance.rs::the_d6_refusal_names_the_active_profile
C-020: crates/repark-core/src/config_file/tests/wiring.rs::file_built_session_stamps_the_registry_maintenance_policy, crates/repark-core/src/config_file/tests/wiring.rs::maintenance_table_resolves_into_file_config_with_profile_name, crates/repark-core/src/config_file/tests/wiring.rs::repask_env_profile_names_the_maintenance_stamp
C-021: python/repark/tests/iceberg/test_maintenance_policy_1.py::test_run_maintenance_dry_run_defaults_to_true
C-022: python/repark/tests/iceberg/test_maintenance_policy_1.py::test_run_maintenance_override_kwargs_reach_the_call
C-023: python/repark/tests/iceberg/test_maintenance_policy_1.py::test_run_maintenance_no_policy_refuses_by_class
C-024: python/repark/tests/iceberg/test_maintenance_policy_1.py::test_session_run_maintenance_frame_shape
C-025: python/repark/tests/iceberg/test_maintenance_policy_1.py::test_run_maintenance_adaptive_partitioning_refuses_reserved
C-026: none (records)
C-027: python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark-parity/tests/test_ex_0_example_coverage.py
C-028: crates/repark-spark/src/tests/call_rewrite_options.rs
C-029: crates/repark-spark/src/tests/call_rewrite_options.rs::call_rewrite_missing_table_reports_the_table_before_a_bad_flag
C-030: python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark-parity/tests/test_ex_0_example_coverage.py::test_ex_0_enumerator_emits_five_families_and_repark_sql, scripts/check_rust_file_size.py
