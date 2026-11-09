# empty-dml-shared-guard-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-core/src/stack/empty_dml/tests.rs::an_empty_delete_or_update_never_reaches_the_provider_and_answers_a_zero_count, crates/repark-sql/tests/empty_dml_shared_guard.rs::the_native_door_path_writes_nothing_for_an_empty_dml_on_every_table_kind, crates/repark-sql/tests/empty_dml_shared_guard.rs::the_ansi_door_writes_nothing_for_an_empty_dml_on_every_table_kind, crates/repark-sql/tests/empty_dml_shared_guard.rs::the_spark_door_writes_nothing_for_an_empty_dml_on_every_table_kind, python/repark/tests/writes/test_empty_dml_shared_guard_1.py::test_an_empty_dml_writes_and_commits_nothing
C-002: crates/repark-python/src/session_tests.rs::every_python_door_session_plans_an_empty_dml_as_a_no_op, crates/repark-python/src/session_tests.rs::the_native_door_answers_a_zero_count_for_an_empty_dml, crates/repark-python/src/column/expr_tests.rs::the_expression_context_plans_an_empty_dml_as_a_no_op, crates/repark-core/src/stack/empty_dml/tests.rs::an_empty_dml_nested_in_a_wider_plan_is_guarded_too
C-003: crates/repark-sql/tests/empty_dml_shared_guard.rs::the_native_door_path_writes_nothing_for_an_empty_dml_on_every_table_kind, crates/repark-sql/tests/empty_dml_shared_guard.rs::the_ansi_door_writes_nothing_for_an_empty_dml_on_every_table_kind, crates/repark-sql/tests/empty_dml_shared_guard.rs::the_spark_door_writes_nothing_for_an_empty_dml_on_every_table_kind, python/repark/tests/writes/test_empty_dml_shared_guard_1.py::test_an_empty_dml_writes_and_commits_nothing, crates/repark-core/src/stack/empty_dml/tests.rs::the_rewrite_replaces_only_a_delete_or_update_over_an_empty_relation
C-004: none (records)
C-005: python/repark/tests/writes/test_empty_dml_shared_guard_1.py::test_an_empty_dml_writes_and_commits_nothing, crates/repark-core/src/stack/empty_dml/tests.rs::truncate_and_an_empty_insert_source_are_left_to_the_provider
C-006: none (records)
C-007: crates/repark-core/src/stack/empty_dml/tests.rs::a_refused_wrapper_of_an_empty_dml_stays_refused, python/repark/tests/writes/test_empty_dml_shared_guard_1.py::test_a_refused_wrapper_of_an_empty_dml_stays_refused
C-008: python/repark/tests/writes/test_empty_dml_shared_guard_1.py::test_a_control_dml_still_writes_what_spark_writes, crates/repark-core/src/stack/empty_dml/tests.rs::a_selective_unrestricted_or_identity_dml_still_reaches_the_provider
C-009: python/repark/tests/writes/test_empty_dml_shared_guard_1.py::test_an_empty_dml_writes_and_commits_nothing
C-010: python/repark/tests/writes/test_empty_dml_shared_guard_1.py::test_an_empty_dml_writes_and_commits_nothing
C-011: none (records)
C-012: crates/repark-core/src/stack/empty_dml/tests.rs::an_empty_dml_nested_in_a_wider_plan_is_guarded_too
C-013: python/repark/tests/writes/test_empty_dml_shared_guard_1.py::test_an_empty_dml_writes_and_commits_nothing, crates/repark-sql/tests/empty_dml_shared_guard.rs::the_spark_door_writes_nothing_for_an_empty_dml_on_every_table_kind, crates/repark-sql/tests/empty_dml_shared_guard.rs::the_ansi_door_writes_nothing_for_an_empty_dml_on_every_table_kind
C-014: crates/repark-sql/tests/empty_dml_shared_guard.rs::the_native_door_path_writes_nothing_for_an_empty_dml_on_every_table_kind
C-015: none (records)
C-016: none (records)
C-017: none (records)
