# sql-set-door-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/sql/test_sql_set_door_1.py
C-002: python/repark/tests/sql/test_sql_set_door_1.py::test_set_quoted_time_zone_value_refuses_with_spark_error, python/repark/tests/sql/test_sql_set_door_1.py::test_set_shuffle_partitions_non_integer_refuses_with_spark_error, python/repark/tests/sql/test_sql_set_door_1.py::test_set_static_sql_conf_refuses_with_spark_error, python/repark/tests/sql/test_sql_set_door_1.py::test_set_time_zone_literal_with_an_unresolvable_zone_refuses, python/repark/tests/sql/test_sql_set_door_1.py::test_set_time_zone_local_is_a_declared_refusal, python/repark/tests/sql/test_sql_set_door_1.py::test_set_unresolvable_time_zone_refuses_with_spark_error
C-003: python/repark/tests/sql/test_sql_set_door_1.py::test_set_repark_namespaced_key_round_trips
C-004: crates/repark-functions/src/session_time_zone/tests.rs::current_timezone_answers_the_carrier_zone_as_a_non_null_string, crates/repark-functions/src/session_time_zone/tests.rs::current_timezone_defaults_to_utc_without_the_carrier, python/repark/tests/sql/test_sql_set_door_1.py::test_current_timezone_answers_the_session_zone
C-005: python/dbt-repark/tests/test_statement_surface.py::_refused, python/dbt-repark/tests/test_statement_surface.py::_served
C-006: none (records)
C-007: none (records)
C-008: none (records)
C-009: none (records)
C-010: none (records)
C-011: none (records)
C-012: none (records)
C-013: none (records)
C-014: none (records)
