# ice-system-functions-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_bucket_long_pins_recorded_values
C-002: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_bucket_string_pins_recorded_values
C-003: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_bucket_date_and_timestamp_pin_recorded_values
C-004: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_bucket_decimal_and_binary_pin_recorded_values
C-005: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_truncate_string_pins_recorded_values, python/repark/tests/iceberg/test_ice_system_functions_1.py::test_truncate_string_shorter_than_width_returns_whole
C-006: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_truncate_long_negative_floors_down, python/repark/tests/iceberg/test_ice_system_functions_1.py::test_truncate_long_pins_recorded_values
C-007: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_truncate_decimal_pins_recorded_values_and_schema
C-008: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_truncate_binary_pins_recorded_values_and_schema
C-009: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_every_function_returns_null_for_null_input
C-010: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_bucket_old_argument_order_refuses, python/repark/tests/iceberg/test_ice_system_functions_1.py::test_bucket_width_must_be_a_positive_literal
C-011: python/repark/tests/iceberg/test_ice_system_functions_1.py
C-012: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_years_pins_recorded_values, python/repark/tests/iceberg/test_ice_system_functions_1.py::test_years_pre_epoch_is_negative
C-013: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_months_pins_recorded_values
C-014: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_days_pins_recorded_values_and_schema
C-015: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_hours_pins_recorded_values
C-016: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_iceberg_version_is_non_null_per_row
C-017: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_every_function_returns_null_for_null_input
C-018: python/repark/tests/iceberg/test_ice_system_functions_1.py
C-019: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_bucket_in_where_pins_recorded_count
C-020: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_show_user_functions_in_system_pins_roster
C-021: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_bare_truncate_still_resolves_to_the_builtin
C-022: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_functions_resolve_under_every_configured_catalog
C-023: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_show_functions_without_in_is_unchanged
C-024: python/repark/tests/iceberg/test_ice_system_functions_1.py
C-025: python/repark/tests/iceberg/test_ice_system_functions_1.py::test_internal_bucket_name_still_resolves
