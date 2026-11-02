# pa-dbt-mismatch-redact-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/dbt-repark/tests/test_profile_mismatch_redact.py::test_catalog_secret_change_names_the_catalog_key, python/dbt-repark/tests/test_profile_mismatch_redact.py::test_session_secret_change_names_the_session_key, python/dbt-repark/tests/test_profile_mismatch_redact.py::test_source_url_change_names_both_url_keys_sorted
C-002: python/dbt-repark/tests/test_profile_mismatch_redact.py::test_added_and_removed_keys_are_named, python/dbt-repark/tests/test_profile_mismatch_redact.py::test_only_database_differs_names_both_and_no_key
C-003: python/dbt-repark/tests/test_profile_mismatch_redact.py::test_dbt_debug_reports_the_refusal_without_a_value, python/dbt-repark/tests/test_profile_mismatch_redact.py::test_refusal_still_raises_and_keeps_the_live_session
C-004: python/dbt-repark/tests/test_profile_mismatch_redact.py::test_refusal_still_raises_and_keeps_the_live_session
C-005: python/dbt-repark/tests/test_profile_mismatch_redact.py::test_connection_info_renders_catalog_properties_redacted, python/dbt-repark/tests/test_profile_mismatch_redact.py::test_dbt_debug_reports_the_refusal_without_a_value, python/dbt-repark/tests/test_profile_mismatch_redact.py::test_jinja_target_renders_identifiers_and_catalog_properties_redacted, python/dbt-repark/tests/test_profile_mismatch_redact.py::test_warehouse_url_credentials_are_masked
C-006: python/dbt-repark/tests/test_profile_mismatch_redact.py
