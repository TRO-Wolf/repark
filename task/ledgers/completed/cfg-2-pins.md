# cfg-2 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: none (records)
C-002: crates/repark-core/src/config_file/tests/mod.rs::auto_register_non_boolean_refuses_naming_key_path
C-003: crates/repark-core/src/named_sources/tests.rs
C-004: crates/repark-core/src/named_sources/tests.rs::source_registration_opens_no_connection
C-005: crates/repark-core/src/named_sources/tests.rs
C-006: crates/repark-core/src/named_sources/tests.rs::sources_listing_names_kind_profile_and_redacts_secrets
C-007: crates/repark-core/src/named_sources/tests.rs::auto_register_false_lists_but_does_not_register
C-008: crates/repark-core/src/named_sources/tests.rs::catalog_named_like_source_refuses_as_duplicate
C-009: crates/repark-core/src/named_sources/tests.rs::unknown_source_handle_refuses_naming_declared_sources
C-010: python/repark/tests/session/test_config_mirror.py::test_rendered_database_source_loads
C-011: crates/repark-core/src/named_sources/tests.rs
C-012: none (records)
C-013: python/repark/tests/io/test_session_sources.py::test_sources_lists_declared_source_with_redacted_properties, python/repark/tests/session/test_config_mirror.py
C-014: python/repark/tests/io/test_session_sources.py
C-015: python/repark/tests/io/test_session_sources.py::test_unknown_source_raises_naming_declared_sources
C-016: python/repark/tests/io/test_session_sources.py
C-017: python/repark/tests/io/test_session_sources.py::test_auto_register_false_listed_not_registered
C-018: none (records)
C-019: python/repark/tests/io/test_session_sources.py::test_list_catalogs_with_auto_registered_source
