# a13-shared-ctas-fallback pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-core/src/session/tests/session.rs::register_memory_catalog_fallback_root_is_the_warehouse
C-002: crates/repark-core/src/session/tests/session.rs::configured_memory_catalog_fallback_root_is_the_warehouse
C-003: crates/repark-spark/src/tests/ctas.rs::register_memory_catalog_location_less_ctas_lands_under_warehouse, python/repark/tests/writes/test_a13_ctas_fallback.py::test_location_less_ctas_writes_under_the_warehouse
C-005: python/repark/tests/writes/test_a13_ctas_fallback.py::test_two_warehouses_do_not_share_a_location_less_table
C-007: crates/repark-spark/src/tests/ctas.rs::ctas_location_less_namespace_fails_loud_for_non_memory_catalog
C-008: crates/repark-spark/src/tests/call_orphan.rs::call_orphan_shared_ctas_root_rule
C-011: crates/repark-core/src/session/tests/a13.rs::register_memory_catalog_file_uri_fallback_root_is_the_filesystem_path
