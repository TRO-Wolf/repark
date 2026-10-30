# u1-mem-layout-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-core/src/session/tests/session.rs::register_memory_catalog_fallback_root_is_the_warehouse
C-002: crates/repark-spark/src/tests/mem_layout.rs::mem_layout_ctas_lands_at_warehouse_namespace_table
C-003: crates/repark-spark/src/tests/mem_layout.rs::mem_layout_explicit_location_wins
C-004: crates/repark-spark/src/tests/mem_layout.rs::mem_layout_namespace_location_wins
C-005: crates/repark-spark/src/tests/mem_layout.rs::mem_layout_multi_level_namespace_nests_each_level
C-006: crates/repark-spark/src/tests/mem_layout.rs::mem_layout_unrecorded_temp_fallback_keeps_repark_ctas_path
C-007: crates/repark-spark/src/tests/mem_layout.rs::mem_layout_refuses_path_escape_identifiers
C-008: crates/repark-spark/src/tests/mem_layout.rs::mem_layout_file_uri_warehouse_gives_the_plain_location
C-009: none (records)
C-010: crates/repark-spark/src/tests/mem_layout.rs
C-011: crates/repark-spark/src/tests/call_orphan_cotenancy.rs
C-012: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_two_catalogs_on_one_warehouse_refuse
C-013: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_two_sessions_with_one_catalog_name_refuse
C-014: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_location_inside_another_table_refuses
C-015: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_own_history_metadata_copy_is_swept
C-016: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_stray_non_metadata_file_is_swept
C-017: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_single_catalog_default_sweep_deletes_the_orphan
C-018: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_nested_namespace_table_data_dir_passes_the_guards
C-019: crates/repark-spark/src/tests/call_orphan_cotenancy.rs
C-020: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_unreadable_metadata_file_refuses
C-021: none (records)
C-022: crates/repark-spark/src/tests/call_orphan_scope.rs::call_remove_orphan_files_refuses_a_location_holding_another_table, crates/repark-spark/src/tests/call_orphan_scope.rs::ctas
C-023: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_two_catalogs_data_dir_scan_refuses
C-024: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_two_catalogs_data_dir_scan_refuses, crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_two_sessions_data_dir_scan_refuses
C-025: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_same_catalog_shared_location_data_dir_scan_refuses, crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_two_catalogs_data_dir_scan_refuses
C-026: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_lone_table_data_dir_scan_deletes_the_orphan, crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_two_catalogs_data_dir_scan_refuses
C-027: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_host_table_data_dir_scan_ignores_a_table_nested_in_its_root, crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_two_catalogs_data_dir_scan_refuses
C-028: crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_fallback_root_guard_passes_a_table_dir_inside_the_root, crates/repark-spark/src/tests/call_orphan_cotenancy.rs::call_orphan_cotenancy_two_catalogs_data_dir_scan_refuses
C-029: crates/repark-spark/src/tests/call_orphan_ancestor.rs::call_orphan_ancestor_two_catalogs_sibling_data_dir_scan_refuses
C-030: crates/repark-spark/src/tests/call_orphan_ancestor.rs::call_orphan_ancestor_two_catalogs_sibling_data_dir_scan_refuses, crates/repark-spark/src/tests/call_orphan_ancestor.rs::call_orphan_ancestor_two_sessions_sibling_data_dir_scan_refuses
C-031: crates/repark-spark/src/tests/call_orphan_ancestor.rs::call_orphan_ancestor_enumeration_walks_to_the_storage_root_or_the_own_location, crates/repark-spark/src/tests/call_orphan_ancestor.rs::call_orphan_ancestor_two_catalogs_sibling_data_dir_scan_refuses
C-032: crates/repark-spark/src/tests/call_orphan_ancestor.rs::call_orphan_ancestor_own_data_dir_scan_beside_another_catalog_deletes_the_orphan, crates/repark-spark/src/tests/call_orphan_ancestor.rs::call_orphan_ancestor_two_catalogs_sibling_data_dir_scan_refuses
C-033: python/repark/tests/writes/test_profiles1_table_properties.py::_data_files, python/repark/tests/writes/test_profiles1_table_properties.py::test_bogus_distribution_mode_refuses_at_write
C-034: crates/repark-spark/src/tests/call_orphan_ancestor.rs::call_orphan_ancestor_same_catalog_scan_of_a_table_nested_in_the_own_location_refuses, crates/repark-spark/src/tests/call_orphan_ancestor.rs::call_orphan_ancestor_two_catalogs_sibling_data_dir_scan_refuses
