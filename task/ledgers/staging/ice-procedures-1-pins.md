# ice-procedures-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/iceberg/test_ice_procedures_1.py::test_pos_rdf_positional_form_compacts
C-002: python/repark/tests/iceberg/test_ice_procedures_1.py::test_pos_rpd_positional_options_bind
C-003: python/repark/tests/iceberg/test_ice_procedures_1.py::test_call_mixed_args_rollback_binds
C-004: python/repark/tests/iceberg/test_ice_procedures_1.py::test_bind_duplicate_binding_refuses
C-005: python/repark/tests/iceberg/test_ice_procedures_1.py::test_bind_unknown_argument_refuses
C-006: python/repark/tests/iceberg/test_ice_procedures_1.py::test_bind_missing_required_refuses
C-007: python/repark/tests/iceberg/test_ice_procedures_1.py::test_bind_excess_positional_refuses
C-008: python/repark/tests/iceberg/test_ice_procedures_1.py::test_bind_null_is_unset
C-009: crates/repark-spark/src/tests/call.rs
C-010: python/repark/tests/iceberg/test_ice_procedures_1.py::test_dangling_extra_and_precedence_kept
C-011: none (records)
C-012: python/repark/tests/iceberg/test_ice_procedures_1.py::test_rpd_where_rewrites_matching_partition
C-013: python/repark/tests/iceberg/test_ice_procedures_1.py::test_expire_snapshot_ids_expires_exactly_those
C-014: python/repark/tests/iceberg/test_ice_procedures_1.py::test_expire_accept_and_ignore_trio_equals_plain
C-015: python/repark/tests/iceberg/test_ice_procedures_1.py::test_add_files_partitioned, python/repark/tests/iceberg/test_ice_procedures_1.py::test_add_files_unpartitioned
C-016: python/repark/tests/iceberg/test_ice_procedures_1.py::test_add_files_partition_filter
C-017: python/repark/tests/iceberg/test_ice_procedures_1.py::test_add_files_parallelism_matches_serial
C-018: python/repark/tests/iceberg/test_ice_procedures_1.py::test_add_files_check_duplicate_raises
C-019: python/repark/tests/iceberg/test_ice_procedures_1.py::test_add_files_name_mapping_no_copy_by_name
C-020: none (records)
C-021: crates/repark-spark/src/tests/call_rdf_branch.rs::call_rdf_branch_rewrites_only_the_named_branch, crates/repark-spark/src/tests/call_rdf_branch.rs::call_rdf_branch_unknown_ref_pins_the_fork_message, crates/repark-spark/src/tests/call_rdf_branch.rs::call_rdf_branch_with_dangling_deletes_refuses_both_spellings, python/repark/tests/iceberg/test_ice_procedures_1.py::test_rdf_branch_rewrites_only_the_named_branch
C-022: python/repark/tests/iceberg/test_ice_procedures_1.py::test_rm_sort_by_changes_manifest_clustering, python/repark/tests/iceberg/test_ice_procedures_1.py::test_rm_sort_by_empty_array_refuses, python/repark/tests/iceberg/test_ice_procedures_1.py::test_rm_sort_by_non_partition_column_refuses, python/repark/tests/iceberg/test_ice_procedures_1.py::test_rm_sort_by_positional_and_null
