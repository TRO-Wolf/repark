# ipi-07-branch-read-schema-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_branch_selector_projects_current_schema
C-002: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_branch_reader_option_projects_current_schema
C-003: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_version_as_of_branch_projects_current_schema
C-004: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_branch_filter_on_new_column
C-005: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_branch_drop_and_rename_project_current_schema
C-006: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_tag_snapshot_and_timestamp_keep_snapshot_schema
C-007: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_tag_reader_option_keeps_refusing
C-008: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_unknown_ref_keeps_refusal
C-009: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_live_branch_schema_matches_spark
