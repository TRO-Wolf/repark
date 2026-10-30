# ipi-07-branch-read-schema-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/branch_read_schema.rs::branch_selector_projects_current_schema_after_add_column, python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_branch_selector_projects_current_schema
C-002: crates/repark-spark/src/tests/branch_read_schema.rs::reader_options_branch_projects_current_schema, python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_branch_reader_option_projects_current_schema
C-003: crates/repark-spark/src/tests/branch_read_schema.rs::version_as_of_branch_projects_current_schema, python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_version_as_of_branch_projects_current_schema
C-004: crates/repark-spark/src/tests/branch_read_schema.rs::branch_selector_filters_on_the_added_column, python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_branch_filter_on_new_column
C-005: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_branch_drop_and_rename_project_current_schema
C-006: crates/repark-spark/src/tests/branch_read_schema.rs::tag_snapshot_and_timestamp_keep_the_snapshot_schema, python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_tag_snapshot_and_timestamp_keep_snapshot_schema
C-007: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_tag_reader_option_keeps_refusing
C-008: crates/repark-spark/src/tests/branch_read_schema.rs::unknown_ref_keeps_the_exact_refusal, python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_unknown_ref_keeps_refusal
C-009: python/repark/tests/iceberg/test_ice_branch_schema_1.py::test_live_branch_schema_matches_spark
