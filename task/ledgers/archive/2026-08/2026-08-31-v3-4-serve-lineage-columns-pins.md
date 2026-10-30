# v3-4-serve-lineage-columns pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/v3_lineage.rs::v3_lineage_oracle_matrix_is_the_c001_record
C-002: crates/repark-spark/src/tests/v3_lineage.rs::created_v3_table_serves_derived_row_ids, crates/repark-spark/src/tests/v3_lineage.rs::partitioned_v3_dv_select_star_hides_lineage_columns, crates/repark-spark/src/tests/v3_lineage.rs::partitioned_v3_dv_serves_spark_equal_lineage_for_surviving_rows
C-003: none (records)
C-004: python/repark/tests/iceberg/test_v3_lineage_columns.py::test_facade_partitioned_v3_dv_serves_spark_equal_lineage
C-005: crates/repark-spark/src/tests/v3_lineage.rs::v2_table_lineage_columns_are_unresolved, python/repark/tests/iceberg/test_v3_lineage_columns.py::test_facade_v2_table_lineage_columns_are_unresolved
C-006: crates/repark-spark/src/tests/v3_lineage.rs::v1_table_lineage_columns_are_unresolved
C-007: crates/repark-spark/src/tests/v3_lineage.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-008: crates/repark-spark/src/tests/v3_lineage.rs::equality_delete_v3_serves_spark_equal_lineage_for_surviving_rows, python/repark/tests/iceberg/test_v3_lineage_columns.py::test_facade_equality_delete_v3_serves_spark_equal_lineage
C-009: crates/repark-spark/src/tests/v3_lineage.rs::cow_keep_refusal_files_are_byte_untouched
C-010: crates/repark-spark/src/tests/v3_lineage.rs, python/repark-parity/tests/test_plan_1_northstar_fnp_sequence.py
C-011: crates/repark-spark/src/tests/v3_lineage.rs::join_naming_lineage_refuses_v3_rowid2, python/repark/tests/iceberg/test_v3_lineage_columns.py::test_facade_join_naming_lineage_refuses_v3_rowid2
C-012: crates/repark-spark/src/tests/v3_lineage.rs::qualified_and_aliased_single_table_lineage_selects, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-013: crates/repark-spark/src/tests/v3_lineage.rs::cte_and_subquery_naming_lineage_refuse_v3_rowid2, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-014: crates/repark-spark/src/tests/v3_lineage.rs::version_as_of_naming_lineage_refuses_v3_rowid2, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-015: crates/repark-spark/src/tests/v3_lineage.rs::unquoted_row_id_folds_quoted_mixed_case_stays_exact, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-016: crates/repark-spark/src/tests/v3_lineage.rs::select_star_plus_row_id_expands_user_columns_only, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-017: crates/repark-iceberg/src/catalog/tests/lineage_columns.rs::stored_row_id_wins_over_first_row_id_plus_position
C-018: crates/repark-spark/src/tests/v3_lineage.rs::v1_table_lineage_columns_are_unresolved, crates/repark-spark/src/tests/v3_lineage.rs::v2_table_lineage_columns_are_unresolved, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-019: crates/repark-iceberg/src/catalog/tests/lineage_columns.rs::try_new_with_snapshot_is_removed
C-020: crates/repark-iceberg/src/catalog/tests/lineage_columns.rs::filter_on_id_keeps_matching_lineage_row, crates/repark-spark/src/tests/v3_lineage.rs::filtered_lineage_select_returns_matching_rows, python/repark/tests/iceberg/test_v3_lineage_columns.py
