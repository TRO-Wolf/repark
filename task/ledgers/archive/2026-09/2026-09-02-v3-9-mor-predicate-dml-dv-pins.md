# v3-9-mor-predicate-dml-dv pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/v3_subquery_dml.rs
C-002: python/repark/tests/iceberg/test_v3_live_oracle.py::_live_subquery_where_dml_measurement, python/repark/tests/iceberg/test_v3_live_oracle.py::_materialize, python/repark/tests/iceberg/test_v3_live_oracle.py::test_partitioned_dv_update_commits_and_rewrite_returns_zeros, python/repark/tests/iceberg/test_v3_live_oracle.py::test_v3_merge_matched_update_live_cow_and_mor, python/repark/tests/iceberg/test_v3_live_oracle.py::test_v3_mor_subquery_where_dml_live, python/repark/tests/iceberg/test_v3_live_oracle.py::test_v3_upgrade_v2_to_v3_live_matches_spark
C-003: crates/repark-spark/src/tests/v3_mor_dml.rs::created_v3_mor_plain_where_dml_matches_the_subquery_cell, crates/repark-spark/src/tests/v3_subquery_dml.rs, crates/repark-spark/src/tests/v3e4.rs, python/repark/tests/iceberg/test_v3_cow_dml.py, python/repark/tests/iceberg/test_v3_live_oracle.py
C-004: crates/repark-spark/src/tests/v3_mor_dml.rs::created_v3_mor_plain_where_dml_matches_the_subquery_cell
C-005: crates/repark-spark/src/tests/v3_lineage.rs, python/repark-parity/tests/test_plan_1_northstar_fnp_sequence.py, python/repark-parity/tests/test_v3r_1_rulings.py, python/repark/tests/iceberg/test_v3_live_oracle.py
C-006: crates/repark-spark/src/tests/create_table.rs, crates/repark-spark/src/tests/v3_mor_dml.rs, python/repark/tests/iceberg/test_v3_create_opt_in.py
C-007: crates/repark-spark/src/tests/v3e4.rs::subquery_delete_on_the_shared_puffin_v3_table_keeps_both_file_scoped_deletion_vectors
C-008: python/repark/tests/iceberg/test_v3_live_oracle.py::_live_subquery_where_dml_measurement
C-009: crates/repark-iceberg/src/write/predicate_dml/tests/update.rs::identity_pairs_share_one_arc_per_data_file_path
