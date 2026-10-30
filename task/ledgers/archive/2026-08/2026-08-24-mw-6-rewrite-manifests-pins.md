# mw-6-rewrite-manifests pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/call_manifests.rs::call_rewrite_manifests_compacts_like_spark, python/repark/tests/iceberg/test_maintenance_call.py::test_rewrite_manifests_compacts_like_spark
C-002: crates/repark-spark/src/tests/call_manifests.rs::call_rewrite_manifests_compacts_like_spark, python/repark/tests/iceberg/test_maintenance_call.py
C-003: crates/repark-spark/src/tests/call_manifests.rs, python/repark/tests/iceberg/test_maintenance_call.py
C-004: crates/repark-spark/src/tests/call_manifests.rs::call_rewrite_manifests_no_op_returns_zeros_and_commits_nothing, python/repark/tests/iceberg/test_maintenance_call.py::test_rewrite_manifests_no_op_returns_zeros
C-006: crates/repark-spark/src/tests/call_manifests.rs::call_rewrite_manifests_rewrites_only_the_current_spec
C-007: crates/repark-spark/src/tests/call_manifests.rs::call_rewrite_manifests_argument_surface_is_sparks
C-009: crates/repark-spark/src/tests/call_manifests.rs::call_rewrite_manifests_on_a_table_with_no_snapshot_returns_zeros
C-011: crates/repark-spark/src/tests/call_manifests.rs::call_rewrite_manifests_added_count_diverges_above_the_target_size
