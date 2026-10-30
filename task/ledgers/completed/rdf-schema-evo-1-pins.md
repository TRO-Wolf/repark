# rdf-schema-evo-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/iceberg/test_rdf_schema_evo_1.py::test_rewrite_after_add_column_and_partition_field_matches_spark
C-002: python/repark/tests/iceberg/test_rdf_schema_evo_1.py::test_rewrite_after_add_column_only_partitioned, python/repark/tests/iceberg/test_rdf_schema_evo_1.py::test_rewrite_after_add_column_only_unpartitioned_is_unaffected
C-003: python/repark/tests/iceberg/test_rdf_schema_evo_1.py::test_rewrite_after_drop_column_matches_spark
C-004: python/repark/tests/iceberg/test_rdf_schema_evo_1.py::test_rewrite_after_rename_column_matches_spark
C-005: python/repark/tests/iceberg/test_rdf_schema_evo_1.py::test_rewrite_after_promote_column_matches_spark, python/repark/tests/iceberg/test_rdf_schema_evo_1.py::test_rewrite_after_promote_partition_source_matches_spark
C-006: python/repark/tests/iceberg/test_rdf_schema_evo_1.py::test_rewrite_v3_deletion_vectors_after_evolution_matches_spark
C-007: python/repark/tests/iceberg/test_rdf_schema_evo_1.py::test_rewrite_with_post_evolution_write_is_unaffected
C-008: python/repark/tests/iceberg/test_rdf_schema_evo_1.py::test_rewrite_manifests_on_evolved_table, python/repark/tests/iceberg/test_rdf_schema_evo_1.py::test_rewrite_position_delete_files_on_evolved_table
C-009: python/repark/tests/iceberg/test_rdf_schema_evo_1.py
