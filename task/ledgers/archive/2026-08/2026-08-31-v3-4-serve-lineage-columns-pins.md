# v3-4-serve-lineage-columns pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/v3_lineage.rs
C-002: crates/repark-core/src/lineage_columns.rs::prepare_lineage_sql, crates/repark-iceberg/src/catalog/lineage_columns.rs::LineageColumnsTableProvider, crates/repark-spark/src/tests/v3_lineage.rs
C-003: crates/repark-core/src/lineage_columns.rs::prepare_lineage_sql, crates/repark-sql/src/v3/partitioned_equality_deletes.rs
C-004: python/repark/tests/iceberg/test_v3_lineage_columns.py
C-005: crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-006: crates/repark-spark/src/tests/v3_lineage.rs
C-007: crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-008: crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-009: crates/repark-spark/src/tests/v3_lineage.rs
C-010: crates/repark-spark/src/tests/v3_lineage.rs, python/repark-parity/tests/test_plan_1_northstar_fnp_sequence.py
C-011: crates/repark-core/src/lineage_columns.rs::prepare_lineage_sql, crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-012: crates/repark-core/src/lineage_columns.rs::prepare_lineage_sql, crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-013: crates/repark-core/src/lineage_columns.rs::prepare_lineage_sql, crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-014: crates/repark-core/src/lineage_columns.rs::prepare_lineage_sql, crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-015: crates/repark-core/src/lineage_columns.rs::prepare_lineage_sql, crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-016: crates/repark-core/src/lineage_columns.rs::prepare_lineage_sql, crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-017: crates/repark-iceberg/src/catalog/lineage_columns.rs::LineageColumnsTableProvider, crates/repark-iceberg/src/catalog/tests/lineage_columns.rs
C-018: crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
C-019: crates/repark-iceberg/src/catalog/lineage_columns.rs::LineageColumnsTableProvider, crates/repark-iceberg/src/catalog/tests/lineage_columns.rs
C-020: crates/repark-iceberg/src/catalog/lineage_columns.rs::LineageColumnsTableProvider, crates/repark-iceberg/src/catalog/tests/lineage_columns.rs, crates/repark-spark/src/tests/v3_lineage.rs, crates/repark-sql/src/v3/partitioned_equality_deletes.rs, python/repark/tests/iceberg/test_v3_lineage_columns.py
