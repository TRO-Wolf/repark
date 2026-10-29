# decimal-cache-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-functions/src/decimal_precision.rs::measure_cache_seam_logical_vs_physical_decimal_field
C-002: crates/repark-functions/src/decimal_precision.rs, crates/repark-functions/src/lambda_rebind.rs, crates/repark-spark/src/extension/tests.rs
C-003: crates/repark-core/src/session/temp_views.rs::cache_conform_tests, crates/repark-core/src/session/temp_views.rs::conform_batches_to_schema, crates/repark-core/src/session/temp_views.rs::register_collected_memtable
C-004: crates/repark-core/src/session/temp_views.rs::cache_conform_tests, crates/repark-core/src/session/temp_views.rs::conform_batches_to_schema, crates/repark-core/src/session/temp_views.rs::register_collected_memtable
C-005: python/repark/tests/types/decimal_cache_1_oracle.json, python/repark/tests/types/test_decimal_cache_1.py::test_neg_null_column_rows_stay_null
C-006: python/repark/tests/types/test_decimal_cache_1.py::test_neg_null_column_rows_stay_null
C-007: crates/repark-python/src/column/display.rs::unary_neg, python/repark/src/repark/spark/column.py::__neg__, python/repark/tests/types/decimal_cache_1_oracle.json, python/repark/tests/types/test_decimal_cache_1.py::test_neg_null_column_rows_stay_null
C-008: crates/repark-core/src/session/temp_views.rs::cache_conform_tests, crates/repark-core/src/session/temp_views.rs::conform_batches_to_schema, crates/repark-core/src/session/temp_views.rs::register_collected_memtable
C-009: crates/repark-core/src/session/temp_views.rs::cache_conform_tests, crates/repark-core/src/session/temp_views.rs::conform_batches_to_schema, crates/repark-core/src/session/temp_views.rs::register_collected_memtable
C-010: crates/repark-core/src/session/temp_views.rs::cache_conform_tests, crates/repark-core/src/session/temp_views.rs::conform_batches_to_schema, crates/repark-core/src/session/temp_views.rs::register_collected_memtable
C-011: crates/repark-core/src/session/temp_views.rs::cache_conform_tests, crates/repark-core/src/session/temp_views.rs::conform_batches_to_schema, crates/repark-core/src/session/temp_views.rs::register_collected_memtable
C-012: crates/repark-functions/src/decimal_precision.rs::SparkNegateNullDecimal, crates/repark-functions/src/decimal_precision/map.md, crates/repark-spark/src/extension/tests.rs, python/repark/tests/types/decimal_cache_1_oracle.json, python/repark/tests/types/test_decimal_cache_1.py::test_neg_null_column_rows_stay_null
