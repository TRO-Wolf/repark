# dynflatten-listnull-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/dataframe/test_dynflatten_listnull.py
C-002: crates/repark-core/src/dynamic_flatten/tests/preserve_nulls.rs, crates/repark-core/src/spark_nullable.rs::MAX_NESTED_TYPE_DEPTH, crates/repark-core/src/spark_nullable.rs::promote_parquet_null_types, crates/repark-core/src/spark_nullable.rs::relax_schema_to_nullable, python/repark/tests/dataframe/test_dynflatten_listnull.py
C-003: python/repark/tests/dataframe/test_dynflatten_listnull.py, python/repark/tests/io/test_datasets_facade.py
C-004: python/repark/tests/dataframe/test_dynflatten_listnull.py, python/repark/tests/parity/test_parity_live_dynflatten.py
C-005: python/repark/tests/dataframe/test_dynflatten_listnull.py
C-006: crates/repark-core/src/spark_nullable.rs::MAX_NESTED_TYPE_DEPTH, crates/repark-core/src/spark_nullable.rs::promote_parquet_null_types, crates/repark-core/src/spark_nullable.rs::relax_schema_to_nullable, python/repark/tests/dataframe/test_dynflatten_listnull.py
