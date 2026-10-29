# log1p-1-precise-kernels pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/_live_parity.py, python/repark/tests/functions/test_log1p_1.py, python/repark/tests/parity/test_parity_live.py
C-002: crates/repark-functions/src/spark_log1p.rs::register, crates/repark-python/src/column/function_dispatch/dispatch_json.rs, crates/repark-python/src/writer_layout.rs, crates/repark-sql/src/dialect.rs, python/repark/src/repark/spark/functions_json.py, python/repark/tests/functions/test_log1p_1.py
C-003: scripts/check_example_coverage.py::BACKLOG_BASELINE
C-004: python/repark/tests/_live_parity.py, python/repark/tests/functions/test_log1p_1.py, python/repark/tests/parity/test_parity_live.py
C-005: python/repark/tests/functions/test_bl15_bl16_math_divergences.py
