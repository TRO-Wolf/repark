# facade-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: python/repark/tests/io/test_create_dataframe_materialize.py, python/repark/tests/io/test_facade_1_arrow_c_stream.py::test_create_dataframe_ipc_fallback_stays_when_capsule_hidden, python/repark/tests/io/test_stream_ipc_ingest.py, scripts/check_lib_py.py
C-002: python/repark/tests/io/test_facade_1_arrow_c_stream.py::test_package_imports_with_pyarrow_hidden, python/repark/tests/io/test_facade_1_arrow_c_stream.py::test_polars_and_pandas_consume_arrow_c_stream_with_pyarrow_hidden, python/repark/tests/io/test_facade_1_arrow_c_stream.py::test_to_arrow_names_pyarrow_extra_when_hidden, scripts/check_lib_py.py
C-003: python/repark/tests/dataframe/test_dfcore_4b_exports.py, python/repark/tests/io/test_applyinpandas.py, python/repark/tests/io/test_facade_1_arrow_c_stream.py, python/repark/tests/io/test_mapinarrow.py, python/repark/tests/io/test_mapinarrow_oracle.py, python/repark/tests/perf/test_perf_facade_collect_rows.py
C-004: python/repark/tests/io/test_facade_1_arrow_c_stream.py
C-005: python/repark-parity/tests/test_api_freeze.py, python/repark-parity/tests/test_v1_gate_docs.py, python/repark/tests/io/test_facade_1_arrow_c_stream.py::test_to_arrow_to_pandas_collect_signatures_unchanged
C-006: python/repark/tests/dataframe/test_t3_ux_polish.py::test_h1_to_polars_uses_display_names, python/repark/tests/io/test_facade_1_arrow_c_stream.py::test_to_polars_does_not_call_to_arrow, scripts/check_lib_py.py
