# fa-5-6 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-core/src/session/df_guards/duplicate_names.rs::DISPLAY_NAME_KEY, crates/repark-core/src/session/tests/duplicate_names.rs, crates/repark-python/src/dataframe_names.rs::rename_duplicate_tolerant, crates/repark-python/src/dataframe_names.rs::rename_output_fields
C-002: crates/repark-core/src/session/path_write/rollback.rs, crates/repark-core/src/session/tests/duplicate_names.rs, crates/repark-core/src/session/text_write_format/select.rs, crates/repark-core/src/session/text_write_format/serializer.rs::ReparkTextSerializer, crates/repark-core/src/session/text_write_format/serializer.rs::with_display_header, python/repark/src/repark/spark/dataframe/core.py::writeTo, python/repark/tests/resolution/test_fa_5_duplicate_csv.py
C-003: python/repark/src/repark/spark/dataframe/core.py::writeTo, python/repark/src/repark/spark/qualified_names.py, python/repark/tests/resolution/test_fa_5_duplicate_csv.py
C-004: crates/repark-core/src/session/text_write_format/select.rs, python/repark/src/repark/spark/dataframe/core.py::writeTo, python/repark/tests/resolution/test_fa_5_duplicate_csv.py
C-005: python/repark/tests/resolution/test_fa_5_duplicate_csv.py
C-008: python/repark/tests/resolution/test_fa_5_duplicate_csv.py
