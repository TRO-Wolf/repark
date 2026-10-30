# sqp-1-spark-string-literals pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/spark_string_literals.rs
C-002: crates/repark-spark/src/tests/spark_string_literals.rs::escaped_quotes_lex_and_unpaired_backslash_refuses
C-003: crates/repark-spark/src/tests/spark_string_literals.rs::adjacent_literals_concatenate
C-004: crates/repark-spark/src/tests/spark_string_literals.rs::raw_strings_are_verbatim
C-005: crates/repark-spark/src/tests/spark_string_literals.rs::datafusion_native_statements_keep_generic_literals, crates/repark-spark/src/tests/spark_string_literals.rs::unescape_is_exactly_once_on_every_path
C-006: crates/repark-sql/tests/ansi_door_string_literals.rs::ansi_door_keeps_generic_literals
C-007: python/repark/tests/sql/test_sqp_1_string_literals.py
C-008: crates/repark-spark/src/tests/spark_string_literals.rs::like_rlike_and_identifier_controls
C-009: crates/repark-spark/src/tests/cast_binary.rs
C-010: crates/repark-spark/src/tests/spark_string_literals.rs::front_door_has_one_caller
C-011: python/repark-parity/tests/test_sqp_1_record.py, python/repark/tests/sql/test_sqp_1_string_literals.py
C-012: crates/repark-spark/src/tests/spark_string_literals.rs::fast_path_borrows_and_errors_carry_position
C-013: python/repark/tests/sql/test_sqp_1_string_literals.py::test_lit_backslash_survives_the_aggregate_embed, python/repark/tests/sql/test_sqp_1_string_literals.py::test_sql_literal_renders_a_backslash_as_a_spark_literal, python/repark/tests/sql/test_sqp_1_string_literals.py::test_stop_words_remover_backslash_and_apostrophe, python/repark/tests/sql/test_sqp_1_string_literals.py::test_string_indexer_round_trips_a_backslash_label, python/repark/tests/sql/test_sqp_1_string_literals.py::test_unpivot_backslash_column_value, scripts/check_python_conventions.py
