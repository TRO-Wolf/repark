# v3-1-charter pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/call_register.rs::call_unknown_procedure_lists_register_table, python/repark/tests/iceberg/test_maintenance_call.py::test_unknown_procedure_lists_supported
C-002: crates/repark-spark/src/tests/call_register.rs::call_register_table_accepts_positional_arguments, crates/repark-spark/src/tests/call_register.rs::call_register_table_refuses_a_missing_metadata_file_argument
C-003: crates/repark-spark/src/tests/call_register.rs::call_register_table_refuses_an_empty_metadata_file
C-004: python/repark/tests/iceberg/test_maintenance_call.py::test_register_table_adopts_and_returns_spark_columns
C-005: crates/repark-spark/src/tests/call_register.rs::call_register_table_adopts_an_engine_written_table_and_returns_sparks_three_columns, crates/repark-spark/src/tests/call_register.rs::call_register_table_of_a_table_with_no_snapshot_returns_three_nulls
C-007: crates/repark-sql/src/refusals/tests.rs::call_refusal_steers_to_callable_ops_and_names_the_trigger
C-008: crates/repark-spark/src/tests/call_register.rs::call_register_table_adopts_a_spark_written_v3_table_with_puffin_vectors
C-010: crates/repark-spark/src/tests/call_register.rs::call_register_table_of_hadoop_named_metadata_writes_name_the_convention
C-011: crates/repark-spark/src/tests/call_v3.rs::the_engine_still_cannot_produce_a_v3_table
