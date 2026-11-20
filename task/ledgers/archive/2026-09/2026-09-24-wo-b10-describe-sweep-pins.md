# wo-b10-describe-sweep pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/tests/describe_near_miss.rs::four_part_metadata_suffix_answers_metadata_rows, crates/repark-spark/src/tests/describe_near_miss.rs::four_part_missing_base_and_unknown_suffix_name_the_written_parts, python/repark/tests/iceberg/test_ice_mt_describe_1.py
C-002: crates/repark-spark/src/tests/describe_near_miss.rs, crates/repark-spark/src/tests/describe_owner.rs, python/repark/tests/iceberg/test_describe_table.py::_answer
C-003: crates/repark-spark/src/tests/describe_near_miss.rs::unclosed_bracketed_comment_answers_the_front_door_refusal, python/repark/tests/iceberg/test_describe_table.py::_answer, python/repark/tests/iceberg/test_describe_table.py::test_describe_unclosed_comment_answers_unclosed_bracketed_comment
C-004: crates/repark-spark/src/tests/describe_near_miss.rs, python/repark/tests/iceberg/test_describe_table.py::_answer
C-005: crates/repark-spark/src/tests/describe_column_errors.rs::describe_column_rows, crates/repark-spark/src/tests/describe_owner.rs::describe_rows, crates/repark-spark/src/tests/describe_table.rs::describe_column_rows, crates/repark-spark/src/tests/describe_table.rs::describe_rows
C-006: none (records)
C-007: crates/repark-spark/src/tests/describe_near_miss.rs::four_part_metadata_suffix_answers_metadata_rows, crates/repark-spark/src/tests/describe_near_miss.rs::metadata_rewrite_still_serves_every_other_metadata_path
C-008: crates/repark-spark/src/tests/describe_near_miss.rs::partition_only_tail_scan_covers_each_branch, crates/repark-spark/src/tests/describe_near_miss.rs::partition_spec_without_column_answers_spark_v2_refusal, python/repark/tests/iceberg/test_describe_table.py::test_describe_table_partition_without_column_is_v2_refusal
C-009: crates/repark-spark/src/tests/describe_near_miss.rs::unresolved_column_re_escapes_backticks_in_spark_order, python/repark/tests/iceberg/test_describe_table.py::test_describe_table_doubled_backtick_column_is_re_escaped
C-010: crates/repark-spark/src/tests/describe_owner.rs::identity_partition_rows_quote_only_names_that_need_quoting
