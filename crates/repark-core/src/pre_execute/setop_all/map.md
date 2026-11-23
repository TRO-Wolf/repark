# map — repark-core/src/pre_execute/setop_all

## Purpose

Tests for SQL-SETOP-ALL-MULTISET-1's multiset rewrite and `MINUS ALL` spelling (`../setop_all.rs`).

## Contents

- `tests.rs` — `every_shape_and_spelling_of_minus_all_becomes_except_all_in_both_parser_dialects`
  (twelve shapes with non-ASCII text before the operator, six spellings, Generic and Databricks);
  `text_that_only_mentions_minus_all_is_unchanged` (string literals, comments, quoted identifiers,
  the distinct `MINUS`, `= ALL (…)`, an untokenizable text); `the_rule_sits_right_after_the_struct_set_operation_rule`;
  `the_rewrite_keeps_the_join_schema_and_a_second_pass_is_the_identity` (top level, scalar and `IN`
  subqueries: no semi or anti join is left, an `Unnest` is); `the_distinct_forms_and_a_semi_join_written_as_a_join_are_untouched`;
  `every_door_answers_the_spark_multiset_and_hands_minus_all_on_as_except_all` (a recording
  `SqlDialect`, plain and behind the local-fs guard, Generic and Databricks parsers: Spark's rows
  for the ALL forms, NULL keys, an empty side and a scalar count, the controls unchanged);
  `an_insert_or_ctas_of_a_multiset_stores_the_spark_rows` (a `MemTable` and a CTAS table).

## Pointers

- Up: [../map.md](../map.md)
