# map — repark-core/src/pre_execute/setop_all

## Purpose

Tests for SQL-SETOP-ALL-REFUSE-1's statement refusal (`../setop_all.rs`).

## Contents

- `tests.rs` — `every_shape_and_spelling_of_a_multiset_operation_is_found_in_both_parser_dialects`
  (eleven shapes, from top level to `MERGE USING` and `EXPLAIN`, times ten spellings, Generic and
  Databricks); `controls_and_text_that_only_mentions_the_words_are_not_found` (the seven controls
  in every shape, string literals, comments, quoted identifiers, `all_rows`, `= ALL (…)`,
  `ORDER BY ALL`, an untokenizable text);
  `the_refusal_names_the_operator_the_distinct_form_the_workaround_and_the_card` (the three texts,
  and `engine_err` maps the DataFusion form to the same `NotImplemented` text);
  `the_session_seat_refuses_before_the_door_and_lets_the_controls_through` (a counting
  `SqlDialect`, plain and behind the local-fs guard, Generic and Databricks parsers: a refused
  statement never reaches the dialect, the controls do and answer their counts);
  `a_refused_insert_or_ctas_writes_nothing` (a `MemTable` keeps zero rows, the CTAS table does not
  exist, the `EXCEPT` control inserts one row).

## Pointers

- Up: [../map.md](../map.md)
