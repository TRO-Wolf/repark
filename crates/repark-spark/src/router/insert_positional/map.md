# map — repark-spark/src/router/insert_positional

U8 WRITE-SQL PR1 (2026-09-24): the two INSERT intercepts that `../insert_positional.rs`
declares. Pins: [../../tests/replace_where.rs](../../tests/replace_where.rs),
[../../tests/partition_append.rs](../../tests/partition_append.rs).

## Purpose

Rewrite or execute the INSERT forms the stock parser cannot model on the Spark door.

## Contents

- `replace_where.rs` — `INSERT INTO [TABLE] <name> REPLACE WHERE <predicate> <query>`.
  `parse_replace_where` tokenizes (after a cheap `replace` substring check), finds
  `REPLACE WHERE` at paren depth 0 before any query keyword, and parses the predicate with
  `parse_expr` and the rest with `parse_query`. `REPLACE WHERE` after `OVERWRITE`, a column
  list, `BY NAME` or `PARTITION` refuses `PARSE_SYNTAX_ERROR` near `'REPLACE'` as a parser
  error (`ParseException`). `execute_replace_where` refuses a subquery predicate, plans the
  source through `spark_ast::execute_insert_source` (the branch write ref stripped), checks
  the width, refuses a volatile predicate from the planned `Filter`, converts it with
  `repark_iceberg::write::spark_overwrite_filter`, stages, and commits
  `commit_overwrite_by_filter_with_summary`; a literal `false` commits an append, as Spark's
  optimizer does. A missing target answers `TABLE_OR_VIEW_NOT_FOUND`.
  pins: u8-write-sql/C-001, C-002, C-003, C-004, C-005
- `partition_append.rs` — `INSERT INTO … PARTITION (…)` becomes a plain positional INSERT.
  Static values (Spark's string form, checked by an Arrow cast with `safe: false`,
  `CAST_INVALID_INPUT` on failure) go in at their table positions (or after a column list),
  into each `VALUES` row or around a wrapped source; a dynamic-only clause is dropped after
  the width check. `validated_static_equalities` checks the names (`NON_PARTITION_COLUMN`).
  A repeated key refuses `DUPLICATE_KEY` as a parser error, on `INSERT OVERWRITE` too, and an
  overwrite's static values get the same cast check before `insert_overwrite.rs` runs.
  `sql_has_partition_append` marks the statement as an owned write head;
  `refuse_positional_arity` is shared with `replace_where.rs`.
  pins: u8-write-sql/C-006, C-007, C-008, C-009

## Pointers

- Up: [../map.md](../map.md)
