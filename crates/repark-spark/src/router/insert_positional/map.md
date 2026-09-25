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
  `commit_overwrite_by_filter_with_summary` (U7 PR2, 2026-09-24: with the statement's
  `isolation-level` and `validate-from-snapshot-id` as `FilterValidation`; the DataFrame
  writer's `overwrite(condition)` reaches this door through its generated statement,
  pins: u7-write-df-2/C-006, C-009); a literal `false` commits an append, as Spark's
  optimizer does. A missing target answers `TABLE_OR_VIEW_NOT_FOUND`, also when its namespace
  does not exist (round 2, 2026-09-25). The width check plans the deduplicated source, so a
  source whose output names repeat writes (critic r1 V-003); the refusal names the columns
  from the raw source, so no dedup alias reaches the text (round 3, critic r2 V-004). Round 4
  (2026-09-25, critic r3 V-005): `as_written` swaps each `__repark_suffix_literal__(x)` marker
  back to `x` before the kernel converts or renders the predicate, so `id = 1BD` refuses
  `…: id = CAST(1 AS DECIMAL(1,0))`; the non-determinism check still reads the parsed predicate.
  **U7 PR2 slice-2 round 2 (2026-09-25, critic r4 V-001..V-007):** with `StatementWriteOptions::source_by_name`
  (the DataFrame writer's `overwrite(condition)`), `by_name_source` rewrites the source through
  `insert_by_name::by_name_source_query` once the target is resolved — the frame's columns
  bind to the table by name (Spark's `caseSensitive` folding), a column the frame lacks is
  `NULL`, a frame column the table lacks refuses `INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS`
  with Spark's text (before, the facade projected by position and a frame with one extra and
  one missing column committed shifted columns), a wider frame `TOO_MANY_DATA_COLUMNS` first;
  `prepare_source` reparses the rewritten source. The SQL door stays positional.
  pins: u8-write-sql/C-001, C-002, C-003, C-004, C-005, C-016, C-018, C-021, C-024;
  u7-write-df-2/C-013
- `partition_append.rs` — `INSERT INTO … PARTITION (…)` becomes a plain positional INSERT.
  Static values (Spark's string form, checked by an Arrow cast with `safe: false`,
  `CAST_INVALID_INPUT` on failure) go in at their table positions (or after a column list),
  into each `VALUES` row or around a wrapped source; a dynamic-only clause is dropped after
  the width check. `validated_static_equalities` checks the names (`NON_PARTITION_COLUMN`).
  A repeated key refuses `DUPLICATE_KEY` as a parser error, on `INSERT OVERWRITE` too, and an
  overwrite's static values get the same cast check before `insert_overwrite.rs` runs.
  `sql_has_partition_append` marks the statement as an owned write head;
  `refuse_positional_arity` is shared with `replace_where.rs`; it plans one source and names
  the columns from another (the raw source), and `rewrite_partition_clause` takes the raw
  source for the same reason (round 3). Round 4 (2026-09-25, critic r3 V-003): the arity text
  names each raw item as Spark does (`spark_source_names` → `item_name` →
  `expression_output_name`: an alias, a column, a cast of a column as the column, any other
  cast as `CAST(<name> AS <TYPE>)`, `substr` / `substring` in their written spelling, and U6's
  `spark_names::expression_name` for the rest); a source whose leftmost `SELECT` has a `*` falls
  back to the planned names, with each `__repark_col_<n>` mapped back to raw item `n` by
  `named_back`.
  pins: u8-write-sql/C-006, C-007, C-008, C-009, C-023

## Pointers

- Up: [../map.md](../map.md)
