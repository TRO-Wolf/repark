# map — repark-iceberg/src/write/overwrite_filter

U8 WRITE-SQL PR1 (2026-09-24): the pins of Spark's overwrite-by-filter kernel.
pins: u8-write-sql/C-003, C-004, C-013, C-015, C-017, C-019, C-020

## Purpose

Tests for [`overwrite_filter.rs`](../overwrite_filter.rs), which declares `#[cfg(test)] mod tests;`.

## Contents

- `tests.rs` — the translation table (every operator Spark converts, a string literal coerced
  to the column type, `true`/`false`, `AND` with `false` folding to `FALSE`), `NOT IN` as
  `notNull AND notIn`, the untranslatable forms and their exact
  `Cannot convert Spark predicate to Iceberg expression: …` text, the mis-cased column's
  Iceberg field-lookup text, and four commits on a memory catalog: a whole-partition
  overwrite, added rows outside the filter (not validated), an empty source that commits
  `delete` on a seeded and on an unsnapshotted table, and a partial file match that refuses
  before any commit. Round 2 (2026-09-25): the `<`/`<=` `notNull` conjunct, the `NOT`
  push-down table, the one-element `IN` fold (the `notNull AND notIn` shape is pinned on a
  two-element list), and the out-of-range integer folds. Round 3 (2026-09-25): the conjunct-split
  refusal texts, the `<=>` / beyond-i64 constants, and fractional rounding, each case taken
  from `python/repark/tests/u8_write_sql_spark_oracle.json`. Round 5 (2026-09-25, verifier
  V-001): `a_refusal_renders_the_first_unconvertible_conjunct_as_spark_does` also holds the
  ten `r4/…` renderings of a fractional comparison beside an unconvertible conjunct (`<` as
  `< ceil`, `<=` as `<= floor`, `>` as `> floor`, `>=` as `>= ceil`, `NOT BETWEEN`). Round 4 (2026-09-25, critic r3
  V-001, V-002): `a_decimal_literal_on_an_int_column_follows_spark_unwrap_rules` — a decimal
  literal outside the INT range folds to the out-of-range texts, never a constant, and a `.0`
  literal at the INT boundary follows Spark's boundary rules; BIGINT keeps its constants.
  pins: u8-write-sql/C-022

## Pointers

- Up: [`../map.md`](../map.md)
