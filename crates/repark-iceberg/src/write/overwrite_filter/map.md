# map — repark-iceberg/src/write/overwrite_filter

U8 WRITE-SQL PR1 (2026-09-24): the pins of Spark's overwrite-by-filter kernel.
pins: u8-write-sql/C-003, C-004, C-013, C-015, C-017

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
  two-element list), and the out-of-range integer folds.

## Pointers

- Up: [`../map.md`](../map.md)
