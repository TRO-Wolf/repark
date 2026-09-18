# map — repark-core/src/range_table

## Purpose

File-backed tests for the Spark-shaped `range` table function
(`../range_table.rs`): schema shape, argument forms, coercion, and refusals.

## Contents

- `tests.rs` — provider pins over a default `ReparkSession`
  (`#[cfg(test)] mod tests;` in `../range_table.rs`).
  pins: range-tvf-id-1/C-001, C-002, C-003
- `../range_table.rs` — `SparkRangeFunc` (RePark's `range`, column `id`) plus
  `register_spark_range`, wired once in `ReparkSessionBuilder::build` so the
  facade door and the native door share the registration. Row generation
  reuses DataFusion's `GenerateSeriesTable` with `include_end = false`;
  `generate_series` is untouched.

## Pointers

- Up: [../map.md](../map.md)
