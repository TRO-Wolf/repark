# map — repark-core/src/range_table

## Purpose

File-backed tests for the Spark-shaped `range` table function
(`../range_table.rs`): schema shape, argument forms, coercion, refusals, and
the overflow-safe row stream.

## Contents

- `tests.rs` — provider pins over a default `ReparkSession`
  (`#[cfg(test)] mod tests;` in `../range_table.rs`).
  pins: range-tvf-id-1/C-001, C-002, C-003
  pins: range-tvf-id-2/C-001, C-002, C-003, C-004
- `../range_table.rs` — `SparkRangeFunc` (RePark's `range`, column `id`) plus
  `register_spark_range`, wired once in `ReparkSessionBuilder::build` so the
  facade door and the native door share the registration. NULL bounds refuse
  with `UNEXPECTED_INPUT_TYPE`; every Spark-accepted width coerces to `i64`
  with `CAST_INVALID_INPUT` on malformed strings; `RangeTable` with a
  `RangePartition` stream over `StreamingTableExec` emits exactly Spark's
  `i128` element count with session batch-size batches, projection and limit
  support; a non-positive `numPartitions` on a non-empty range refuses with
  `IllegalArgumentException`. `generate_series` is untouched.

## Pointers

- Up: [../map.md](../map.md)
