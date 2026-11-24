# map — repark-spark/src/dialect

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

File-backed tests for `SparkDialect` (`../dialect.rs`): installed on a `ReparkSession` via
`with_sql_dialect`, the seam adapter routes `sql()` through the router. Spark ORDER BY defaults
and targeted refusals survive the session error fold; the TRUNCATE probe pins
`TABLE_OR_VIEW_NOT_FOUND` for a missing table (DML-C).

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../dialect.rs` (`dialect_execute_runs_the_spark_router`,
  `dialect_surfaces_router_refusals`, `spark_dialect_respells_minus_all_after_spark_normalizers`,
  and `spark_dialect_respells_minus_all_in_values_ctas_and_insert_by_name`).

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| Spark defaults absent through the session | The builder must install BOTH `SparkDialect` (routing) and `SparkExtension` (registrations) |

First checks: `cargo test -p repark-spark dialect::`. Escalate to: [../map.md#debug](../map.md).
