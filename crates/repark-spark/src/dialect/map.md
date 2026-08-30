# map — repark-spark/src/dialect

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

File-backed tests for `SparkDialect` (`../dialect.rs`): installed on a `ReparkSession` via
`with_sql_dialect`, the seam adapter routes `sql()` through the router. Spark ORDER BY defaults
and targeted refusals survive the session error fold; the refusal probe pins TRUNCATE (C4-L-001).

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../dialect.rs`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| Spark defaults absent through the session | The builder must install BOTH `SparkDialect` (routing) and `SparkExtension` (registrations) |

First checks: `cargo test -p repark-spark dialect::`. Escalate to: [../map.md#debug](../map.md).
