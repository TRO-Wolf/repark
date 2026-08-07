# map — repark-spark/src/dialect

## Purpose

File-backed tests for `SparkDialect` (`../dialect.rs`): installed on a `ReparkSession` via
`with_sql_dialect`, the seam adapter routes `sql()` through the ported router (Spark ORDER BY
defaults observable end to end; refuse arms survive the session error fold).

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../dialect.rs`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| Spark defaults absent through the session | The builder must install BOTH `SparkDialect` (routing) and `SparkExtension` (registrations) |

First checks: `cargo test -p repark-spark dialect::`. Escalate to: [../map.md#debug](../map.md).
