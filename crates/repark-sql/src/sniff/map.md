# map — repark-sql/src/sniff

## Purpose

File-backed tests for `../sniff.rs`. The error-path wrong-door sniff. Each recognized Spark-ism has a row (the MESSAGE is the
product here), plus the three properties the error-path placement is chosen for: the original
error survives, non-Spark SQL is untouched, and literals/comments cannot trigger it.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../sniff.rs`.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A working statement got a Spark steer | impossible by construction — the sniff only runs after a failure; check the underlying error |
| A FAILING but ANSI-legal statement got a Spark steer | `../sniff.rs` `scope_for` / `Scope::Leading` — `USING`, `NAMESPACE`, `DATABASE`, `BRANCH`/`TAG` and `CALL` fire only under their leading keyword; `ansi_legal_statements_are_never_steered_to_the_spark_door` is the pin |

First checks: `cargo test -p repark-sql sniff::`. Escalate to: [../map.md#debug](../map.md).
