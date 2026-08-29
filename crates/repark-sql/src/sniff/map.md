# map — repark-sql/src/sniff

## Purpose

File-backed tests for `../sniff.rs`. Each recognized Spark-ism has a row. The tests pin the
message, original-error preservation, non-Spark immunity, and literal/comment immunity.

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
