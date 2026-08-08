# map — repark-sql/src/refusals

## Purpose

File-backed tests for `../refusals.rs` — the refuse set.

Every refusal is a behavior. Each message class is pinned by the property that makes it useful —
it names the shape, and it names what to do instead — not by its exact wording.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../refusals.rs`.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A refusal message changed and a test broke | the assertions pin the STEER and the trigger, not the prose; if the steer changed, the ruling changed too |
| The `EXECUTE` recognizer fired on a supported ALTER | `alter_execute_recognizer_does_not_fire_on_other_statements` enumerates the shapes it must ignore |

First checks: `cargo test -p repark-sql refusals::`. Escalate to: [../map.md#debug](../map.md).
