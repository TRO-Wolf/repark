# map — repark-sql/src/refusals

## Purpose

File-backed tests for `../refusals.rs` — the refuse set.

Every refusal is a behavior. Each message class is pinned by the property that makes it useful —
it names the shape, and it names what to do instead — not by its exact wording.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../refusals.rs`.
  **V3-1:** the Q7 CALL pin uses `ice.system.register_table` so the ANSI-door refusal is
  named for the new procedure, not only `rewrite_data_files`.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A refusal message changed and a test broke | the assertions pin the STEER and the trigger, not the prose; if the steer changed, the ruling changed too |
| The `EXECUTE` recognizer fired on a supported ALTER | `alter_execute_recognizer_does_not_fire_on_other_statements` enumerates the shapes it must ignore; `alter_execute_recognizer_is_anchored_to_the_verb_slot` pins that a COLUMN named `execute` is legal |
| The `EXECUTE` recognizer missed a real `ALTER … EXECUTE` | The verb slot is found by walking the dotted/quoted name — `alter_execute_recognizer_finds_the_verb_after_any_name_spelling` covers each spelling |

First checks: `cargo test -p repark-sql refusals::`. Escalate to: [../map.md#debug](../map.md).
