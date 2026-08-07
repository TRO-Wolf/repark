# map — repark-spark/src/extension

## Purpose

File-backed tests for `SparkExtension` (`../extension.rs`): the `configure` hook's
`repark.sql.*` `ConfigExtension` install (r24 SB1 re-home, incl. the fail-loud unparsable-value
contract) and the `register` hook's function-registry + analyzer-rule installation.

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../extension.rs`.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A `repark.sql.*` key silently ignored | `configure` parses the builder conf map — key spelling vs `repark_functions::cardinality` consts |

First checks: `cargo test -p repark-spark extension::`. Escalate to: [../map.md#debug](../map.md).
