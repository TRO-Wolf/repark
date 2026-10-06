# map — repark-connect/tests

## Purpose

Integration tests for `repark-connect`: one binary, `it`, per the layout of record (every crate
carries one `tests/it/main.rs`, so `src/` holds product code only). See [../map.md](../map.md).

## Contents

- `it/` — [it/map.md](it/map.md): the integration binary and its modules.

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-connect --test it`. Escalate to: [../map.md#debug](../map.md).
