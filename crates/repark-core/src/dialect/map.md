# map — repark-core/src/dialect

## Purpose

File-backed tests for the SQL dialect seam (`../dialect.rs`): `DataFusionDialect` passthrough
and the `EngineContext` construction contract (new-seam tests, additive — not part of the ported
v1 census).

## Contents

- `tests.rs` — passthrough + explicit-field-construction pins (`#[cfg(test)] mod tests;` in
  `../dialect.rs`).

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A statement the Spark door handled fails here | Phase-1 default is plain DataFusion (`DataFusionDialect`); Spark interception returns as the phase-2 dialect impl. |

First checks: `cargo test -p repark-core dialect`. Escalate to: [../map.md#debug](../map.md).
