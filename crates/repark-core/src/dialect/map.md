# map — repark-core/src/dialect

## Purpose

File-backed tests for the SQL dialect seam (`../dialect.rs`): `DataFusionDialect` passthrough
and the `EngineContext` construction contract.

## Contents

- `tests.rs` — passthrough + explicit-field-construction pins (`#[cfg(test)] mod tests;` in
  `../dialect.rs`).
- `../dialect.rs` — `SqlDialect::on_session_built` (default no-op) runs from
  `ReparkSessionBuilder::build` after extension `register`. AnsiDialect installs
  F-Y10-1 integer overflow there. pins: f-y10-1-int-overflow/C-003
  **ICE-DYN-OVERWRITE-1 round 2, ruling Q-20a-6 (2026-09-17):** `EngineContext`
  carries `force_static_overwrite` (default false in `new`; the literals here set
  it explicitly). pins: ice-dyn-overwrite-1/L-001

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A statement the Spark door handled fails here | Phase-1 default is plain DataFusion (`DataFusionDialect`); Spark interception lives in `repark-spark`'s `SparkDialect`. |
| Downstream dialect crate can't build an `EngineContext` | `EngineContext` is `#[non_exhaustive]`; construct via `EngineContext::new` (pinned by `engine_context_new_is_the_downstream_constructor`). |

First checks: `cargo test -p repark-core dialect`. Escalate to: [../map.md#debug](../map.md).

**ICE-TT-RESOLVE-1 (2026-09-19):** the dialect context carries `session_time_zone` so time-travel
resolution sees the statement-time zone. pins: ice-tt-resolve-1/C-003
**ICE-TT-RESOLVE-1 round 2 (2026-09-19):** `new` is 3-arg again (zone defaults);
`new_with_time_zone` carries an explicit zone. pins: ice-tt-resolve-1/C-003
