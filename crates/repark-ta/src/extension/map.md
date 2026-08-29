# map — repark-ta/src/extension

## Purpose

DataFusion-only tests for `TaExtension` (`../extension.rs`). Registration must expose the complete
UDF set and match kernel bits. The default `configure` hook must pass configuration through.

Both live behind the `datafusion` feature — the module does not exist without it.

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../extension.rs`.

## Pointers

- Up: [../map.md](../map.md)
- The seam being implemented: `repark-core/src/extension.rs` (`SessionExtension`).
- The consumer: `repark-spark/src/extension.rs` (`SparkExtension` composes `TaExtension`).

## Debug

| Symptom | First check |
|---|---|
| `ta_ema` unknown after `register` | The kernel spec table in `../udf/mod.rs` (`SPECS`) — `register_all` iterates `window_udfs()`, so a missing name is a missing spec row, not an extension bug |
| Bit mismatch vs the kernel | An engine/UDF regression — never edit the assertion. Reproduce with the goldens battery: `cargo test -p repark-ta --features datafusion` |
| `configure` test fails | Someone gave `TaExtension` a `configure` override; TA installs no `ConfigExtension` by design (design Q11 "register-only") |
| `configure` test stops compiling | Check the current `SessionExtension::configure` signature; the pass-through assertion must remain unchanged in meaning |

First checks: `cargo test -p repark-ta --features datafusion extension::`. Escalate to:
[../map.md#debug](../map.md).
