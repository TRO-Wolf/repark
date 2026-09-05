# map — repark-python/tests

## Purpose

Rust integration tests exercise the public PyO3 binding boundary with an embedded interpreter.
They cover session construction, SQL, DataFrame actions, joins, windows, ML functions, and Arrow
C Stream value, type, and laziness behavior.

## Files

- [`bindings.rs`](bindings.rs) contains the integration suite and Arrow stream helpers.

## Contracts pinned

- Builder knobs reject invalid zero values and config-driven memory catalogs register at build time.
- Spark and native session doors expose their distinct function and SQL behavior.
- Arrow export uses the required capsule name, preserves values and types, and defers execution
  errors until the stream is drained.
- DataFrame joins preserve Spark key and semi/anti output schemas; windows and aggregates retain
  their facade semantics.
- **TYPES-1 (2026-09-05):** literal-built columns read as `Int32` (Spark int); `sum` still
  answers `Int64`. pins: types-1/C-001, C-004
- Unsupported readers return named typed refusals without leaking credentials.
- Panic fences return catchable exceptions and leave the interpreter usable.

## Verification

Run `cargo test -p repark-python --quiet` with `extension-module` disabled. The suite requires the
embedded Python interpreter and must not use `--all-features`.

## Pointers

- Up: [src map](../src/map.md)
- Crate: [repark-python map](../map.md)
