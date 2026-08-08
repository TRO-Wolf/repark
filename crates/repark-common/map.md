# map — repark-common

## Purpose

Shared error-seed types, the workspace-wide `Error` enum, and the dialect-neutral SQL **surface
registry** both SQL doors are audited against. Depends on nothing else in the
workspace — the bottom of the dependency DAG, which is what keeps the higher engine crates
acyclic. (Ported from the private v1 repository's error-seed crate; renamed because this
workspace reserves `repark-core` for the Session crate.)

## Contents

- `Cargo.toml` — package + `thiserror` dep.
- `src/lib.rs` — `Error` (typed, `#[non_exhaustive]`), `ErrorClass`, and the `Result<T>` alias
  shared by the workspace. **Error-boundary honesty (C1-CRATE-001):** not every crate returns
  `repark_common::Error` end-to-end — intermediate layers still surface `iceberg::Result` /
  `DataFusionError` and fold at the session/PyO3 boundary via `engine_err` / `iceberg_err`.
- `src/surfaces.rs` (+ `src/surfaces/tests.rs`) — the surface registry: the 43-ID capability
  vocabulary, `Row` / `SessionProfile`, and the `audit()` each door's `matrix.rs` runs as a
  compile-run test (design `docs/design/sql-doors.md` §2 Q13, graft G2). Tier 0 so neither
  door needs an edge to the other.
- `src/tests.rs` — file-backed test module: the exhaustive `exception_class` routing pin and the
  message-preservation pin.

## I want to...

| ...do this | go to |
|---|---|
| Add an error variant | `src/lib.rs` (keep variants specific; no catch-all `String`) |
| Add a shared domain type | `src/lib.rs` (it must not pull in heavier crates) |
| Add / rename a SQL surface ID | `src/surfaces.rs` (const + `ALL`), the inventory in `src/surfaces/tests.rs`, then a row in EACH door's `matrix.rs` |

## Pointers

- Up: [../map.md](../map.md)
- Related: every other crate re-exports `Error` / `Result` from here.

## Debug

| Symptom | First check |
|---|---|
| Circular dependency | `repark-common` must depend on no sibling crate |
| A door's `matrix_maps_every_surface` went RED | a `surfaces::ALL` entry has no row in that door — the audit error names the ID |

First checks: `cargo check -p repark-common`. Escalate to: [../map.md#debug](../map.md).
