# map — repark-common

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

## Purpose

Shared error-seed types, the workspace-wide `Error` enum, and the dialect-neutral SQL **surface
registry** both SQL doors are audited against (crate-DAG **tier 0**, the foundation). Depends on
nothing else in the workspace — the bottom of the dependency DAG, which is what keeps the higher
engine crates acyclic. (Ported from the private v1 repository's error-seed crate; renamed because this
workspace reserves `repark-core` for the Session crate.)

## Contents

- `Cargo.toml` — package + `thiserror` dep.
- `src/lib.rs` — `Error` (typed, `#[non_exhaustive]`), `ErrorClass`, and the `Result<T>` alias
  shared by the workspace. **Error-boundary honesty (C1-CRATE-001):** not every crate returns
  `repark_common::Error` end-to-end — intermediate layers still surface `iceberg::Result` /
  `DataFusionError` and fold at the session/PyO3 boundary via `engine_err` / `iceberg_err`.
- `src/surfaces.rs` (+ `src/surfaces/tests.rs`) — the surface registry: the 50-ID capability
  vocabulary (43 statement/DDL/guard/ergonomic IDs + the 7 `SEMANTICS_*` value-semantics IDs
  from H-2 G8), `Row` / `SessionProfile`, and the `audit()` each door's `matrix.rs` runs as a
  compile-run test (design `docs/design/sql-doors.md` §2 Q13, graft G2). Tier 0 so neither
  door needs an edge to the other.
- `src/tests.rs` — file-backed test module: the exhaustive `exception_class` routing pin and the
  message-preservation pin.

## I want to...

| ...do this | go to |
|---|---|
| Add an error variant | `src/lib.rs` (keep variants specific; no catch-all `String`) |
| Add a shared domain type | `src/lib.rs` (it must not pull in heavier crates) |
| Add / rename a SQL surface ID | `src/surfaces.rs` (const + `ALL`), the inventory in `src/surfaces/tests.rs`, then a row in EACH door's `matrix.rs`. A `Tested` cite must be a live `cargo test -- --list` name (`make check-matrix-test-liveness`) |

## Component contract

- **Owns:** the workspace `Error` / `ErrorClass` / `Result` seed; the dialect-neutral SQL **surface
  registry** (`surfaces`: the 50-ID capability vocabulary, `Row` / `SessionProfile`, `audit()`).
- **Does not own:** any engine / session / IO logic; error *folding* (that happens at the
  session / PyO3 boundary); door-specific matrices (each door owns its `matrix.rs`).
- **Public inputs:** none at runtime — a leaf of pure types; doors call `surfaces::audit()` in tests.
- **Public outputs:** `Error` / `ErrorClass` / `Result`, re-exported workspace-wide; the `surfaces`
  vocabulary + `audit()`.
- **State & lifecycle:** stateless — pure value types + const tables.
- **Allowed internal deps:** none (bottom of the DAG). Third-party: `thiserror` only.
- **Failure model:** defines the taxonomy; typed `Error` variants, no catch-all `String`. Not every
  crate returns it end-to-end — intermediate layers surface `iceberg::Result` / `DataFusionError`
  and fold higher (C1-CRATE-001).
- **Extension points:** add an `Error` variant / shared seed type; add a SQL surface ID (const +
  `ALL` + a row in each door's `matrix.rs`).
- **Test strategy:** file-backed `tests.rs` — exhaustive exception-class routing + message-preservation
  pins; `surfaces/tests.rs` inventory audit.
- **Known limitations:** must stay dependency-light — pulling in a heavier crate would risk
  reintroducing a cycle. **Name liveness is a harness-level gate, not `audit()`:** each door's
  `matrix.rs` audit runs *inside* a Rust test binary and cannot enumerate cargo test names.
  `make check-matrix-test-liveness` (`scripts/check_matrix_test_liveness.py`, in `make ci` /
  `make preflight`, dual-wired with the ci.yml rust-test job) diffs every `Tested` cite
  against `cargo test --locked --workspace --lib --tests --bins -- --list`. A row citing a
  dead name reds that gate. `audit()` still catches an unmapped, stale, duplicated or
  untraceable ID.

## Pointers

- Up: [../map.md](../map.md)
- Related: every other crate re-exports `Error` / `Result` from here.

## Debug

| Symptom | First check |
|---|---|
| Circular dependency | `repark-common` must depend on no sibling crate |
| A door's `matrix_maps_every_surface` went RED | a `surfaces::ALL` entry has no row in that door — the audit error names the ID |
| `matrix-test-liveness` RED | a `Tested` cite is missing from `cargo test -- --list` — `make check-matrix-test-liveness` |

First checks: `cargo check -p repark-common`. Escalate to: [../map.md#debug](../map.md).
