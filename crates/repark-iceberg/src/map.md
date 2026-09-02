# map — repark-iceberg/src

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Source for `repark-iceberg` — two independent module trees ported from v1 crates. See
[../map.md](../map.md).
Source comments are condensed to API and safety contracts; executable behavior is unchanged.

## Contents

- `lib.rs` — thin manifest (WI-2 2026-08-15 added `InsertStoreAssignment` to the `write`
  re-export block — the plain-INSERT store-assignment `AnalyzerRule` the Spark door registers;
  V3-1 re-exports `iceberg_to_datafusion` so the Spark CALL router shares the iceberg error
  fold): `pub mod catalog; pub mod write;` + the union of the two v1
  crate-root re-export lists (public names unchanged except that one added mapper).
- `catalog/` — Glue + S3 Tables + memory catalog builders, DataFusion `CatalogProvider`
  registration, scheme-based `FileIO` selection, the hoisted `reregister_catalog_provider`
  session-refresh adapter (`catalog_ops.rs`), and V3-4 current-snapshot
  `lineage_columns.rs` (filters through; `V3-ROWID-2` for time-travel). See
  [catalog/map.md](catalog/map.md).
- `write/` — MERGE INTO / identity DELETE+UPDATE (`predicate_dml`) / append / overwrite /
  partition overwrite (DML-B) / ALTER /
  snapshot refs over the owned fork. Named-ref commits use `commit_target` / `to_branch`.
  See [write/map.md](write/map.md).
  pins: rp-5-fork-repin/C-004
- [tests/](tests/map.md) — crate-root test modules: fork-pin proof, shared tracing harness,
  and the R91 unknown-write refuse pin.
  pins: rp-5-fork-repin/C-006

## I want to...

| ...do this | go to |
|---|---|
| Catalog wiring | [catalog/map.md](catalog/map.md) |
| Write paths | [write/map.md](write/map.md) |
| Re-export surface | `lib.rs` |
| Span-capture in tests records nothing | `tests/tracing.rs` (one global subscriber; install via its accessors, never `set_global_default` directly) |

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-iceberg`. Escalate per module tree:
[catalog/map.md#debug](catalog/map.md) / [write/map.md#debug](write/map.md).
