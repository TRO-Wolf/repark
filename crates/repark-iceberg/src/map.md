# map — repark-iceberg/src

## Purpose

Source for `repark-iceberg` — two independent module trees ported from v1 crates. See
[../map.md](../map.md).

## Contents

- `lib.rs` — thin manifest (WI-2 2026-08-15 added `InsertStoreAssignment` to the `write`
  re-export block — the plain-INSERT store-assignment `AnalyzerRule` the Spark door registers): `pub mod catalog; pub mod write;` + the union of the two v1
  crate-root re-export lists (public names unchanged).
- `catalog/` — Glue + S3 Tables + memory catalog builders, DataFusion `CatalogProvider`
  registration, scheme-based `FileIO` selection, and the hoisted `reregister_catalog_provider`
  session-refresh adapter (`catalog_ops.rs`). See [catalog/map.md](catalog/map.md).
- `write/` — MERGE INTO / identity DELETE+UPDATE (`predicate_dml`) / append / overwrite / ALTER /
  snapshot refs over the owned fork. See [write/map.md](write/map.md).
- `fork_pin_tests.rs` — `cfg(test)`-only fork-pin proof (ADR-0001): names + exercises
  fork-only public API (`iceberg::plan_commit_base_load` / `CommitBaseLoadPlan`), so the test
  target compile-fails on a silent crates.io registry fallback.
- `test_tracing.rs` — `cfg(test)`-only shared tracing harness (one global subscriber, both
  cohorts' capture layers; forced-edit class 6 — see docs/design/session-api.md §5).

## I want to...

| ...do this | go to |
|---|---|
| Catalog wiring | [catalog/map.md](catalog/map.md) |
| Write paths | [write/map.md](write/map.md) |
| Re-export surface | `lib.rs` |
| Span-capture in tests records nothing | `test_tracing.rs` (one global subscriber; install via its accessors, never `set_global_default` directly) |

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-iceberg`. Escalate per module tree:
[catalog/map.md#debug](catalog/map.md) / [write/map.md#debug](write/map.md).
