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
  **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** also re-exports the session-write-conf
  surface (`SessionWriteView`, `resolve_write_for_session`, the `session_write_conf_from_*`
  readers, `with_session_write_conf`).
- `lib.rs` — **CAST-OVERFLOW-INSERT-1 (2026-09-29):** also re-exports `StoreOverflowCast`,
  the fractional-store-overflow `AnalyzerRule` the Spark door registers.
- `catalog/` — Glue + S3 Tables + memory catalog builders, DataFusion `CatalogProvider`
  registration, scheme-based `FileIO` selection, the hoisted `reregister_catalog_provider`
  session-refresh adapter (`catalog_ops.rs`), and V3-4 current-snapshot
  `lineage_columns.rs` (filters through; `V3-ROWID-2` for time-travel). Since
  **PERF-ICE-CATALOG-IO-1** it also owns `caches.rs`: the session-scoped `CatalogCaches` handed
  to every catalog this session builds, and the `repark.iceberg.*` knobs that shape them (two in
  IO-1, three since **PERF-ICE-CATALOG-IO-2** added `manifestCacheBytes`). See
  [catalog/map.md](catalog/map.md).
  **ICE-FOOTER-CACHE-1 (2026-09-19):** a fourth knob, `repark.iceberg.footerCacheBytes` (default
  64 MiB, `0` disables), sizes one session-owned `ParquetFooterCache` that every catalog builder
  receives.
- `write/` — MERGE INTO / identity DELETE+UPDATE (`predicate_dml`) / append / overwrite /
  partition overwrite (DML-B) / ALTER /
  snapshot refs over the owned fork. Named-ref commits use `commit_target` / `to_branch`.
  See [write/map.md](write/map.md).
- [microbatch/](microbatch/map.md) — **MB-1 round 2 (2026-10-07):** the
  micro-batch track's offsets, identifiers, offset JSON, the track's one
  error enum (`MicroBatchError`), and the window planner plus the batch
  provider over the incremental append scan.
  pins: mb-1/C-007, mb-1/C-013
- [view/](view/map.md) — **ICE-VIEWS-1 (2026-09-20):** the view service over the
  fork's `Catalog` view methods (`create_or_replace_view`, `drop_catalog_view`,
  `list_catalog_views`, schema/property/location helpers, `view_read_spec`).
- [tests/](tests/map.md) — crate-root test modules: fork-pin proof, shared tracing harness,
  and the R91 unknown-write refuse pin.

## I want to...

| ...do this | go to |
|---|---|
| Catalog wiring | [catalog/map.md](catalog/map.md) |
| Write paths | [write/map.md](write/map.md) |
| Micro-batch offsets, window, and provider | [microbatch/map.md](microbatch/map.md) |
| View service | [view/map.md](view/map.md) |
| Re-export surface | `lib.rs` |
| Span-capture in tests records nothing | `tests/tracing.rs` (one global subscriber; install via its accessors, never `set_global_default` directly) |

## Pointers

- Up: [../map.md](../map.md)

## Debug

First checks: `cargo test -p repark-iceberg`. Escalate per module tree:
[catalog/map.md#debug](catalog/map.md) / [write/map.md#debug](write/map.md).
