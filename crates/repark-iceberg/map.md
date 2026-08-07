# map — repark-iceberg

## Purpose

The Iceberg surface, merged from two v1 crates as two independent module trees:

- `src/catalog/` — build AWS Glue (primary) and S3 Tables (secondary) Iceberg catalogs and
  register them as DataFusion `CatalogProvider`s, so three-part names
  (`glue_catalog.namespace.table`) resolve with zero translation. The **only** module tree that
  depends on the AWS SDK.
- `src/write/` — the **thin Spark-semantics write adapter** over the owned iceberg-rust fork
  (ADR: the heavy engine lives in the fork, not here): `ALTER TABLE` primitives, the
  RePark-owned **MERGE INTO** executor (copy-on-write AND merge-on-read, per the fork's
  ENGINE_CONTRACT §6), the public bulk `append`, and the stage-then-swap `INSERT OVERWRITE`
  commit. `DELETE`/`UPDATE`/`INSERT` need no adapter — DataFusion plans them onto the fork's
  `iceberg-datafusion` `TableProvider`.

Public names are unchanged from v1: `repark_catalog::X` → `repark_iceberg::catalog::X`,
`repark_write::Y` → `repark_iceberg::write::Y`; the crate root re-exports the union of the two
v1 crate-root re-export lists.

## Contents

- `Cargo.toml` — union of the two v1 manifests: `repark-common` (error seed re-exported by the
  write half; the catalog half stays `datafusion::error::Result` — the fold lives in
  repark-core) + `iceberg` + `iceberg-datafusion` + `iceberg-catalog-glue` +
  `iceberg-catalog-s3tables` + `iceberg-storage-opendal` (`opendal-s3`) + `datafusion` +
  `parquet` + `async-trait` + `futures`/`uuid` + `tracing`. Dev-deps `tokio` + `tempfile` +
  `tracing-subscriber` (registry). The `iceberg*` family is sourced from the owned fork via the
  workspace `[patch.crates-io]`.
- `src/lib.rs` — thin manifest: `pub mod catalog; pub mod write;` + the union re-export lists
  (+ the file-backed `#[cfg(test)] mod test_tracing;`).
- `src/test_tracing.rs` — shared test-only tracing harness (forced-edit class 6): ONE global
  subscriber carrying both v1 capture layers (catalog span-field capture + merge span-name
  recorder), installed once via a tolerant `Once`; accessors used by `catalog/tests.rs` and
  `write/merge/streaming_scan_tests.rs`.
- `src/catalog/`, `src/write/` — see [src/map.md](src/map.md) and the per-module maps.

## I want to...

| ...do this | go to |
|---|---|
| Register an Iceberg catalog / list live names / build a provider snapshot | [src/catalog/map.md](src/catalog/map.md) |
| Get an AWS-free catalog for local dev / tests | `memory_catalog(warehouse)` in `src/catalog/` |
| Build the Glue (primary) or S3 Tables (secondary) catalog | `glue_catalog` / `s3tables_catalog` in `src/catalog/` |
| MERGE INTO / append / overwrite / ALTER / snapshot refs | [src/write/map.md](src/write/map.md) |
| Wire DELETE/UPDATE/INSERT OVERWRITE | nothing here — DataFusion plans them onto the fork's `TableProvider` |
| Change credential handling | not here — AWS SDK default chain *inside the fork* |

## Pointers

- Up: [../map.md](../map.md)
- Related: repark-core registers the catalog providers and installs the write knobs.

## Debug

| Symptom | First check |
|---|---|
| Catalog registration / listing / staleness issues | [src/catalog/map.md](src/catalog/map.md#debug) |
| MERGE / append / overwrite / ALTER issues | [src/write/map.md](src/write/map.md#debug) |
| Fork-pin doubt (crates.io fallback?) | `src/fork_pin_tests.rs` (exercises fork-only `plan_commit_base_load`) + the ported name-only proof in `src/catalog/tests.rs` — neither compiles against crates.io iceberg 0.9.1 |

First checks: `cargo test -p repark-iceberg` (all AWS-free on `MemoryCatalog`). Escalate to:
[../map.md#debug](../map.md).
