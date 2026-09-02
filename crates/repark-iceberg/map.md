# map — repark-iceberg

## Purpose

The Iceberg surface (crate-DAG **tier 1**, the table service both doors reach through the
session), merged from two v1 crates as two independent module trees:

- `src/catalog/` — build AWS Glue (primary) and S3 Tables (secondary) Iceberg catalogs and
  register them as DataFusion `CatalogProvider`s, so three-part names
  (`glue_catalog.namespace.table`) resolve with zero translation. The **only** module tree that
  depends on the AWS SDK.
- `src/write/` — the **thin Spark-semantics write adapter** over the owned iceberg-rust fork
  (ADR: the heavy engine lives in the fork, not here): `ALTER TABLE` primitives, the
  RePark-owned **MERGE INTO** executor (copy-on-write AND merge-on-read, per the fork's
  ENGINE_CONTRACT §6), the public bulk `append`, the stage-then-swap `INSERT OVERWRITE`
  commit, and partition-scoped overwrite (DML-B). `DELETE`/`UPDATE`/`INSERT` need no adapter —
  DataFusion plans them onto the fork's `iceberg-datafusion` `TableProvider`.

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
  (+ the file-backed `#[cfg(test)] mod tests;`).
- `src/tests/tracing.rs` — shared test-only tracing harness (forced-edit class 6): ONE global
  subscriber carrying both v1 capture layers (catalog span-field capture + merge span-name
  recorder), installed once via a tolerant `Once`; accessors used by `catalog/tests/catalog.rs` and
  `write/merge/tests/streaming_scan.rs`.
- `src/catalog/`, `src/write/` — see [src/map.md](src/map.md) and the per-module maps.

## I want to...

| ...do this | go to |
|---|---|
| Register an Iceberg catalog / list live names / build a provider snapshot | [src/catalog/map.md](src/catalog/map.md) |
| Get an AWS-free catalog for local dev / tests | `memory_catalog(warehouse)` in `src/catalog/` |
| Build the Glue (primary) or S3 Tables (secondary) catalog | `glue_catalog` / `s3tables_catalog` in `src/catalog/` |
| MERGE INTO / append / overwrite / partition overwrite / ALTER / snapshot refs | [src/write/map.md](src/write/map.md) |
| Identity DELETE/UPDATE (subquery `WHERE`) | `src/write/predicate_dml.rs` |
| Wire ordinary DELETE/UPDATE/INSERT OVERWRITE | DataFusion → fork `TableProvider` (non-subquery) |
| Change credential handling | not here — AWS SDK default chain *inside the fork* |

## Component contract

- **Owns:** the Iceberg surface — Glue (primary) + S3 Tables (secondary) catalog wiring for
  DataFusion (`catalog/`); the thin Spark-semantics write adapter over the owned fork (`write/`:
  RePark-owned MERGE INTO, identity DELETE (`predicate_dml`), bulk `append`, stage-then-swap
  `INSERT OVERWRITE`, partition-scoped overwrite (DML-B), `ALTER` primitives); the
  `[patch.crates-io]` fork-pin consumers.
- **Does not own:** the table-format engine (write actions, evolution, snapshots, maintenance — those
  live **in the fork**); ordinary (non-subquery) DELETE / UPDATE / INSERT (planned onto the fork's
  `TableProvider`); the session + error fold (repark-core).
- **Public inputs:** a DataFusion `SessionContext` + catalog config; write plans / batches from the
  session; MERGE / append / overwrite calls.
- **Public outputs:** registered `CatalogProvider`s (three-part names resolve); committed snapshots;
  the union of the two v1 crates' re-exports (`catalog::*`, `write::*`).
- **State & lifecycle:** catalog handles held per session; writes are optimistic-commit transactions
  over the fork; no long-lived mutable global.
- **Allowed internal deps:** `repark-common` (write half). The catalog half is the **only** module
  tree that depends on the AWS SDK. Third-party: the `iceberg*` fork family + datafusion + parquet.
- **Failure model:** catalog half stays `datafusion::error::Result`; write half re-exports
  `repark_common::Error`; the fold to the session taxonomy lives in repark-core.
- **Extension points:** add a catalog builder (`src/catalog/`); add / adjust a write action
  (`src/write/`, minding the fork `ENGINE_CONTRACT`). MERGE stays RePark-owned.
- **Test strategy:** `cargo test -p repark-iceberg` — all AWS-free on `MemoryCatalog`; fork-pin proofs
  that will not compile against crates.io iceberg 0.9.1.
- **Known limitations:** the merge-on-read unpartitioned multi-spec edge is guarded loud (a fork
  fast-path defect); credential handling lives inside the fork. Two fork-coupled gaps are carried
  deliberately, and **both are re-verified at every fork repin**
  ([../../AGENTS.md](../../AGENTS.md) "Version-pin contract"):
  - **`NamespaceScopedCatalog` (in `src/catalog/provider.rs`) both-sides trait-wrapping audit
    (G17) is CLOSED.** At fork pin `fb0cacfa` (RP-6, 2026-09-01; unchanged from RP-5's
    `00cdde0`, RP-4's `33be9a0`, RP-3's `d408da42`, RP-2's `ce92a7bf`, and RP-1's
    `5e7b2e4`) the `Catalog` trait has 14 required + **16 defaulted** methods; no
    method was added or removed in `00cdde0..fb0cacfa`. The wrapper explicitly forwards all 14 required methods
    (with `list_namespaces` filtered to one namespace) and **13 of 16** defaulted
    methods — including the HIGH `publish_replace_table` (whose trait default is
    `FeatureUnsupported` and would swallow `MemoryCatalog`'s CAS replace). The
    remaining **3** defaulted methods (`update_namespace_properties` /
    `set_namespace_properties` / `remove_namespace_properties`) are **stated
    omissions**: the trait defaults compose only from methods already forwarded.
    Pins live in `src/catalog/tests/namespace_scoped.rs`. **Repin duty:**
    re-enumerate the fork trait surface; a method that newly gains a real override
    (no longer a pure composition default) becomes an explicit forward.
    pins: rp-1-fork-repin/C-001, C-002, C-003
    pins: rp-3-fork-repin/C-001, C-002
    pins: rp-4-fork-repin/C-001, C-002
    pins: rp-5-fork-repin/C-001, C-002
    pins: rp-6-fork-repin/C-001, C-002, C-006
  - **The metadata-projection shim retired at RP-5 (fork R169/R170, F-8 `#247`).** The
    fork's metadata-table `scan` honors `projection` (empty projection included) and
    `table_names` lists catalog entries only (`$`-twins are not enumerated). Engine
    `catalog/metadata_projection.rs` is deleted. Pins
    `crates/repark-spark/src/tests/metadata_tables.rs::metadata_tables_are_hidden_from_enumeration_but_stay_queryable_through_the_spark_door`
    and `::metadata_table_projection_honor_all_types` now pin the fork.
    pins: rp-5-fork-repin/C-003
  - **`IcebergSchemaProvider` name-directory population is still lazy at pin `fb0cacfa`
    (RP-6 re-verified 2026-09-01; first measured at `5e7b2e4`).** `try_new`
    no longer `list_tables`; first `table` / `table_names` / `table_exist` lists live and
    then freezes. `ReparkCatalogProvider` eager-lists at snapshot and namespace-refresh
    (`freeze_fork_name_directory` in `src/catalog/provider.rs`) so an out-of-band create
    stays invisible to free SQL until invalidate (ADR-0004 T6). Pins:
    `full_rebuild_lists_every_namespace`,
    `incremental_provider_preserves_oob_staleness_residual`,
    `empty_invalidate_is_noop_not_full_rebuild`,
    `rebuild_same_catalog_heals_oob_and_stays_repark_provider`.
    **Repin duty:** if a future rev lists at construction again the freeze is a no-op; if a
    rev lists on every access (never freezes) those four pins fail-closed.
    pins: rp-1-fork-repin/C-011
  - **R91 unknown-on-write (RP-5 C-006).** Fork `#246` falsified the V3-6 pin that
    a Null `unknown` column commits then fails at scan. The pin now asserts the
    parquet write refuses `Writing the unknown column 'u' is not supported yet`.
    CREATE refusal stands. No new surface.
    pins: rp-5-fork-repin/C-006
  - **RP-5 document lockstep (C-008).** Pin history, registry REF-1 FIXED / REF-3 BACKLOG /
    RDF-1 BACKLOG, and handoff F-6b/F-6c/F-8/F-16r/F-0 consumed notes match the pins.
    pins: rp-5-fork-repin/C-008
  - **RDF-1 document lockstep (C-004).** Registry `RDF-1` FIXED with the measured bounds and
    counts on both engines; the guide's "What the cycle cannot reclaim" now states what IS
    reclaimed and names the residue; north star §3b errata and `docs/design/map.md`; handoff
    F-16 residue 2 re-homed (the bounds half was RePark's, refuted fork-side in `#259`);
    `task/roadmap/mid-term/map.md`. STATUS.md does not name `RDF-1` and is untouched.
    pins: rdf-1-position-delete-bounds/C-004

## Pointers

- Up: [../map.md](../map.md)
- Related: repark-core registers the catalog providers and installs the write knobs.

## Debug

| Symptom | First check |
|---|---|
| Catalog registration / listing / staleness issues | [src/catalog/map.md](src/catalog/map.md#debug) |
| MERGE / append / overwrite / ALTER issues | [src/write/map.md](src/write/map.md#debug) |
| Fork-pin doubt (crates.io fallback?) | `src/tests/fork_pin.rs` (exercises fork-only `plan_commit_base_load`) + the ported name-only proof in `src/catalog/tests/catalog.rs` — neither compiles against crates.io iceberg 0.9.1 |

First checks: `cargo test -p repark-iceberg` (all AWS-free on `MemoryCatalog`). Escalate to:
[../map.md#debug](../map.md).
