# map — repark-iceberg/src/catalog

## Purpose

Iceberg catalog wiring for DataFusion (v1 `repark-catalog`, ported byte-faithful). Build AWS
Glue (primary) and S3 Tables (secondary) catalogs and register any `iceberg::Catalog` as a
DataFusion `CatalogProvider`, so `glue_catalog.namespace.table` resolves with zero translation.

## Contents

- `mod.rs` (v1 `lib.rs`) — `register_iceberg_catalog(ctx, name, catalog)` via
  `build_iceberg_catalog_provider` → [`ReparkCatalogProvider`] (full snapshot at register; free
  SQL needs invalidation after mutations; product DDL invalidates the touched namespace only at
  O(1) via `invalidate_catalog_namespaces`). Live list-on-access helpers `list_table_names` /
  `list_namespace_names` (no DF snapshot; `CATALOG_LISTING_STRATEGY = "list-on-access"`) power
  the Spark Catalog facade. Module decls + the public re-export list (names unchanged from v1).
- `catalog_ops.rs` — `reregister_catalog_provider(ctx, catalog, name)`: the session
  `refresh_catalog_provider` escape hatch's engine-side adapter (full O(databases) rebuild via
  `rebuild_catalog_provider`). Hoisted MOVE-ONLY from v1 `repark-sql/src/catalog_ops.rs`; the
  rest of that v1 file (P11 refusals, namespace resolution, O(1) reregister helpers) ports with
  the SQL layer in phase 2. No direct tests here — v1 coverage is session-level and ports in
  PR-C.
- `builders.rs` — `memory_catalog(warehouse)` (AWS-free in-memory catalog over a local-FS
  warehouse), `glue_catalog(props)` (primary; `warehouse` required), `s3tables_catalog(props)`
  (secondary; `table_bucket_arn` required) — both AWS builders validate the required prop
  fail-loud (naming the key) before construction, then pass every other prop through to Iceberg
  `FileIO`; prop helpers + the `iceberg_to_datafusion` error map. Catalog-edge spans record
  **prop key names only**, never values.
- `location.rs` — namespace-location key identity (`NAMESPACE_LOCATION_PROPERTY` `"location"` /
  `NAMESPACE_LOCATION_URI_PROPERTY` `"location_uri"`; `resolve_namespace_location` read
  precedence + `mirror_namespace_location_keys` unidirectional non-clobbering dual-write) and
  scheme-based FileIO selection (`storage_factory_for_location` / `file_io_for_location`:
  `s3://`/`s3a://` → the fork's OpenDAL S3 factory; `file://`/bare **absolute** path → LocalFs;
  anything else — unknown scheme, single-slash typo `s3:/…`, relative/empty path — fails loud,
  never a silent LocalFs).
- `metadata_projection.rs` — `ProjectingMetadataTableProvider` +
  `MetadataProjectionSchemaProvider` wrap fork `table$meta` providers so `scan` honors DF
  projection via `ProjectionExec` (never collect-then-project). Applied in `provider.rs`
  snapshot/refresh.
- `provider.rs` — `ReparkCatalogProvider` (mutable namespace→schema map) +
  `invalidate_catalog_namespaces` / `drop_catalog_namespace_from_provider` /
  `rebuild_catalog_provider`. Product DDL rebuilds only the touched namespace; empty invalidate
  is a no-op; DROP NAMESPACE is a zero-list map remove; invalidate/drop fail loud when the DF
  catalog name is not registered. Every schema snapshot/refresh wraps with
  `MetadataProjectionSchemaProvider`.
- `tests.rs` — the file-backed unit battery (all AWS-free): CTAS reality, AWS-builder
  validation + offline construction, live-list staleness pins, O(1) invalidation pins,
  scheme-selection + key-identity partitions, span secret-hygiene pins, and the fork-patch
  proof test (`fork_patch_in_effect_deletefilter_is_public` — names a fork-only public symbol,
  cannot compile against crates.io iceberg 0.9.1).

**CTAS reality:** `IcebergSchemaProvider::register_table` is schema-only (rejects a `MemTable`
with data), so CTAS-from-SELECT must be decomposed into `CREATE (cols)` + `INSERT INTO` by the
SQL interception layer (phase-2 door). Locked down by tests here.

## I want to...

| ...do this | go to |
|---|---|
| Register an Iceberg catalog | `register_iceberg_catalog` in `mod.rs` |
| Live list table/namespace names (no DF snapshot) | `list_table_names` / `list_namespace_names` in `mod.rs` |
| Rebuild the DF provider snapshot from the live catalog | `build_iceberg_catalog_provider` / `rebuild_catalog_provider` |
| Invalidate one namespace after product DDL (O(1)) | `invalidate_catalog_namespaces` / `drop_catalog_namespace_from_provider` in `provider.rs` |
| AWS-free catalog for local dev / tests | `memory_catalog(warehouse)` in `builders.rs` |
| Pick a FileIO backend by location scheme | `file_io_for_location` / `storage_factory_for_location` in `location.rs` |
| Read / write a namespace's warehouse location | `resolve_namespace_location` / `mirror_namespace_location_keys` in `location.rs` |
| Change credential handling | not here — AWS SDK default chain *inside the fork* |

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| CTAS errors "does not support tables with data" | expected — decompose into `CREATE (cols)` + `INSERT INTO` (SQL layer) |
| Table created after register not found in SQL | provider name directory is snapshotted; product DDL invalidates the touched namespace; OOB DDL needs refresh / full rebuild. Facade listing is live (`list_table_names`) |
| Builder returns "requires a non-empty `…` property" | Glue needs `warehouse`, S3 Tables needs `table_bucket_arn` |
| Constructing Glue/S3 Tables in a test without AWS | pin `region_name` so SDK-config load skips the IMDS region probe; creds resolve lazily on first request |
| S3 Tables `301`/region errors | pass explicit `region_name`; ARN region must match SDK region |
| Hang on Glue/S3 catalog with no logs | enable `RUST_LOG=repark_iceberg=info`; expect `catalog.*` span close timings. Span fields are key names only |

First checks: `cargo test -p repark-iceberg catalog::`. Escalate to: [../../map.md#debug](../../map.md).

- **EC-9 scrub (2026-08-08, phase-3 PR-5):** pre-existing private fixture/doc literals
  (a team/bucket name fragment) replaced with `example-team` equivalents — outcome-neutral
  (fixtures and their oracles changed together); enumerated in docs/history/port-v2/p3e-facade-ledger.md.
