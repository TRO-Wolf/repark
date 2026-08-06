# map — repark-core

## Purpose

The Session-centric engine API: construct the DataFusion `SessionContext`, configure the memory
pool, register catalogs, hold the `CatalogRegistry`, and expose the engine entrypoints (`sql`,
readers, temp views, namespace/catalog ops). Execution routes through an `ExecutionBackend`
trait — the seam that lets a future distributed coordinator slot in without reworking the write
path (distribution is deferred). SQL routing and session-build registration are seam-inverted
(`SqlDialect` / `SessionExtension`) so the phase-2 doors plug in without touching this crate.


## Contents

- `Cargo.toml` — depends on `repark-common` (error seed), `repark-iceberg` (catalog builders +
  write knob installers), `datafusion`, `arrow`, `iceberg`, `iceberg-datafusion` (the hoisted
  `read_table_at` static provider), `chrono` (the hoisted `TIMESTAMP AS OF` parser), and the
  S3-read stack (`object_store`, `aws-config`, `aws-credential-types`, `async-trait`, `url`).
- `src/session.rs` — `ReparkSession` + `ReparkSessionBuilder`: knob surface
  (`config`/`configs`, memory limit with the 1 MiB floor / 8 GiB `FairSpillPool` default,
  `batch_size`, `target_partitions`), sync `build()` + async
  `register_configured_catalogs()` finalize (two-phase lifecycle), catalog ops
  (`register_iceberg_catalog`, `register_memory_catalog`, `create_namespace` with the
  location/location_uri mirror, `table_exists`, the listing families,
  `refresh_catalog_provider`), readers (`read_parquet`/`read_csv`/`read_json`,
  `read_iceberg_table` + `TimeTravelOpts`), the temp-view family, and the `testing_` seams.
  Excel/postgres readers are deferred with their crates.
- `src/backend.rs` — `ExecutionBackend` seam + `SingleNodeBackend` (the default; distribution
  deferred).
- `src/catalog_config.rs` — `spark.sql.catalog.<name>.*` / `repark.sql.catalog.<name>.*` →
  `Vec<CatalogSpec>` (glue / s3tables / memory) parser; dual-prefix conflict fail-loud (keys
  only, never values); S3 Tables ARN shape check; `CatalogSpec` hand-written `Debug` redacts
  secret-like prop values. Parsed at `build()`, registered by `register_configured_catalogs`.
- `src/read_options.rs` — CSV/JSON Spark option-map helpers.
- `src/error_map.rs` — DataFusion/iceberg error folds into `repark_common::Error`; public
  `engine_err` (the single `DataFusionError → Error` classifier).
- `src/idents.rs` — table-identifier segment parse + path-escape refuse (delegates to
  `repark_iceberg::write::idents::path_escape_kind` — single-source needles).
- `src/object_store_s3.rs` — `s3://` / `s3a://` `read_parquet` support:
  `AwsConfigCredentialProvider` (aws-config default chain → `object_store::CredentialProvider`),
  `build_amazon_s3_store`, `register_bucket_store` (one store under BOTH scheme URLs),
  `parse_s3_bucket` / `is_s3_scheme`.

## I want to...

| ...do this | go to |
|---|---|
| Add a `ReparkSession` method / config knob | `src/session.rs` |
| Register a catalog / namespace | `register_iceberg_catalog` / `create_namespace` in `src/session.rs` |
| Map a `spark.sql.catalog.*` config block | `src/catalog_config.rs` (`parse_catalog_specs`) |
| Change `s3://` / `s3a://` read routing or the AWS credential bridge | `src/object_store_s3.rs` |
| Tune memory/spill/batch/partition defaults | `src/session.rs` (`FairSpillPool`, `target_partitions`, batch size) |
| Change error classification | `src/error_map.rs` (`engine_err` / `classify_datafusion_error`) |
| Add the distribution backend (later) | implement `ExecutionBackend` in a new crate |
| Plug a statement router / SQL front end | implement `SqlDialect` (`src/dialect.rs`) |
| Install door registrations at build time | implement `SessionExtension` (`src/extension.rs`) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../repark-iceberg/map.md](../repark-iceberg/map.md) (catalog builders + write knobs),
  [../repark-common/map.md](../repark-common/map.md) (error seed).

## Debug

| Symptom | First check |
|---|---|
| OOM on write | Set the `FairSpillPool` budget (`memory_limit_gb`/`_bytes`); `0` opts out to Infinite |
| Three-part name doesn't resolve | Catalog registered under the right name? See repark-iceberg |
| `read_parquet("s3://…")` fails: no region / no credentials | Region from the aws-config chain or `repark.hadoop.fs.s3a.endpoint.region` / `spark.hadoop.fs.s3a.endpoint.region` (dual keys must agree); creds from the default chain. See `src/object_store_s3.rs`. |
| Same-session `read_parquet` after path overwrite returns old rows | Object-list cache must stay at limit 0 in `src/session.rs` `build()`; stage-swap reuses the destination path. |
| Catalog dual-prefix conflict / secret-looking error | Conflict messages name keys only (never raw values); see `catalog_config.rs`. |
| S3 Tables ARN rejected | Warehouse / `table_bucket_arn` must start with `arn:aws:s3tables:`. |
| `s3a://` path not found but `s3://` works | Both schemes must be registered for the bucket (`register_bucket_store` does both); DataFusion looks up `scheme://bucket` verbatim. |

First checks: `cargo test -p repark-core`. Escalate to: [../map.md#debug](../map.md).
