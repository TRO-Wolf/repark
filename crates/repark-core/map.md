# map — repark-core

## Purpose

The Session-centric engine API (crate-DAG **tier 2**, the engine session both doors and the
bindings plug into): construct the DataFusion `SessionContext`, configure the memory
pool, register catalogs, hold the `CatalogRegistry`, and expose the engine entrypoints (`sql`,
readers, temp views, namespace/catalog ops). Execution routes through an `ExecutionBackend`
trait — today a local execution-context holder over in-process DataFusion, whose *boundary* (not
its minimal surface) is what would let a future distributed coordinator be introduced without
reworking the write path; distribution is deferred by decision, and the seam would have to widen
first ([../../ARCHITECTURE.md](../../ARCHITECTURE.md) "`ExecutionBackend` — what the seam is,
honestly"). SQL routing and session-build registration are seam-inverted
(`SqlDialect` / `SessionExtension`) so the phase-2 doors plug in without touching this crate.


## Contents

- `Cargo.toml` — depends on `repark-common` (error seed), `repark-iceberg` (catalog builders +
  write knob installers), `datafusion`, `arrow` (with the **`chrono-tz` feature declared here**,
  not inherited: `src/session_time_zone.rs` validates IANA zone ids through `arrow`'s `Tz`, which
  without that feature accepts only fixed offsets — it reaches this crate today only via
  `datafusion-functions`, so owning the enable keeps a DataFusion feature change from turning
  `America/New_York` into a build refusal; `Cargo.lock` is unchanged by the declaration),
  `iceberg`, `iceberg-datafusion` (the hoisted
  `read_table_at` static provider), `chrono` (the hoisted `TIMESTAMP AS OF` parser), and the
  S3-read stack (`object_store`, `aws-config`, `aws-credential-types`, `async-trait`, `url`), plus
  `tokio` — added phase-3 PR-3 solely to NAME `EngineRuntime` (EC-5); core still constructs no
  runtime and never blocks. No new package resolves: DataFusion already pulls tokio into the lock.
- `src/session.rs` — `ReparkSession` + `ReparkSessionBuilder`: knob surface
  (`config`/`configs`, memory limit with the 1 MiB floor / 8 GiB `FairSpillPool` default,
  `batch_size`, `target_partitions`), sync `build()` + async
  `register_configured_catalogs()` finalize (two-phase lifecycle), catalog ops
  (`register_iceberg_catalog`, `register_memory_catalog`, `create_namespace` with the
  location/location_uri mirror, `table_exists`, the listing families,
  `refresh_catalog_provider`), readers (`read_parquet`/`read_csv`/`read_json`,
  `read_iceberg_table` + `TimeTravelOpts`), the temp-view family, and the `testing_` seams.
  Excel/postgres readers are deferred with their crates.
- `src/backend.rs` — the `ExecutionBackend` seam (one method returning the concrete DataFusion
  `SessionContext`; a local execution-context holder + deliberately-minimal extension point) +
  `SingleNodeBackend`, the only implementation. Distribution is deferred by decision
  ([../../docs/adr/0004-server-prep-disciplines.md](../../docs/adr/0004-server-prep-disciplines.md)).
- `src/runtime.rs` (+ `src/runtime/`) — `EngineRuntime`, the embedding's executor handle
  (phase-3 PR-3, EC-5 / design §4 Q7): additive, tier-legal, constructed only from an
  `Arc<Runtime>` the embedder owns. The process-wide instance lives in `repark-python`.
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
- `src/lib.rs` — the crate-root manifest (module declarations + re-exports; no logic).
- `src/dialect.rs` / `src/extension.rs` — the phase-2 seams: `SqlDialect` (+ `EngineContext`,
  default `DataFusionDialect`) and `SessionExtension` (configure/register hooks,
  `NoopSessionExtension`).
- `src/catalog_state.rs` — the hoisted `CatalogRegistry` + `LocationPolicy` (E-4 temp-root
  resolution at registration).
- `src/time_travel.rs` — the hoisted `TimeTravelSpec` parsers + `read_table_at`
  (snapshot-pinned static provider).
- `src/map.md` — the per-file source inventory (authoritative detail for everything above, plus
  the file-backed test module dirs).

## I want to...

| ...do this | go to |
|---|---|
| Add a `ReparkSession` method / config knob | `src/session.rs` |
| Register a catalog / namespace | `register_iceberg_catalog` / `create_namespace` in `src/session.rs` |
| Map a `spark.sql.catalog.*` config block | `src/catalog_config.rs` (`parse_catalog_specs`) |
| Change `s3://` / `s3a://` read routing or the AWS credential bridge | `src/object_store_s3.rs` |
| Tune memory/spill/batch/partition defaults | `src/session.rs` (`FairSpillPool`, `target_partitions`, batch size) |
| Change error classification | `src/error_map.rs` (`engine_err` / `classify_datafusion_error`) |
| Add the distribution backend (later) | [../../ARCHITECTURE.md](../../ARCHITECTURE.md) first — the seam's surface (and its call sites) widens before a new-crate `impl` is the work |
| Plug a statement router / SQL front end | implement `SqlDialect` (`src/dialect.rs`) |
| Install door registrations at build time | implement `SessionExtension` (`src/extension.rs`) |

## Component contract

- **Owns:** `ReparkSession` + builder (the engine API); the `ExecutionBackend` / `SqlDialect` /
  `SessionExtension` seams; catalog & namespace ops; readers (parquet / csv / json / iceberg + time
  travel); temp views; `*.sql.catalog.*` config parsing; `s3://` / `s3a://` read routing + the AWS
  credential bridge; the error fold (`engine_err`).
- **Does not own:** SQL grammar / routing (the doors, via `SqlDialect`); the write engine + catalog
  internals (repark-iceberg + the fork); Spark functions (repark-functions); the Python surface.
- **Public inputs:** builder knobs (config map, memory / batch / partition); SQL text via `sql()`; a
  door's `SqlDialect` / `SessionExtension`; catalogs registered at runtime.
- **Public outputs:** a `ReparkSession`; DataFusion `DataFrame`s; an `EngineContext` snapshot for
  dialects; registered catalog providers.
- **State & lifecycle:** two-phase — sync `build()` (no I/O) then async
  `register_configured_catalogs()`. Config / runtime / dialect / extension are immutable after build;
  the `CatalogRegistry` stays mutable (RwLock, snapshotted per query — no lock held across `.await`).
- **Allowed internal deps:** `repark-common`, `repark-iceberg` (+ datafusion / iceberg / arrow / AWS /
  tokio). No edge up to any door.
- **Failure model:** folds `DataFusionError` / `iceberg::Error` into `repark_common::Error` at the
  session boundary (`engine_err`); config errors fail loud at build.
- **Extension points:** `SqlDialect` (a door front end) and `SessionExtension` (build-time
  registrations) are ready seams — a door plugs in without touching this crate. `ExecutionBackend`
  is a *future* extension point, not a ready one: its one-method surface hands back a concrete
  DataFusion `SessionContext`, so distribution means widening the trait and moving its call sites,
  not adding a second `impl`.
- **Test strategy:** `cargo test -p repark-core` — AWS-free; catalog / session / reader unit +
  file-backed modules.
- **Known limitations:** `SingleNodeBackend` is the only backend and the `ExecutionBackend` surface
  is deliberately minimal (honest framing:
  [../../ARCHITECTURE.md](../../ARCHITECTURE.md); current state:
  [../../STATUS.md](../../STATUS.md) "Architectural risks"); `ReparkSession` is a growing policy
  object whose internal decomposition is deferred and driver-gated
  ([../../docs/adr/0005-defer-session-decomposition.md](../../docs/adr/0005-defer-session-decomposition.md)).

## Pointers

- Up: [../map.md](../map.md)
- Down: [src/map.md](src/map.md) (per-file source inventory).
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
| Every IANA session zone is suddenly refused, fixed offsets still work | The `chrono-tz` feature on this crate's `arrow` dependency was dropped (`Cargo.toml`): without it `arrow::array::timezone::Tz` parses offsets only. Re-declare it here — never rely on `datafusion`'s feature graph. |
| `s3a://` path not found but `s3://` works | Both schemes must be registered for the bucket (`register_bucket_store` does both); DataFusion looks up `scheme://bucket` verbatim. |

First checks: `cargo test -p repark-core`. Escalate to: [../map.md#debug](../map.md).
