# map — repark-core/src

## Purpose

Source for `repark-core` — `ReparkSession` over a DataFusion `SessionContext` + the
`ExecutionBackend` seam. Catalogs come in two ways: direct builder registration or the Spark
`spark.sql.catalog.<name>.*` config path (`catalog_config` → `register_configured_catalogs`);
`s3://`/`s3a://` reads route through `object_store_s3`. See [../map.md](../map.md).


## Contents

- `session.rs` — `ReparkSession` + `ReparkSessionBuilder` (file-backed tests). Builder collects
  the Spark-style `.config(...)` map (`config(key, value)` / `configs(map)`); sync `build()`
  validates knobs, parses the config's `spark.sql.catalog.<name>.*` /
  `repark.sql.catalog.<name>.*` blocks into `CatalogSpec`s (fail-loud, synchronous), threads every
  `datafusion.*` key from the same map onto the `SessionConfig` via
  `apply_datafusion_config_keys` (P2G R2 — `DATAFUSION_CONFIG_PREFIX`; applied after the typed
  setters + core defaults so an explicit conf wins, before the extension hook; an unknown key is
  an `Error::Config`, never silently inert — this is what makes
  `datafusion.catalog.information_schema = true` real and Q8's `SHOW TABLES` / `DESCRIBE` /
  `information_schema.*` live in BOTH SQL doors), installs a
  default 8 GiB `FairSpillPool` when memory is unset (`memory_limit_bytes(0)` /
  `memory_limit_gb(0)` opt out to Infinite; non-zero budgets below 1 MiB refuse at build;
  `batch_size(0)` / `target_partitions(0)` refuse at build), attaches the write/scan knobs as
  DataFusion `ConfigExtension`s via `repark_iceberg::write::*` (`with_merge_session_knobs`,
  `with_scan_concurrency`, `with_write_concurrency`), and builds `RuntimeEnv` with
  `object_list_cache_limit(0)` so path-overwrite stage-swap never serves a stale listing. Async
  finalize `register_configured_catalogs()` dispatches each parsed `CatalogSpec` to
  `repark_iceberg::catalog`'s `memory`/`glue`/`s3tables` builder; the LATE variant
  `register_late_configured_catalogs(config)` runs the same parse+dispatch against a live
  session for the getOrCreate reuse path (new names register, existing skipped-and-reported).
  Entry points: `sql`, `register_iceberg_catalog` (policy `RequireExplicitLocation`;
  `register_memory_catalog` = the AWS-free local catalog, `TempFallbackAllowed`),
  `create_namespace` (mirrors `location` onto `location_uri` via
  `repark_iceberg::catalog::mirror_namespace_location_keys`), the temp-view family
  (`create_or_replace_temp_view` / `materialize_dataframe_as_temp_view` /
  `materialize_dataframe_as_cache_view` / `create_or_replace_temp_view_from` /
  `drop_temp_view`), `table_exists` (quote-aware segment parse; path-escape segments reject),
  the listing families (`list_iceberg_table_names` live list-on-access / `list_temp_view_names`
  / `list_df_schema_table_names`), `refresh_catalog_provider`, `read_parquet` (an
  `s3://`/`s3a://` path lazily registers that bucket's store once — per-session guard),
  `read_csv` / `read_json` (Spark-style option maps), `read_iceberg_table` + `TimeTravelOpts`
  (snapshot-id / as-of-timestamp / branch / tag, mutual exclusion), and the `testing_` seams
  (`testing_create_ref` / `testing_list_snapshots` / `testing_oob_create_table` /
  `testing_oob_drop_table`). Excel/postgres readers are deferred with their crates.
- `error_map.rs` — `engine_err` (pub — the single `DataFusionError → repark_common::Error`
  classifier): `SQL` → `Parse`, `Plan`/`SchemaError` → `Analysis`, `NotImplemented` →
  `NotImplemented`, `External` downcast to a live `iceberg::Error` → classified by its
  structured `ErrorKind` (`classify_iceberg_error`, the ONE iceberg kind→class mapping — also
  the direct `iceberg_err` fold), wrappers peeled iteratively (bounded,
  `MAX_ERROR_PEEL_DEPTH`), everything else → base `Error::DataFusion`. Postgres/excel folds are
  deferred with their crates. Also `resolve_s3_region_override` (dual-key S3 read-region
  override; identical values collapse, different values fail loud naming both keys).
- `catalog_config.rs` — the `spark.sql.catalog.<name>.*` → `Vec<CatalogSpec { name, kind,
  props }>` parser (`parse_catalog_specs`, pure/AWS-free). Both prefixes share one keyspace
  (cross-spelling duplicates collapse when identical, fail loud otherwise). Rules: bare
  `…catalog.<name>` = the Spark catalog class or a short kind; `<name>.catalog-impl` ending
  `GlueCatalog`→`Glue` / `S3TablesCatalog`→`S3Tables`; `<name>.type` = `glue`/`s3tables`/
  `memory` (`memory` requires `warehouse`); `<name>.io-impl` dropped; every other prop passes
  through verbatim (an S3 Tables `warehouse` ARN is carried into `table_bucket_arn` when the
  latter is absent). Fail-loud `Error::Config` naming the exact key. Registration policy: Glue
  `RequireExplicitLocation`; S3 Tables `ServiceManagedLocation`; memory keeps the temp
  fallback. `CatalogSpec` hand-written `Debug` redacts secret-like prop values.
- `read_options.rs` — CSV/JSON Spark option-map helpers (header/sep/quote/escape/comment/
  nullvalue/multiline/compression; `nullValue` forces all-Utf8 schema).
- `idents.rs` — table-identifier segment parse + path-escape refuse
  (`reject_path_escape_segment` delegates to `repark_iceberg::write::idents::path_escape_kind`
  — shared needles).
- `object_store_s3.rs` — `s3://` / `s3a://` object-store registration for `read_parquet`.
  `AwsConfigCredentialProvider` bridges the aws-config default credential chain (env → shared
  file → IMDS) into `object_store::CredentialProvider`; `build_amazon_s3_store` resolves
  region + credentials into an `AmazonS3` (the ONLY AWS-touching fn); `register_bucket_store`
  puts one store under BOTH `s3://bucket` and `s3a://bucket`; `parse_s3_bucket` /
  `is_s3_scheme` route paths. Tests register an `InMemory` store to prove routing AWS-free.
- `backend.rs` — the `ExecutionBackend` seam (distribution deferred) + `SingleNodeBackend`.
- `dialect.rs` (+ `dialect/tests.rs`) — the SQL dialect seam (design §3): `EngineContext`
  (`#[non_exhaustive]`, mirrors v1 `execute_with_read_only`'s field set; `EngineContext::new`
  is the sanctioned downstream constructor, added phase-2 PR-2) + `SqlDialect` +
  `DataFusionDialect` (the phase-1 default: plain `SessionContext::sql`). UNSTABLE until the
  phase-2 doors land.
- `runtime.rs` (+ `runtime/tests.rs`) — **`EngineRuntime`** (phase-3 PR-3, EC-5 / design §4 Q7):
  the name the engine gives the **embedding's** Tokio runtime — a cloneable `Arc<Runtime>` handle
  with `runtime()` and `block_on`. ADDITIVE and tier-legal: core constructs no runtime, has no
  `Default`, and never blocks on its own behalf; the process-wide INSTANCE is the binding's
  `OnceLock<EngineRuntime>` in `repark-python`. Honors the phase-1 omissions ledger's recorded
  resolution rather than reversing it, and gives a second embedding (a Flight SQL handler is the
  anticipated one) a named type instead of a convention.
- `extension.rs` (+ `extension/tests.rs`) — the registration seam (design §3):
  `SessionExtension` with two defaulted hooks (`configure` pre-assembly, `register`
  post-context) at v1's inline registration positions; `NoopSessionExtension` is the
  no-extension baseline.
- `catalog_state.rs` — the engine-side `CatalogRegistry` (iceberg `Catalog` handles by name) +
  `LocationPolicy` (staged-CTAS location resolution: `RequireExplicitLocation` /
  `ServiceManagedLocation` / `TempFallbackAllowed { root }` — E-4: the temp root resolves once
  at registration, never at query time). Hoisted MOVE-ONLY from the v1 SQL crate.
- `time_travel.rs` (+ `time_travel/tests.rs`) — `TimeTravelSpec` + parsers
  (`parse_version_value`, `parse_timestamp_to_ms`), snapshot resolution, and `read_table_at`
  (snapshot-pinned static provider via `iceberg-datafusion`). Hoisted MOVE-ONLY from the v1 SQL
  crate; the SQL-text rewrite half stays deferred with the phase-2 router.
- `session/` — file-backed test modules of `session.rs`: `aws_gate_tests.rs` (E-2 gate pins
  incl. the late-config region-signal pin, AWS-free) and `tests.rs` (the ported v1 battery, 38 port-now tests in v1 order; names port
  under the declared-rename map — the 18-test deferred subset is in
  `task/port/deferred-tests.md`; plus the phase-2 PR-2 G8 pin
  `bare_session_without_extension_carries_df_54_1_subquery_guard`, NEW — outside the ported
  census).

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| `read_parquet("s3://…")` errors: no region / no credentials | Region from the aws-config chain or the `repark.hadoop.fs.s3a.endpoint.region` / `spark.hadoop.fs.s3a.endpoint.region` config; creds from the default chain. `object_store_s3.rs`. |
| `s3a://` path not found but `s3://` works | Both schemes must be registered per bucket (`register_bucket_store` does both); DataFusion looks up `scheme://bucket` verbatim. |
| `table_exists` on a quoted name misparses | Quote-aware `parse_table_identifier_segments` (double-quote/backtick; dots inside quotes OK); path-escape segments (`..` / `/` / `\`) reject at parse. |
| `SHOW TABLES` / `DESCRIBE` refuses "unless information_schema is enabled" | Set it on the builder: `.config("datafusion.catalog.information_schema", "true")` (P2G R2 — `apply_datafusion_config_keys` in `session.rs`). It is OFF by default; nothing else enables it. |
| A `datafusion.*` builder key fails the build | Intended: an unknown/unparseable key is `Error::Config` naming the key, so a typo cannot go silently inert. Check the spelling against DataFusion's `ConfigOptions`. |
| `$`-suffixed metadata tables show up in `SHOW TABLES` | Current, known behavior (`information_schema_still_exposes_the_dollar_metadata_tables`); whether `repark_iceberg::catalog`'s `SchemaProvider::table_names` should filter them is the open product question in `task/p2g-ansi-m2-ledger.md`. |

First checks: `cargo test -p repark-core`. Escalate to: [../map.md#debug](../map.md).
