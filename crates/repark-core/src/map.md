# map — repark-core/src

## Purpose

Source for `repark-core` — `ReparkSession` over a DataFusion `SessionContext` + the
`ExecutionBackend` seam (a local execution-context holder and future extension point, *not* a
distribution abstraction — see [../../../ARCHITECTURE.md](../../../ARCHITECTURE.md), "what the
seam is, honestly"). Catalogs come in two ways: direct builder registration or the Spark
`spark.sql.catalog.<name>.*` config path (`catalog_config` → `register_configured_catalogs`);
`s3://`/`s3a://` reads route through `object_store_s3`. See [../map.md](../map.md).


## Contents

- `session.rs` — `ReparkSession` + `ReparkSessionBuilder` (file-backed tests). **G-6:** rustdoc
  intra-links fixed (private helpers named in backticks, not broken `[links]`;
  `Self::list_iceberg_table_names` for the live list path). Builder collects
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
  `repark_iceberg::catalog::mirror_namespace_location_keys`; G-6 Q1: existing
  namespace + contradictory explicit location fails loud naming both paths;
  matching / no-request-location stay idempotent), the temp-view family
  (`create_or_replace_temp_view` / `materialize_dataframe_as_temp_view` /
  `materialize_dataframe_as_cache_view` / `create_or_replace_temp_view_from` /
  `drop_temp_view`), `table_exists` (quote-aware segment parse; path-escape segments reject),
  the listing families (`list_iceberg_table_names` live list-on-access / `list_temp_view_names`
  / `list_df_schema_table_names`), `refresh_catalog_provider`, `read_parquet` (an
  `s3://`/`s3a://` path lazily registers that bucket's store once — per-session guard),
  `read_csv` / `read_json` (Spark-style option maps), `read_iceberg_table` + `TimeTravelOpts`
  (snapshot-id / as-of-timestamp / branch / tag, mutual exclusion), and the `testing_` seams
  (`testing_create_ref` / `testing_list_snapshots` / `testing_oob_create_table` /
  `testing_oob_drop_table`). Excel/postgres readers are deferred with their crates. The file's
  accretion of session policy is deliberate (everything-through-Session); its decomposition into
  named internal services is **deferred and driver-gated** —
  [../../../docs/adr/0005-defer-session-decomposition.md](../../../docs/adr/0005-defer-session-decomposition.md)
  names the triggers, so do not split it opportunistically.
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
- `namespace_create.rs` — **R-6 / G-6 Q1 (2026-08-14):** the shared
  `refuse_contradictory_namespace_location` predicate (and its message helper)
  used by `session.rs` `create_namespace` and both SQL doors' `IF NOT EXISTS`
  paths. Matching location / no-request-location adopt; conflict names both
  paths. Standalone policy (ADR-0005 decision 4), not a Session split.
- `object_store_s3.rs` — `s3://` / `s3a://` object-store registration for `read_parquet`.
  `AwsConfigCredentialProvider` bridges the aws-config default credential chain (env → shared
  file → IMDS) into `object_store::CredentialProvider`; `build_amazon_s3_store` resolves
  region + credentials into an `AmazonS3` (the ONLY AWS-touching fn); `register_bucket_store`
  puts one store under BOTH `s3://bucket` and `s3a://bucket`; `parse_s3_bucket` /
  `is_s3_scheme` route paths. Tests register an `InMemory` store to prove routing AWS-free.
- `backend.rs` — the `ExecutionBackend` seam + `SingleNodeBackend`, its only implementation. One
  method, returning the concrete DataFusion `SessionContext`: the **trait boundary** is the
  load-bearing part, not the surface, which would have to widen (with its call sites) before a
  distributed backend could exist. Distribution is deferred by decision
  ([../../../docs/adr/0004-server-prep-disciplines.md](../../../docs/adr/0004-server-prep-disciplines.md)).
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
  anticipated one) a named type instead of a convention. **Exactly one constructor**
  (`EngineRuntime::new`): the verify panel removed a caller-less, test-less
  `impl From<Arc<Runtime>> for EngineRuntime` — a fidelity phase does not ship untested public
  API (design §8, "do not clean up on the way past"; docs/testing.md "every behavior gets a test").
  Re-add it only with a test and a caller.
- `extension.rs` (+ `extension/tests.rs`) — the registration seam (design §3):
  `SessionExtension` with two defaulted hooks (`configure` pre-assembly, `register`
  post-context) at v1's inline registration positions; `NoopSessionExtension` is the
  no-extension baseline. `configure` takes a `SessionBuildConf` — the builder's raw conf map PLUS
  the values `build()` has already resolved from it (today: the session timezone, H-1a split B).
  A door reads the resolved value instead of re-parsing the map, which is what keeps
  "resolved once, at construction" literally true rather than approximately true.
- `catalog_state.rs` — the engine-side `CatalogRegistry` (iceberg `Catalog` handles by name) +
  `LocationPolicy` (staged-CTAS location resolution: `RequireExplicitLocation` /
  `ServiceManagedLocation` / `TempFallbackAllowed { root }` — E-4: the temp root resolves once
  at registration, never at query time). Hoisted MOVE-ONLY from the v1 SQL crate.
- `time_travel.rs` (+ `time_travel/tests.rs`) — `TimeTravelSpec` + parsers
  (`parse_version_value`, `parse_timestamp_to_ms`), snapshot resolution, `read_table_at`
  (snapshot-pinned static provider via `iceberg-datafusion`), and **`next_temp_view_name` — the
  ONE minter of the `__repark_tt_` ephemeral-view namespace, `pub` for that reason** (H-1b fix
  pass, 2026-08-11). Hoisted MOVE-ONLY from the v1 SQL crate; the SQL-text rewrite half stays
  deferred with the phase-2 router.
  **Documented residual (H-1b, 2026-08-11):** `read_table_at` registers a `__repark_tt_<n>` temp
  view and never deregisters it. For its own caller — the reader-options path in `session.rs`
  (`spark.read.option("snapshot-id" | "as-of-timestamp" | "branch" | "tag", …)`) — that is
  CORRECT and deliberately unchanged: the view backs the `DataFrame` handed to the user, and a
  reader has no statement boundary to release at. Both SQL doors DO have one, and both now track
  their rewrite's names in a `PinnedViews` ledger released after planning (`repark-spark`'s own
  mint; `repark-sql` additionally records the name minted here, since its view composes over this
  function). So a `__repark_tt_*` left on a session is a leak only if the session ran a
  time-travel STATEMENT; after a reader-options read it is the residual — see
  `crates/repark-spark/src/map.md` `## Debug` for the three-producer triage.
  **"Deliberately unchanged" only became TRUE at the fix pass.** `repark-spark`'s rewrite minted
  from a SECOND process-global counter, also starting at 1, so it produced the same names this
  module does: a `VERSION AS OF` statement deregistered a live reader-options view and then
  released it. The registration survived the reader, but not the next statement. One minter
  (above) closes it by construction; pin:
  `repark-spark`'s `tests::time_travel::time_travel_statement_pins_never_collide_with_a_reader_options_view`.
  **`read_table_at`'s returned plan shape is load-bearing**, not incidental: the ANSI door reads
  the name minted here off the frame's `LogicalPlan::TableScan` (`repark_sql::time_travel::
  core_pinned_name`, prefix-checked), so wrapping the frame in another node here, or changing the
  prefix, silently restores that door's half of the leak. The fence is the broadened
  `LIKE '__repark_tt%'` assertion in `crates/repark-sql/tests/introspection.rs`.
- `session_time_zone.rs` (+ `session_time_zone/tests.rs`) — the session timezone
  (`spark.sql.session.timeZone`). Holds the **one** authoritative spelling of that conf key
  (`SESSION_TIME_ZONE_KEY` — no alternate spelling exists, deliberately), the validated
  `SessionTimeZone` value type (IANA id or fixed offset, checked against Arrow's zone database),
  the `UTC` default (a DECLARED divergence from Spark's JVM-local default: reproducible, and no
  host-environment read), and `resolve_session_time_zone`, which `session.rs`'s `build()` calls
  ONCE so an unresolvable zone fails at construction rather than at query time. The value is
  carried on the session (`ReparkSession::session_time_zone`) **and handed to the `configure`
  hook** (H-1a split B, 2026-08-10), which is how the Spark door's extractor layer consumes it:
  this crate never imports `repark-functions` (a forbidden upward edge), so the door is the
  crossing point. Nothing about the key, its spelling or its validation lives anywhere but here.
- `session/` — file-backed test modules of `session.rs`: `aws_gate_tests.rs` (E-2 gate pins
  incl. the late-config region-signal pin, AWS-free), `namespace_create_tests.rs`
  (R-6 / G-6 Q1: create-new / same / conflicting / no-location), and `tests.rs`
  (the ported v1 battery, 38 port-now tests in v1 order; names port
  under the declared-rename map — the 18-test deferred subset is in
  `task/port/deferred-tests.md`; plus the phase-2 PR-2 G8 pin
  `bare_session_without_extension_carries_df_54_1_subquery_guard`, NEW — outside the ported
  census).

## Pointers

- Up: [../map.md](../map.md)

### P3E B-1 (2026-08-08)
- `session.rs` gains `REPARK_OWNED_DATAFUSION_PSEUDO_KEYS` — the exact-key exclusion set for
  facade-owned `datafusion.`-prefixed pseudo-keys the build-time sweep must skip
  (`datafusion.runtime.memory_limit`, the LIVE resize knob). Typos of the pseudo-key still
  fail loud; both directions pinned in `session/tests.rs`.

## Debug

| Symptom | First check |
|---|---|
| `read_parquet("s3://…")` errors: no region / no credentials | Region from the aws-config chain or the `repark.hadoop.fs.s3a.endpoint.region` / `spark.hadoop.fs.s3a.endpoint.region` config; creds from the default chain. `object_store_s3.rs`. |
| `s3a://` path not found but `s3://` works | Both schemes must be registered per bucket (`register_bucket_store` does both); DataFusion looks up `scheme://bucket` verbatim. |
| `table_exists` on a quoted name misparses | Quote-aware `parse_table_identifier_segments` (double-quote/backtick; dots inside quotes OK); path-escape segments (`..` / `/` / `\`) reject at parse. |
| `SHOW TABLES` / `DESCRIBE` refuses "unless information_schema is enabled" | Set it on the builder: `.config("datafusion.catalog.information_schema", "true")` (P2G R2 — `apply_datafusion_config_keys` in `session.rs`). It is OFF by default; nothing else enables it. |
| A `datafusion.*` builder key fails the build | Intended: an unknown/unparseable key is `Error::Config` naming the key, so a typo cannot go silently inert. Check the spelling against DataFusion's `ConfigOptions`. |
| `spark.sql.session.timeZone` seems to have no effect on `year`/`hour`/`date_trunc` | Since H-1a split B it DOES, on a Spark-extended session. On a session built without `SparkExtension` it does not, because stock DataFusion's `date_part` reads the array's own zone — the zone reaches the extractors through `SparkExtension::configure`. Pins: `crates/repark-spark/tests/session_timezone.rs`, `crates/repark-sql/tests/session_timezone_ansi_door.rs`. |
| A session refuses to build naming `spark.sql.session.timeZone` | The zone is validated at construction (`session_time_zone.rs`): it must be an IANA id (`America/New_York`) or a fixed offset (`+05:00`). A differently-cased lookalike key is not this knob — there is exactly one spelling. |
| `$`-suffixed metadata tables do NOT show up in `SHOW TABLES` | Intended since 2026-08-10 — `repark_iceberg::catalog::MetadataProjectionSchemaProvider::table_names` filters the fork's synthesized names ([ADR-0006](../../../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md)); they stay queryable as `ns."t$snapshots"`. Pins: `information_schema_hides_the_dollar_metadata_tables_on_the_bare_session` + `a_hidden_metadata_table_is_still_queryable_on_the_bare_session`. |

First checks: `cargo test -p repark-core`. Escalate to: [../map.md#debug](../map.md).

- **EC-9 scrub (2026-08-08, phase-3 PR-5):** pre-existing private fixture/doc literals
  (a team/bucket name fragment) replaced with `example-team` equivalents — outcome-neutral
  (fixtures and their oracles changed together); enumerated in docs/history/port-v2/p3e-facade-ledger.md.
- **B-2 scrub sites in this crate (2026-08-08):** `catalog_config.rs` (doc header + fixtures) and `object_store_s3.rs` (fixtures) carry the `example-team` replacements enumerated in docs/history/port-v2/p3e-facade-ledger.md.

- **Neutral-fixture scrub (2026-08-10, hardening-prep):** an owner-approved, forward-only,
  comment-and-fixture-only pass moved this directory's doc text and example literals to
  neutral placeholders — the upstream job the acceptance shape mirrors is named generically
  ("the source publish job"), and example table/view/entity names are placeholders carrying no
  domain vocabulary. Outcome-neutral: every renamed fixture moved together with the assertions
  that read it. Sites here: `catalog_config.rs` — the module-doc config-block lead-in, the
  acceptance-matrix table row, and the `glue_catalog_config` doc line.
