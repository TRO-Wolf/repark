# map — repark-core/src

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002). Clippy doc_markdown backticks added.

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

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
  `information_schema.*` live in BOTH SQL doors), carries the **two DF-54.1 regression guards**
  (`session/df_guards.rs`) at two different altitudes — a config default,
  `optimizer.enable_physical_uncorrelated_scalar_subquery = false` (the 54.1 physical
  scalar-subquery path drops a top-level Sort), and, since DEFECT-2 2026-08-18, a **scoped
  optimizer rule**: DataFusion's rule list with `push_down_leaf_projections` wrapped so it
  declines on the `Unnest`-carrying plans it miscompiles (the shape every repeated `explode` /
  `dynamicFlatten` builds) and runs untouched everywhere else — installs a
  RAM-relative `FairSpillPool` when memory is unset (`clamp(0.6 × cgroup-or-MemTotal,
  1 MiB, 8 GiB)`; `memory_limit_bytes(0)` / `memory_limit_gb(0)` opt out to Infinite;
  non-zero budgets below 1 MiB refuse at build; runtime `SET datafusion.runtime.memory_limit`
  swaps a new FairSpillPool — see `session/spill.rs`;
  `batch_size(0)` / `target_partitions(0)` refuse at build; unset `batch_size` defaults to
  `DEFAULT_BATCH_SIZE` 65536, not DataFusion's 8192 — 2026-08 perf baseline, typed setter >
  conf key > default), attaches the write/scan knobs as
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
  matching / no-request-location stay idempotent), the temp-view family — which since round 6
  lives in `session/temp_views.rs`, not in this file
  (`create_or_replace_temp_view` / `materialize_dataframe_as_temp_view` /
  `materialize_dataframe_as_cache_view` — both collect remints share
  `register_collected_memtable` which re-stamps tighten provenance (R-A) —
  / `create_or_replace_temp_view_from` / `declare_temp_view_sorted` /
  `drop_temp_view`, all resolving names through `temp_view.rs`),
  `table_exists` (quote-aware segment parse; path-escape segments reject; the ONE-part arm asks
  the pinned temp-view home, not the live default catalog — R6-1),
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
- `dynamic_flatten.rs` (+ `dynamic_flatten/`) — **DF1 native `dynamic_flatten`:** free
  function over a DataFusion `DataFrame` (no frame newtype). Structs first (null-safe
  `get_field` Project, never DF struct `unnest_columns`), then lists one-at-a-time in
  schema order via preserve-null `unnest_columns_with_options` + `Column::new_unqualified`;
  empty lists are rewritten only when `empty_as_null=true` and the list type can be empty.
  `List` / `LargeList` / `FixedSizeList` explode; Dictionary unwraps one level so
  Parquet dict-structs / dict-lists are not skipped (dict-lists are cast to List
  before Unnest); maps are not unnested and list-of-map refuses LOUD; ListView /
  LargeListView refuse LOUD (`[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]`). Errors are
  `Error::Analysis` with `[DYNAMIC_FLATTEN_NAME_COLLISION]` /
  `[DYNAMIC_FLATTEN_MAX_DEPTH]` / `[DYNAMIC_FLATTEN_EMPTY_STRUCT]` /
  `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]`. `max_depth` bounds rewrite passes, not
  row cartesian. File-backed pins: `dynamic_flatten/tests.rs`,
  `dynamic_flatten/tests/octo.rs`, and `dynamic_flatten/tests/preserve_nulls.rs`. Kernel harness uses
  `ReparkSession` (Unnest-safe leaf-pushdown wrapper), not a blanket
  `enable_leaf_expression_pushdown=false`.
  **PERF-DYNFLATTEN-2 (2026-09-04):** "null-safe `get_field` Project" above now means the
  extractor in [dynamic_flatten/null_mask.rs](dynamic_flatten/map.md) for a plain `Struct`
  parent — one scalar UDF that unions the parent's validity into the child array — and the
  CASE only for a `Dictionary(_, Struct)` parent. Same row set, same Arrow types; the
  null-parent cost stops being proportional to rows.
  pins: perf-dynflatten-2-null-mask/C-002
  **PERF-DYNFLATTEN-1:** the rewrite is generic over a `StatsSink`. The product entry
  `dynamic_flatten` instantiates the ZST `NoStats`, so every counter call and the
  `count_plan_kinds` plan walk compile away — the measurement adds NO work to the product path,
  pinned by `product_dynamic_flatten_does_no_plan_walk`. The stats type, its sink impl,
  `dynamic_flatten_with_stats` and `count_plan_kinds` are all `#[cfg(test)]`.
  `dynamic_flatten_with_stats` returns `DynamicFlattenStats`
  (`rewrite_passes`, `schema_walks`, `fields_visited`, `struct_expansions`,
  `list_explodes`, `plan_nodes`, `unnest_nodes`, `projection_nodes`); product
  `dynamic_flatten` is the same rewrite. Depth-3 pin: 4 passes, 10 walks, 3
  expansions, 20 fields visited.
  pins: perf-dynflatten-1-measure/C-002
- `lib.rs` — **PERF-DYNFLATTEN-1:** `built_with_debug_assertions()` returns
  `cfg!(debug_assertions)`. The measurement runner refuses to write a report unless it is
  false, so an H-3 number can never come from a debug build.
  pins: perf-dynflatten-1-measure/C-002
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
- `read_options.rs` — CSV/JSON Spark option-map helpers and the local-CSV all-Utf8 inference
  workaround for `nullValue`.
- `spark_nullable.rs` — **CUTOVER-SCHEMA-1 (2026-09-04):** Spark-style nullability
  derivation. `relax_schema_to_nullable` marks every field nullable, recursive over
  struct/list/map (map keys stay required — Arrow forbids nullable map keys), depth-bound
  32 past which flags still flip but children keep file nullability; both CTAS doors
  derive their Iceberg schema through it, so derived columns store optional the way
  Spark stores them. `read_parquet_nullable` infers the file schema, relaxes it, and
  re-reads with the relaxed schema as the DataFusion schema override — the plan keeps
  a single TableScan, so EXPLAIN output is unchanged. The double infer costs one extra
  listing plus footer reads; S3 registration still happens first in `session.rs`.
  pins: cutover-schema-1/C-001, C-002
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
- `pre_execute.rs` (+ `pre_execute/tests.rs`) — **the shared pre-execute belt (round 5, Z-2):**
  `PreExecute` = plan (`create_logical_plan`, no execution) → `guard` (the ONE choke point for
  pre-execute refusals; today the tighten DDL-sink refuse) → `execute`. The native door
  (`DataFusionDialect`) runs the whole belt; `repark_sql::router::delegate`,
  `repark_sql::create_table` (CTAS derivation) and `repark_spark::spark_ast::execute_passthrough`
  call `guard` on their own planned statement. Door-specific guards (SEC-02 local-fs, the Spark
  AST rewrites) deliberately stay at the doors. New pre-execute refusals land in `guard`, never
  at a door — per-door wiring missed the native door twice (measured).
- `dialect.rs` (+ `dialect/tests.rs`) — the SQL dialect seam (design §3): `EngineContext`
  (`#[non_exhaustive]`, mirrors v1 `execute_with_read_only`'s field set; `EngineContext::new`
  is the sanctioned downstream constructor, added phase-2 PR-2) + `SqlDialect` +
  `DataFusionDialect` (the phase-1 default: DataFusion semantics — round 5 Z-2 routes its
  `execute` through `PreExecute::run` instead of a bare `SessionContext::sql`, so the native
  door is guarded like the two SQL doors). `SqlDialect::on_session_built` (default no-op)
  runs from `ReparkSessionBuilder::build` after extension `register` (F-Y10-1).
  `#[async_trait(?Send)]`
  — rustc 1.96 HRTB + iceberg `Catalog` in `CatalogRegistry`; session awaits in place.
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
  `ServiceManagedLocation` / `TempFallbackAllowed { root }` — E-4: the root resolves once
  at registration, never at query time). **A13:** `register_memory_catalog` sets `root` to the
  warehouse (`memory_warehouse_fallback_root`, also used to normalize CALL `location`
  strings); `CatalogRegistry::from` still uses `std::env::temp_dir()`. Hoisted MOVE-ONLY
  from the v1 SQL crate. **PERF-ICE-CATALOG-IO-1:** the registry also carries this session's
  `CatalogCaches` (`with_cache_settings`, resolved once in `build()` from the conf map), so every
  catalog the session builds shares one metadata-location cache and one retained-entry bound.
  pins: perf-ice-catalog-io-1/C-002, C-004
- `lineage_columns.rs` — **V3-4:** `prepare_lineage_sql` rewrites **single-table** queries
  that name `_row_id` / `_last_updated_sequence_number` onto a v3
  `LineageColumnsTableProvider` temp view (qualified/aliased FROM, unquoted case-fold,
  schema-order `*` expand). JOIN / CTE / subquery / time-travel naming a lineage column
  refuse `[V3-ROWID-2]`. v1/v2 stay unresolved (`No field named _row_id`). Both SQL doors
  call it.
  pins: v3-4-serve-lineage-columns/C-002, C-003, C-011, C-012, C-013, C-014, C-015, C-016
- `time_travel.rs` (+ `time_travel/tests.rs`) — `TimeTravelSpec` + parsers
  (`parse_version_value`, `parse_timestamp_to_ms`), snapshot resolution, `read_table_at`
  (snapshot-pinned static provider via `iceberg-datafusion`), and **`next_temp_view_name` — the
  ONE minter of the `__repark_tt_` namespace** (H-1b fix pass, 2026-08-11). SQL-text rewriting
  remains deferred with the phase-2 router.
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
- `sorted_view.rs` — SE-1 declared-sorted temp views: `verify_batches_sorted` (the O(n)
  adjacent-pair lexicographic check, ASC NULLS LAST, cross-batch) + `declared_sort_order`
  (`Column::from_name`, never ident-parsing `col()` — the U-DF-1 lowercase-fold class)
  + **PR-D1 `tightenNulls`:** `apply_declare_nullability` (restore then optional tighten;
  tag flipped fields with `repark.tighten_nulls=1`; rebuilds via
  `Schema::new_with_metadata` so top-level schema metadata survives). Public
  `refuse_iceberg_create_of_tightened_plan` (walk `TableScan` source schemas
  **with subqueries** — SQM F1 / R-B — and follow `TableSource::get_logical_plan`
  so a lazy `into_view` hop cannot hide the `MemTable`; iterative, visit-budgeted
  with a generic overflow error — not a `tightenNulls` CREATE refusal)
  plus `refuse_iceberg_create_of_tightened_schema` (output-tag belt). R-D: refuse
  only when a tightened source would persist a non-nullable output. Cache/persist/
  checkpoint remint re-stamps schema-level provenance (R-A). Both SQL doors call
  the refuse at CTAS derivation.
  **Round 4 (Y-3/Y-4):** `refuse_iceberg_create_of_tightened_ddl` closes the DDL-SINK door
  — `CREATE VIEW cat.ns.v AS …` and `SELECT … INTO cat.ns.t` never reach CTAS derivation
  (both routers drop them into their catch-all) yet the Iceberg schema provider's
  `register_table` persists a real table. Same R-D predicate, applied to the planned
  `DdlStatement::CreateView` / `CreateMemoryTable` body.
  **Round 5 (Z-1):** the catalog gate is the RESOLVED name, not the spelling — the target is
  resolved through `TableReference::resolve` against `datafusion.catalog.default_catalog` /
  `default_schema`, because `SET datafusion.catalog.default_catalog = <iceberg>` makes a
  one- or two-part `CREATE VIEW` / `SELECT … INTO` persist into the Iceberg catalog exactly
  like the three-part spelling (measured on all three doors). The function therefore takes the
  `SessionContext`. **Round 5 (Z-2):** it is no longer called from the doors directly — every
  door reaches it through `pre_execute.rs`'s `PreExecute::guard`.
  **Round 4 (Y-2):** the `get_logical_plan` follow is unreachable from any SQL-door
  statement on DataFusion 54.1 (`LogicalPlanBuilder::scan` inlines a source that has a
  logical plan) — measured, all four lazy-view pins stayed green with the follow deleted.
  It is live for a scan the builder does NOT inline (non-empty `filters`), which is what
  `filtered_scan_of_a_view_source_exercises_the_get_logical_plan_recurse` pins.

  The public door is `session.rs::declare_temp_view_sorted(..., tighten_nulls)`: verify
  FIRST, then apply nullability, then re-register the `MemTable` `with_sort_order`.
  Trust model is declare + ALWAYS-verify, refuse loud — no unverified fast path, by design
  (a wrong claim would silently corrupt every window result). A NULL key under tighten
  refuses naming the key and `tightenNulls`. Plan pins + refusal battery:
  `../tests/declared_sorted.rs`.
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
  **TZ-8 (2026-08-14):** module docs now say `CAST(ts AS DATE)` / `to_date` honor the zone
  (NTZ stays the stored wall); `datediff` rides CAST; `last_day`/`date_add` over TIMESTAMP
  stay residual. Ledger: `task/r4-tz8-ledger.md`.
- `temp_view.rs` (+ `temp_view/tests.rs`) — **the temp-view NAME choke point (round 6, R6-1):**
  `TempViewHome` (the build-time `catalog.schema` a session's temp views live in, snapshotted
  once) + `temp_view_ref`, which every temp-view entry point resolves names through. A QUALIFIED
  name refuses loud (`Error::Analysis` → facade `AnalysisException`, mirroring PySpark's class):
  `createOrReplaceTempView("ice.sales.v")` used to forward the raw name to `register_table`,
  which resolved into the Iceberg catalog provider and PERSISTED a real table — a `tightenNulls`
  `required: true` payload included, the very thing `pre_execute.rs` refuses on the SQL doors
  (MEASURED, round-6 ledger). A one-part name is pinned `Full` against the home, so
  `SET datafusion.catalog.default_catalog = <iceberg>` cannot move where a temp view registers.
  The home is a NAME **and** the schema PROVIDER that sat under it at build (`assert_home_intact`,
  `Arc::ptr_eq` at every entry point): `default_catalog` is also a BUILD-time key, so a session
  built with `default_catalog = ice` had its home NAME taken over by the Iceberg catalog and
  persisted the payload anyway (MEASURED — round-6 critic S1). With a catalog over the home the
  whole family refuses loud rather than write or answer for it.
  Parsing is DataFusion's own `TableReference::parse_str` — identifier normalization is
  unchanged from BASE.
- `session/` — `temp_views.rs` (the temp-view family: register / replace / materialize / cache /
  declare-sorted / drop, all through `temp_view_ref`; split out of `session.rs` in round 6) and
  `spill.rs` (S-1: FairSpillPool install + runtime SET intercept; production
  siblings of `session.rs`) plus file-backed test modules of `session.rs`: `tests/aws_gate.rs`
  (E-2 gate pins incl. the late-config region-signal pin, AWS-free),
  `tests/namespace_create.rs` (R-6 / G-6 Q1: create-new / same / conflicting / no-location),
  `tests/session/catalog_registration.rs` (same-name linearization, duplicate rejection before provider
  build, distinct-name build overlap, and provider-build failure atomicity),
  and `tests/session.rs` (the ported v1 battery, 38 port-now tests in v1 order; names port
  under the declared-rename map — the 18-test deferred subset is in
  `task/port/deferred-tests.md`; plus the phase-2 PR-2 G8 pin
  `bare_session_without_extension_carries_df_54_1_subquery_guard`, NEW — outside the ported
  census).

## Pointers

- Up: [../map.md](../map.md)
- `dynamic_flatten/`: [dynamic_flatten/map.md](dynamic_flatten/map.md)

### P3E B-1 (2026-08-08)
- `session.rs` re-exports `REPARK_OWNED_DATAFUSION_PSEUDO_KEYS` from `session/spill.rs` —
  the exact-key exclusion set for facade-owned `datafusion.`-prefixed pseudo-keys the
  build-time sweep must skip (`datafusion.runtime.memory_limit`, applied to a FairSpillPool
  at build and on runtime SET; `datafusion.runtime.temp_directory` is build-time only,
  runtime SET refuses and names `TMPDIR`). Typos of a pseudo-key still fail loud; both
  directions pinned in `session/tests/session.rs` / `session/spill.rs`.

## Debug

| Symptom | First check |
|---|---|
| `read_parquet("s3://…")` errors: no region / no credentials | Region from the aws-config chain or the `repark.hadoop.fs.s3a.endpoint.region` / `spark.hadoop.fs.s3a.endpoint.region` config; creds from the default chain. `object_store_s3.rs`. |
| `s3a://` path not found but `s3://` works | Both schemes must be registered per bucket (`register_bucket_store` does both); DataFusion looks up `scheme://bucket` verbatim. |
| `table_exists` on a quoted name misparses | Quote-aware `parse_table_identifier_segments` (double-quote/backtick; dots inside quotes OK); path-escape segments (`..` / `/` / `\`) reject at parse. |
| `SHOW TABLES` / `DESCRIBE` refuses "unless information_schema is enabled" | Set it on the builder: `.config("datafusion.catalog.information_schema", "true")` (P2G R2 — `apply_datafusion_config_keys` in `session.rs`). It is OFF by default; nothing else enables it. |
| A nested-column query got slower after 2026-08-18 | Only if the plan carries an `Unnest` (a repeated `explode` / multi-pass `dynamicFlatten`) AND `push_down_leaf_projections` fails on it: DF-54.1 guard 2 then keeps the unoptimized plan rather than the miscompiled one, so struct-field extraction is not hoisted toward the leaves for that subtree (`session/df_guards.rs`). Plans with no `Unnest` are untouched — MEASURED identical to stock DataFusion, plan and timing. There is deliberately no knob that restores the miscompile; `.config("datafusion.optimizer.enable_leaf_expression_pushdown", "false")` still turns the whole optimization off. Pins: `bare_session_keeps_leaf_expression_pushdown_enabled`, `a_plan_without_unnest_keeps_the_stock_leaf_pushdown`, `an_unnest_plan_the_rule_can_rewrite_still_gets_leaf_pushdown`. |
| `dynamicFlatten` / `dynamic_flatten` wrong names, collisions, or null-parent zeros | Kernel is `dynamic_flatten.rs` — structs are a null-safe Project (the `null_mask.rs` extractor, or the CASE on a dictionary struct; never DF struct unnest); lists bind through `Column::new_unqualified`. Pins: `dynamic_flatten/tests.rs`. |
| A `datafusion.*` builder key fails the build | Intended: an unknown/unparseable key is `Error::Config` naming the key, so a typo cannot go silently inert. Check the spelling against DataFusion's `ConfigOptions`. |
| `spark.sql.session.timeZone` seems to have no effect on `year`/`hour`/`date_trunc` | Since H-1a split B it DOES, on a Spark-extended session. On a session built without `SparkExtension` it does not, because stock DataFusion's `date_part` reads the array's own zone — the zone reaches the extractors through `SparkExtension::configure`. Pins: `crates/repark-spark/tests/session_timezone.rs`, `crates/repark-sql/tests/session_timezone_ansi_door.rs`. |
| A session refuses to build naming `spark.sql.session.timeZone` | The zone is validated at construction (`session_time_zone.rs`): it must be an IANA id (`America/New_York`) or a fixed offset (`+05:00`). A differently-cased lookalike key is not this knob — there is exactly one spelling. |
| `$`-suffixed metadata tables do NOT show up in `SHOW TABLES` | Fork F-8 (RP-5): `table_names` lists catalog entries only. Pins: `information_schema_hides_the_dollar_metadata_tables_on_the_bare_session` + `a_hidden_metadata_table_is_still_queryable_on_the_bare_session`. pins: rp-5-fork-repin/C-003 |
| `SELECT * … JOIN … _row_id` returns shuffled user columns | Intended refuse `[V3-ROWID-2]` — the rewrite is single-table only (`lineage_columns.rs`). A successful HashMap-ordered projection is the L-001 defect. |

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
**SQM round 7 (R7-1).** `session/temp_views.rs` gained the READ half of the temp-view seam:
`temp_view_home()` (the `[catalog, schema]` a product read path prefixes a session-local view
with) and `resolve_temp_view_home_ref(name)` (the `[catalog, schema, table]` a one-part name
resolves to, or `None`). Both re-check `assert_home_intact`, so the read side cannot become a way
around the R6-1 home check. `temp_view.rs` additionally accepts the session's OWN home spelling
(`<home.catalog>.<home.schema>.<view>`) as that same session-local view — any other qualified
name still refuses. Raw SQL bodies on `Session::sql` are unchanged (still DataFusion's
live-default resolution; pinned by `set_to_a_plain_catalog_keeps_the_write_home_and_moves_only_the_read`).
