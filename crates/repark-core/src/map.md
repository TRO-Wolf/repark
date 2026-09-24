# map — repark-core/src

U1-MEM-LAYOUT-1 (2026-09-23): session memory-catalog registration records the warehouse layout root; see the test pins in `session/tests/map.md`. pins: u1-mem-layout-1/C-001

ICE-MIXED-CASE-1 round 3 (2026-09-17, Q-20b-1): `rewrite_fragment_case` takes `unqualified_scope` — bare references resolve against one MERGE side only while qualified references keep validating against both. pins: ice-mixed-case-1/C-004
ICE-MIXED-CASE-1 round 4 (2026-09-17): red-first `dataframe_filter_binds_projection_alias` reproduces the L-01 DataFrame filter regression at the Rust level. pins: ice-mixed-case-1/C-009

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

- `silver.rs` + [silver/](silver/map.md) — typed `SilverPlan` (SILVER-S1, 2026-09-12): TOML
  parse with key-path refusals, closed operation enums, parse-time structural validation,
  canonical identity bytes, deterministic `explain()`. `pub` from this crate, not bound
  into Python, unstable until SIL-1..SIL-10. No data execution.
  pins: silver-s1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- `config_file.rs` + [config_file/](config_file/map.md) — the `repark.toml` loader (CFG-1,
  steps 1–2 landed 2026-09-09). The module owns `ConfigFile` (`profiles: BTreeMap<String,
  Profile>` over the D-1 tables), the public `load()` entry (discovery, then the file read,
  then `parse()`), and the `parse()` reader over `toml::Table`; unknown keys refuse as
  `Error::Config` with the key path. Discovery, the profile merge and `${VAR}`
  interpolation live in `config_file/{discovery,profile,interpolate}.rs`; step 2 landed
  `sources.rs` (`[<profile>.catalog.<name>]` re-bridged through `parse_catalog_specs` for
  byte-identical specs, `[<profile>.database.<kind>.<name>]` into the crate-private
  `SourceSpec`, per-profile cross-family name uniqueness) and `redact.rs`
  (`redact_value`/`redact_config` over `catalog_config::prop_key_is_secret`, widened to
  `pub(crate)` this step). Step 3 wired the builder (`wiring.rs`: forced-or-discovered load,
  `REPARK_ENV` profile, merge, interpolation, translation to flat pairs, the `(key, redacted
  value, source)` dump rows) and took the now-live `#[allow(dead_code)]` attributes off;
  the ones still unreachable (`load`, `redact_config`, the spec-field carriers) stay, and
  the crate is built with warnings denied. The facade round added the `config_file_pairs`
  entry (translated pairs for the binding fold) and nested-`conf` dot-join flattening.
  MAINT-POLICY-1 step 1 (2026-09-10) added `config_file/maintenance.rs` (the typed
  `[<profile>.maintenance]` policy: `MaintenancePolicy` + `TablePolicy`, the D-2
  duration parser, key-by-key table resolution, `adaptive_partitioning` reserved) and
  the raw `maintenance` slot on `Profile`.
  pins: cfg-1/C-001, C-002, C-003, C-004, C-005, C-006, C-010, C-011, C-012, C-013, C-014,
  C-015, C-016, C-017, C-018, C-019, C-020, C-021, C-022, C-023, C-025, C-026, C-027
  pins: maint-policy-1/C-001, C-002, C-003, C-004, C-005, C-006
  **REVIEW-FIX-7 step 1 (2026-09-10):** `parse()` sanitizes TOML failures to
  `message()` plus the locally computed line and column, never the echoed source line.
  pins: review-fix-7/C-002
- `session.rs` — `ReparkSession` + `ReparkSessionBuilder` (file-backed tests). **G-6:** rustdoc
  intra-links fixed (private helpers named in backticks, not broken `[links]`;
  `Self::list_iceberg_table_names` for the live list path). **ICE-READ-PERF-0 (2026-09-19):**
  `register_catalog_spec` builds Glue and S3 Tables catalogs through
  `glue_catalog_counted` / `s3tables_catalog_counted` with the session's I/O counters
  (see `session/map.md`). pins: ice-read-perf-0/C-003 **ICE-CATALOG-CACHE-1 (2026-09-19):**
  both now receive the whole `CatalogCaches` (`iceberg_caches::caches_of`), so configured and
  late-configured Glue and S3 Tables catalogs get the session's metadata and manifest caches.
  pins: ice-catalog-cache-1/C-002 **ICE-WRITE-OPTIONS-1
  round 3 (2026-09-17):** `sql_with_write_options` runs the session dialect's
  `execute_with_write_options` (see `dialect.rs`). **Run 22b rebase (2026-09-18,
  Q-22b-WO-1):** ICE-DYN-OVERWRITE-1's `static_overwrite.rs` (`sql_with_overwrite_flag`,
  `sql_static_overwrite`) is retired; `session/write_options.rs` is the one statement
  funnel. **ICE-OVERWRITE-MODE-1 (2026-09-19):** `sql_with_write_options(query, options,
  overwrite_intent)` fills `EngineContext::overwrite_intent` (`Session` / `Static` for
  `saveAsTable` / `Dynamic` for `writeTo.overwritePartitions`), and `sql_with` calls it with an
  empty map and `Session`. pins: ice-dyn-overwrite-1/L-001; ice-write-options-1/C-014;
  ice-overwrite-mode-1/C-007
  **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the builder folds the
  `spark.sql.iceberg.*` write confs from its config map into the session
  (`with_session_write_conf`), so the funnel's `from_ctx` read answers them. Builder collects
  the Spark-style `.config(...)` map (`config(key, value)` / `configs(map)`); sync `build()`
  validates knobs, parses the config's `spark.sql.catalog.<name>.*` /
  `repark.sql.catalog.<name>.*` blocks into `CatalogSpec`s (fail-loud, synchronous), threads every
  `datafusion.*` key from the same map onto the `SessionConfig` via
  `apply_datafusion_config_keys` (P2G R2 — `DATAFUSION_CONFIG_PREFIX`; applied after the typed
  setters + core defaults so an explicit conf wins, before the extension hook; an unknown key is
  an `Error::Config`, never silently inert — this is what makes
  `datafusion.catalog.information_schema = true` real and Q8's `SHOW TABLES` / `DESCRIBE` /
  `information_schema.*` live in BOTH SQL doors), carries the parsed `SourceSpec`s from a
  loaded `repark.toml` on `source_specs` (CFG-2 step 1 — one `Arc<SourceSpec>` per spec so
  the registry and the per-query registry snapshot clone atomics, not deep specs;
  `register_configured_sources` and the `sources()` / `source(name)` doors live in
  `named_sources.rs`), carries the
  **two DF-54.1 regression guards**
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
  conf key > default; CFG-1 step 3: `from_config_file(path?)` forces a file (`None` keeps
  automatic discovery), `build()` resolves the file first and merges its pairs UNDER the
  builder map (builder wins per key) with the file's `session` knobs as typed fallbacks,
  printing the file's discovery warnings (REVIEW-FIX-7 step 1, 2026-09-10:
  pins: review-fix-7/C-003),
  and the session keeps the redacted `(key, value, source)` `conf_dump()`), attaches the
  write/scan knobs as
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
  `read_csv` (delegates to `read_options::read_csv_path`) / `read_json` (Spark-style option maps; both apply the `flag_secret_columns` policy on the read schema — D-4), `read_iceberg_table` + `TimeTravelOpts`
  (snapshot-id / as-of-timestamp / branch / tag, mutual exclusion), and the `testing_` seams
  (`testing_create_ref` / `testing_list_snapshots` / `testing_oob_create_table` /
  `testing_oob_drop_table`). Excel/postgres readers are deferred with their crates. The file's
  accretion of session policy is deliberate (everything-through-Session); its decomposition into
  named internal services is **deferred and driver-gated** —
  [../../../docs/adr/0005-defer-session-decomposition.md](../../../docs/adr/0005-defer-session-decomposition.md)
  names the triggers, so do not split it opportunistically.
  **REVIEW-FIX-5 C-006 (2026-09-10):** `build()` installs `DescribeOwnerConfig` once via
  `with_session_owner` with the owner snapshotted from the process environment at build;
  the query path never reads the environment. `catalogs_snapshot` is `pub` so doors
  outside this crate can route over the session's registry.
  pins: review-fix-5/C-006
  **EAGER-BUDGET-1 step 1 (2026-09-13):** the file gained one `mod cache_budget;` line —
  the D-2 retained-bytes accounting (`retained_cache_bytes` /
  `distinct_buffer_bytes`) lives in `session/cache_budget.rs`, keeping this file under
  the default ceiling at 988.
  pins: eager-budget-1/C-002
  **EAGER-BUDGET-1 step 2 (2026-09-13):** the D-1/D-3 incremental admission loop and the
  live-buffer seed (`live_cache_buffer_set`) live in `session/temp_views.rs` and
  `session/cache_budget.rs` — this file is unchanged.
  pins: eager-budget-1/C-005, C-007
- `session_owner.rs` — the session-built DESCRIBE owner: `DescribeOwnerConfig`
  (`repark.describe` prefix, `owner`, default `unknown`), the build-time
  `session_owner_snapshot` (`USER`, then `USERNAME`, then `unknown`), and the
  `with_session_owner` installer mirroring `with_repark_sql_config`. Moved here from the
  Spark door (R5-C) because only this crate builds the session; the Spark door imports
  the type and the test session build reuses the snapshot. No `//` comments; the reasons
  live on this row.
  pins: review-fix-5/C-002, C-006
- `stack.rs` (+ [stack/](stack/map.md)) — **PERF-UNPIVOT-1:** `Unpivot` logical node,
  `UnpivotExec`, `apply_stack`, the labeled describe door `apply_labeled_stack` /
  `StackLabels`, marker `stack` UDF, Spark-door `StackRewrite`.
  pins: perf-unpivot-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- `freq_items.rs` — **DF-RUST-3 (2026-09-15):** Spark's `FreqItemCounter` as a
  DataFusion `AggregateUDFImpl` + `Accumulator` over `HashMap<FreqKey, i64>` —
  capacity `floor(1/support)`, the KSP add/merge (negative-remainder branch keeps
  reduced entries), eval dumps keys with no threshold filter, null keys counted.
  `FreqKey` gives `Float32`/`Float64` keys IEEE `==` (`+0.0`/`-0.0` one key, every
  `NaN` equal to nothing), non-null `Map` keys identity (never equal — Spark's
  `MapData`; a NULL map stays a NULL key and dedupes), and every other type
  `ScalarValue` semantics, so values nested in `array`/`struct` stay bit-exact
  like Spark's boxed `equals` (round-2 R-9, round-3 R-13, round-4 R-14).
  `freq_items` runs one aggregate over the named columns
  and answers the `<name>_freqItems` projection.
  pins: df-rust-3/C-001, C-002, C-007, C-010, C-011
- `na_fill.rs` (+ [na_fill/](na_fill/map.md)) — **LOGICAL-WIDTH-1 (2026-09-16, round 2,
  R-12):** `na_fill_expr` builds the `na.fill` replacement in Rust — the fill literal is
  cast to the target column's own Arrow type for every integer/uint/float width (Spark's
  cast-the-literal rule, float-into-int truncates) and left uncast otherwise, then
  `coalesce`d under the bound expression. The column type is read from the plan schema
  (engine name, display-name fallback, last duplicate wins); a miss leaves the literal
  uncast. File-backed pins: `na_fill/tests.rs`.
  pins: logical-width-1/C-012
- `transpose.rs` — **DF-RUST-3 (2026-09-15):** the `ResolveTranspose` algorithm as an
  eager kernel (`transpose_frame`): filter null index rows, enforce
  `spark.sql.transposeMaxValues`, collect once, stable-sort ascending on the raw index
  scalar, cast value cells to the `findTightestCommonType` result, and build the
  `key` + one-column-per-index-row matrix over a `MemTable`. Binary index values
  decode lossily — one U+FFFD per invalid UTF-8 byte (round-2 remediation R-10).
  Conditioned errors
  (`TRANSPOSE_INVALID_INDEX_COLUMN`, `TRANSPOSE_NO_LEAST_COMMON_TYPE`,
  `TRANSPOSE_EXCEED_ROW_LIMIT`) carry `[CONDITION] … SQLSTATE:` in the message.
  pins: df-rust-3/C-003, C-004, C-008
- `update_fields.rs` (+ [update_fields/](update_fields/map.md)) — **COLUMN-PARITY-1
  (2026-09-14, critic round):** Spark `UpdateFields` as the `update_fields` scalar UDF
  plus `update_fields_call` / `register_update_fields`, registered on the session in
  `session/df_guards.rs`. Sequential edit application at plan and exec time with
  parent-validity masking so NULL structs stay NULL through field extraction.
  Re-check round 2 (2026-09-15): the mask ANDs validity bitmaps onto shared value
  buffers; `spark_sql_type` is shared `pub(crate)` for the `isnan` refusal below.
  pins: column-parity-1/C-008, C-009
- `isnan.rs` (+ [isnan/](isnan/map.md)) — **COLUMN-PARITY-1 (2026-09-14, critic round):**
  the `repark_isnan` scalar UDF plus `repark_isnan_call` / `register_repark_isnan`,
  registered on the session in `session/df_guards.rs`. Float/string test the DOUBLE
  value (strict cast, malformed errors). Re-check round 2 (2026-09-15): struct, array
  and map refuse at plan time with `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`; all
  other non-float types answer false.
  pins: column-parity-1/C-008, C-009
- [dynamic_flatten.rs](dynamic_flatten.rs) (+ `dynamic_flatten/`) — **DF1 native `dynamic_flatten`:** free
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
  **U10 / R-DF-LOAD-PATH (2026-09-23):** `mod iceberg_path;` joined the module list at
  the exact 155-line baseline; the stale half-comment above the `error_map` re-export
  (its note is already carried by `session.rs`'s own line) was the line shed for it.
- `iceberg_path.rs` (+ [iceberg_path/](iceberg_path/map.md)) — **U10 / R-DF-LOAD-PATH +
  R-DF-LOAD-METADATA-JSON (2026-09-23):** `ReparkSession::read_iceberg_path`, the
  `format("iceberg").load(<path>)` arm — Spark's `IcebergSource` rule applied at the
  engine door. A path ending `.metadata.json` loads that exact file through the fork's
  `StaticTable::from_metadata_file`; any other path is a table location that strips one
  trailing `/`, prefers `<loc>/metadata/version-hint.text` (`v<N>.metadata.json`), else
  lists the direct children of `<loc>/metadata/` for the highest leading-integer metadata
  file in either the `NNNNN-<uuid>` or `v<N>` form, with `v<N>` and hint versions bounded
  to the Java `int` range. Lower-case `file://<authority>` and `file:<relative>` arguments
  refuse Spark's texts before any I/O, and `file:/abs`-style spellings read as
  `file:///abs`. A location with no resolvable metadata raises `Error::Analysis` naming the
  supplied path. The `FileIO` comes from
  `repark_iceberg::catalog::file_io_for_location` on the normalised spelling
  (scheme-selected local fs / s3 / s3a),
  the read-only static table feeds `IcebergStaticTableProvider::try_new_from_table`, and
  the provider goes straight to `SessionContext::read_table` — no registration is left
  behind and no time-travel or incremental option reaches the path route.
  pins: dfload-1/C-002, C-003, C-004, C-005, C-007, C-008, C-009, C-010
- `plan_canonical.rs` — **DF-PLAN-INTROSPECT-1 (2026-09-15, round 4):** the
  expression-canonicalization half of the hash, split out when the expression
  family outgrew `plan_introspect.rs`. `RelTable` numbers scans and subquery
  aliases in walk order (view-backed scans expand inline, cache-view scans
  expand through the lineage map, leafless aliases take an ordinal of their
  own) and resolves every column reference to `(relation ordinal, column
  name)`; the comparison strip consults the pre-analysis user-cast set (a
  column-side cast beside a bare literal whose value fits the native leaf type
  blocks, coercion still strips); binary comparisons canonicalize operand
  order with operator flips and `AND`/`OR` chains flatten and sort.
  pins: df-plan-introspect-1/C-013, C-015
- `plan_introspect.rs` — **DF-PLAN-INTROSPECT-1 (2026-09-14; follow-ups 2026-09-15,
  rounds 1–4):** three plan-introspection kernels over a `DataFrame`'s plan.
  `input_files` walks the built (never executed) physical plan and collects every
  `FileScanConfig` file-group entry behind a `DataSourceExec`, rendered as the
  object store names it (`file:///` for local paths, remote schemes unchanged) and
  de-duplicated in first-appearance order; a plan with no file scan answers empty.
  `semantic_hash` always analyzes first, then streams the analyzed plan into a
  process-seeded hasher with no intermediate string: output alias names are
  skipped, column names hash with hex runs of 8+ normalized to `#`, every column
  reference hashes as its resolved relation ordinal plus its name (see
  `plan_canonical.rs`), scan-level pushed-down filters are not hashed
  (the `Filter` node above carries them), file scans fingerprint by listing-table
  paths, `generate_series` by bounds, memtables by construction identity
  (independently built frames never share a hash, same data or not — Spark answers
  the same), cube / rollup / grouping sets by distinct tags, and cache-view scans
  expand through the caller-supplied lineage map while other temp-view scans expand
  inline. Identity projections (each output the input column in order, bare or
  same-named alias) and subquery aliases strip; integer-literal casts fold to the
  target width, and an integer comparison between a column (through int-widening
  casts only) and an integer literal hashes as operator plus bare name plus `i128`
  value, so the string, Column, and SQL doors hash one filter one way. A column
  cast the user wrote (a cast beside a bare literal in the pre-analysis plan
  whose value fits the native leaf type) blocks that strip, so narrowing and
  DF-door widening casts hash apart from the plain
  column; casts the analyzer or the SQL door inserted for coercion still strip
  (a SQL-door widening cast is indistinguishable from coercion after eager
  analysis — OPEN C-015).
  `same_semantics` compares the canonical analyzed-plan byte streams (never the
  32-bit fold alone). The fold keeps the low 32 bits of the digest as a signed
  int. Depth-200 chained-filter hash medians 0.0060s on the always-analyze build
  (2026-09-15, release module, round 4). The Iceberg scan exec exposes table and snapshot
  only, never materialized data files, so Iceberg frames answer empty.
  pins: df-plan-introspect-1/C-001, C-002, C-006, C-007, C-008, C-009, C-010, C-011, C-013, C-014
- `error_map.rs` — `engine_err` (pub — the single `DataFusionError → repark_common::Error`
  classifier): `SQL` → `Parse`, `Plan`/`SchemaError` → `Analysis`, `NotImplemented` →
  `NotImplemented`, `External` downcast first to repark-iceberg's `CommitStateUnknownError`
  stamp (→ `Error::CommitStateUnknown` with the minted `engine.operation-id`, ICE-COMMIT-UNKNOWN-1),
  then to `IllegalArgumentMarker` (defined in repark-iceberg's `write/illegal_argument.rs` since ICE-SESSION-WRITE-CONF-1 round 1 (2026-09-19) and re-exported here unchanged; the `IllegalArgumentMarked` arm → `Error::IllegalArgument`, so the CALL options
  validation raises `IllegalArgumentException` — **ICE-RDF-OPTIONS-1 round 1, 2026-09-17**),
  then to `UnsupportedMarker` (**ICE-VIEWS-1 R2, 2026-09-21:** the `UnsupportedMarked` arm → `Error::NotImplemented` verbatim, so a viewless CREATE/REPLACE refuses with Spark's exact bytes),
  then to a live `iceberg::Error` → classified by its
  structured `ErrorKind` (`classify_iceberg_error`, the ONE iceberg kind→class mapping — also
  the direct `iceberg_err` fold; an unstamped `CommitStateUnknown` kind maps to the same
  variant with `operation_id: None`), wrappers peeled iteratively (bounded,
  `MAX_ERROR_PEEL_DEPTH`), everything else → base `Error::DataFusion`. Postgres/excel folds are
  deferred with their crates. **ICE-BRANCH-OPS-1 (2026-09-17):** `Configuration` →
  `IllegalArgument` (Spark raises it for invalid procedure arguments and option values;
  previously fell into the base class). Also `resolve_s3_region_override` (dual-key S3 read-region
  override; identical values collapse, different values fail loud naming both keys).
  **UNRESOLVED-ROUTINE-1 (2026-09-16):** `engine_err_for_sql` reshapes unknown-routine
  texts before classification (never for `Parse`), so `sql_with` answers Spark's shape
  on every dialect. pins: unresolved-routine-1/C-001
  (ORCH-001 remediation: the constructor sits above `engine_err`'s doc comment.)
  **IPI-51 PR2 (2026-09-20):** the hand-formatted `[CONDITION]` strings in
  `text_scan.rs`, `orc_scan.rs`, `time_travel.rs`, `column_resolution.rs`,
  `stack.rs` (+ `stack/udf.rs`), and `session_time_zone.rs` render through
  `repark_common::spark_error`, byte-identical. `stack.rs` keeps `Result` and `engine_err` and drops the unused `Error` import the M-1 move left behind.
  **FNP-MATH-1 WO-6b R3 (2026-09-21):** an `Execution` message headed
  `[ARITHMETIC_OVERFLOW]` routes to `Error::Arithmetic` carrying the inner message
  verbatim (never the `Execution error: ` display); `Analysis` peels DataFusion's
  `user-defined coercion failed with: ` wrap to the `[CONDITION] … SQLSTATE: XXXXX`
  refusal payload when one is present, else keeps the full display. pins: fnp-math-1/C-004
  **WO-A4 (2026-09-23):** a bracketed `ParserError::ParserError` payload stays a `Parse` error
  and renders verbatim; unbracketed payloads and other parser variants keep the DataFusion display.
- [unknown_routine.rs](unknown_routine.rs) — **UNRESOLVED-ROUTINE-1 (2026-09-16):** the blanket reshape
  (see [../map.md](../map.md)).
  **Remediation round 1 (2026-09-16):** token-based call-site matching (see
  [../map.md](../map.md)). pins: unresolved-routine-1/C-001, C-002
- `pool_refusals.rs` — **H3-SPILL-RESIDUE-1 (2026-09-06):** `PoolRefusalLog` (a refusal counter
  plus the engine's own last refusal text), `RefusalRecordingPool` (a `MemoryPool` decorator that
  delegates every method to the inner `FairSpillPool` — name, `Display`, `memory_limit`, both
  grow paths — and records only the `try_grow` refusals), and `pool_refusal_log`, the accessor
  that recovers the log from an `Arc<dyn MemoryPool>`. It exists because a bounded pool must
  answer with a typed refusal: DataFusion 54.1's `NestedLoopJoinExec` re-executes partition 0 of
  its build child on the OOM fallback path (`nested_loop_join.rs::initiate_fallback` after
  `NestedLoopJoinExec::execute` already ran it), which `RepartitionExec` answers with
  `expect("partition not used yet")`. repark cannot patch the dependency, so it reports the
  refusal that caused the panic instead of a bug report. The decorator is the only way to see a
  refusal from outside DataFusion — the pool trait has no hook.
  pins: h3-spill-residue-1/C-002, C-003
  **NEVER-OOM-PANIC-1 (2026-09-16):** `nlj_build_reset.rs` now removes the nested-loop-join
  double-execute at the root, so this path spills (or refuses typed) and no longer reaches
  the containment; the log stays as the tightness proof and the containment stays for the
  other allow-listed fallback paths.
  **Round 2 (2026-09-16):** also home to `REFUSAL_CONTAINMENT_NOTE`, moved here from the
  export reader so the fallback refusal and the containment rewrite share one string.
- `nlj_build_reset.rs` — **NEVER-OOM-PANIC-1 (2026-09-16):** the physical-optimizer rule
  `NljBuildSideReset` (appended last, so the enforcer's `RepartitionExec` is already under
  the build side) plus the wrapper exec `NljBuildSideExec`. The rule wraps each
  `NestedLoopJoinExec` left child once (idempotent); the wrapper's `execute` runs a
  `reset_plan_states` clone of its child, so the OOM fallback's second `execute(0)` meets
  fresh `RepartitionExec` channels and really spills instead of hitting
  `expect("partition not used yet")` and poisoning the shared once-future. Properties,
  schema, partitioning, fetch and limit-pushdown support delegate to the inner plan;
  `metrics` is `None` (per-execute clones own theirs). A child count other than one is an
  `Internal` error, pinned. Wired in
  `session/df_guards.rs::context_with_df_54_1_rule_guards`.
  pins: never-oom-panic-1/C-004, C-005, C-006
  **Round 2 (2026-09-16):** the wrapper carries a `BuildSidePolicy` the rule reads off the
  join (`NljBuildSideExec` counts its executes; only the first is the in-memory load, the
  second is always the fallback re-execute). `BuildSidePolicy::for_join` refuses the
  fallback for DataFusion 54.1's documented unsafe set (LEFT, LEFT SEMI, LEFT ANTI, LEFT
  MARK with more than one right partition): the second execute returns `ResourcesExhausted`
  built from the session's own recorded pool text plus `REFUSAL_CONTAINMENT_NOTE` (moved to
  `pool_refusals.rs` so the export reader reuses the one string), never a row. Every other
  shape spills, including single-right-partition left-family joins. With no recorded refusal
  the wrapper spills rather than fabricate. `metrics()` stays `None` (residue R-02). The
  execute counter is an `AcqRel` atomic with no lock (the two executes are sequential: the
  fallback runs only after the load resolves); `for_join` takes the one-byte `JoinType` by
  value and the test-only `policy()` getter is `#[cfg(test)]`.
  pins: never-oom-panic-1/C-011, C-012, C-013
- `catalog_config.rs` — the `spark.sql.catalog.<name>.*` → `Vec<CatalogSpec { name, kind,
  props }>` parser (`parse_catalog_specs`, pure/AWS-free). Both prefixes share one keyspace
  (cross-spelling duplicates collapse when identical, fail loud otherwise). Rules: bare
  `…catalog.<name>` = the Spark catalog class or a short kind; `<name>.catalog-impl` ending
  `GlueCatalog`→`Glue` / `S3TablesCatalog`→`S3Tables` / `InMemoryCatalog`→`Memory`;
  `<name>.type` = `glue`/`s3tables`/`memory`/`hadoop` (`memory` requires `warehouse`,
  `hadoop` aliases to it per INDEX 25); `<name>.io-impl` dropped; every other prop passes
  through verbatim (an S3 Tables `warehouse` ARN is carried into `table_bucket_arn` when the
  latter is absent). Fail-loud `Error::Config` naming the exact key. Registration policy: Glue
  `RequireExplicitLocation`; S3 Tables `ServiceManagedLocation`; memory keeps the temp
  fallback. `CatalogSpec` hand-written `Debug` redacts secret-like prop values.
  **ICE-CATALOG-SESSION-1 S7 (2026-09-20):** `kind_from_type` + `kind_from_catalog_impl`
  move to `catalog_kind.rs` (1028 → 1007, ratcheted); the accepted sets gain `hadoop` and
  `InMemoryCatalog` (both → `Memory`), with the refusal texts updated.
  pins: ice-catalog-session-1/C-025, C-026, C-030
  **PR-B hadoop naming (2026-09-24):** the `type` arm records a trimmed, case-insensitive
  `hadoop` value on the block. `into_spec` then adds `metadata-naming=hadoop` to the spec props
  through `catalog_kind::with_type_naming`, unless the user set `metadata-naming` explicitly.
  An explicit value passes through verbatim, and the fork validates it. `type=memory`,
  `catalog-impl=…InMemoryCatalog` and a bare `spark.sql.catalog.<name> = hadoop` value add
  nothing. `kind_from_bare_catalog_value` moved to `catalog_kind.rs` so the file stays under
  its baseline (1007 → 1006, ratcheted). The unit pins are in
  [catalog_config/hadoop_naming_tests.rs](catalog_config/map.md).
  **REVIEW-FIX-5 (2026-09-10):** `prop_key_is_secret` is `pub` (re-exported at the crate
  root) so `DESCRIBE TABLE EXTENDED` redacts through the same predicate; no second
  predicate exists. pins: review-fix-5/C-004
  **REVIEW-FIX-5 D-5 (2026-09-10):** the file carries no `//` comments; the moved reasons
  live here. `prop_key_is_secret` folds hyphens and dots to underscores so dotted and
  hyphenated keys share needles, then strips underscores so camelCase and one-word keys
  share them with snake_case; the substring cover reaches `aws_secret_access_key`,
  `s3.access-key-id` and `session_token`, `user_info`/`userinfo` catch the Kafka/JDBC
  `user:password` blob, and the trailing `_key` arm carves out `bucket`/`arn` names (a bare
  `.key` needle is unreachable after the fold). `CatalogSpec` `Debug` sorts keys so output
  is deterministic under `HashMap` order. `parse_catalog_specs` normalises both prefixes
  into one keyspace before building; a cross-spelling value conflict names both keys and
  never interpolates values (props can carry credentials). The `split_once('.')` arms are
  the bare kind value, `<name>.<prop>`, and the double-dot empty-name refusal. `io-impl`
  is dropped because iceberg-rust `FileIO` is not pluggable by Java class name. The
  C1-SEC-002 matrix additionally pins hyphenated OpenDAL/Spark spellings (C2-SEC-002),
  camelCase and one-word spellings, `privateKey` plus OAuth `bearer`, and the
  `basic.auth.user.info` blob; the kind matrix pins the `repark.sql.catalog.m = memory`
  synonym of the bare Spark spelling.
- `read_options.rs` — CSV/JSON Spark option-map helpers, `read_csv_path` (nullValue
  all-Utf8 scan; `utf8_columns` re-read so timestamp CAST sees raw offset text), and the
  local-CSV first-line Utf8 schema. **CSV-INFER-PERF-1** moved the CSV read body here so
  `session.rs` could drop under the default file-size ceiling. Round 2: `inferSchema`
  without `nullValue` keeps DataFusion's 1000-row sample. Round 3: `utf8_columns`
  forces an all-Utf8 schema from the first local record (no second infer), so a
  `multiLine` re-read past 1000 records cannot raise on chunked record boundaries.
  Round 4: a `multiLine` first read also uses that infer-free schema, so quoted
  fields with embedded newlines infer at any record count. Round 5: an embedded
  newline inside the header still raises (line-based first-record read);
  DECLARED `CSV-INFER-HEADER-NEWLINE`.
  **TORTURE-1 step 3 (D-4/D-4a):** `flag_secret_columns` = `off|warn|refuse` (default
  `off`; `flagsecretcolumns` is the folded alias; any other value refuses naming the
  option and the three values, before any I/O). `read_csv_path` applies the flag once on
  the final frame and `session.rs::read_json` does the same: `warn` eprintln's one
  `WARNING: flag_secret_columns=warn` line naming the flagged top-level columns,
  `refuse` raises `Error::Analysis` naming them — `prop_key_is_secret` on each schema
  name verbatim, no nested walk, no value inspection. Readers without the csv/json
  option map (parquet, table) never see the key; the facade refuses it loud there.
  pins: nullability-2/C-006
  pins: csv-infer-perf-1/C-002, C-005
  pins: torture-1/C-018, C-020
- `text_scan.rs` — **IO-TEXT-1 (2026-09-14):** the Spark `text` scan. **IO-TEXT-1 (2026-09-15, orchestrator):** `text_scan.rs` carries no doc comments (the unit's workers are briefed comment-free); the two public `Result` entry points take `#[allow(clippy::missing_errors_doc)]` instead.
  A `TableProvider` over sorted local files, plain dirs (hidden `_`/`.` skipped,
  `key=value` dirs descended), and Hadoop globs (see `text_glob.rs`), serving one
  nullable `value` Utf8 column through `StreamingTableExec` over at most 8 contiguous
  file-group partitions: universal `\n`/`\r\n`/`\r` splitting (or one custom `lineSep`)
  with one trailing terminator dropped, `wholetext` one row per file, chunked reads with
  separator hold-back so batches stream without buffering a file (wholetext excepted),
  invalid UTF-8 decoding lossy, the plan limit threaded into the scanner, missing paths
  and unmatched globs answering `PATH_NOT_FOUND`. **Round 3 (2026-09-15, U-3):**
  directory reads append discovered partition columns after `value` (see
  `partition_discovery.rs`), honoring value-only, partition-only, and empty
  projections; the scanner stops appending at the row limit and emits at once.
  `ReparkSession::read_text` lives here
  as an inherent impl so `session.rs` keeps its size; Rust tests cover the split arms,
  the globs, the limit, and the error texts. **Round 4 (2026-09-15, W-1..W-4,
  V-2):** the user-schema overlay lives in `text_schema.rs` (this module passes
  the pairs plus `basePath` through); partitioned leaves win over root files;
  bare globs discover nothing while `basePath` globs discover beneath the base;
  the partition-dir walk is an explicit stack. **Round 5 (2026-09-15, X-1):**
  the battery moves verbatim to [`text_scan/`](text_scan/map.md).
  **Round 5 (2026-09-15, X-5):** the per-file partition values ride one `Arc`
  into every scan partition and `execute` instead of a clone per partition.
  **Round 7 (2026-09-15, Z-1):** `expand_text_paths` takes the session zone
  so inferred timestamps parse session-local.
- `text_glob.rs` — **IO-TEXT-1 follow-up (2026-09-15):** hand-written Hadoop glob
  matcher (`*?[]{}`, no `/` crossing, char-aware, brace nesting capped, no new
  dependency) with matcher unit tests. **Round 3 (2026-09-15, U-5/U-6):** each
  pattern parses once to per-segment tokens (the old `match_segment`/`match_glob`
  pair is deleted); `\`-escaped meta matches literally, and a pattern whose last
  segment names a directory lists one leaf level through an iterative walk.
  pins: io-text-1/T-5, U-5, U-6
  **IO-ORC-1 (2026-09-16):** adds `match_file_name_glob` (one-segment match for the
  ORC `pathGlobFilter`); the matcher itself is unchanged. pins: io-orc-1/C-006
- `text_io.rs` — **IO-TEXT-1 (2026-09-14):** the Spark `text` writer.
  **Follow-up (2026-09-15):** the writer streams `execute_stream` batches into
  sequential `part-*.txt` (NULL rows write empty lines, every row terminated; an empty
  frame still writes one empty part), writing array bytes direct. Schema check stays
  offender-first with Spark's `UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE` text, then the
  verbatim 1290 count text; empty `lineSep` refuses as `IllegalArgument` (Spark's
  class; the partitioned writer already refused so). Rust tests cover the error texts
  and the round trip. Partitioned fan-out lives in `text_partition.rs`.
  pins: io-text-1/C-002, T-2, T-4, T-7, U-9
  pins: io-text-1/C-001, C-002
- `partition_discovery.rs` — **IO-TEXT-1 round 3 (2026-09-15, U-3):** Hive
  partition discovery plus the shared batch materialization IO-ORC-1 reuses:
  `discover_partitions` takes leaf files under a root and returns the
  partition schema (directory order) with per-file typed values, knowing
  nothing about text; beside it the projection planner (`plan_partition_slots`),
  the per-row push (`push_partition_row`), the text row emitter
  (`TextRowSink`/`emit_text_row`, stops at the row limit), and the batch
  finishers (`finish_partition_columns`, `order_batch_columns`). Unit tests
  cover unescape, order, default→NULL, inference, leaf paths, bigint.
  **Round 4 (2026-09-15, W-1/W-3):** same-depth name lists refuse
  `CONFLICTING_PARTITION_COLUMN_NAMES` `KD009`; beside them the user-type
  parser (`user_partition_type`: string/int/bigint/double/date),
  the raw caster (`cast_raw_partition_value`), and the
  `INVALID_PARTITION_VALUE` `42846` message builder.
  **Round 5 (2026-09-15, X-1/X-2/X-3):** every per-file record keeps the raw
  unescaped texts beside the inferred values; name lists that differ at any
  depth refuse through one shared message builder; user types gain decimal
  and session-zone timestamp with canonical decimal/timestamp text.
  **Round 6 (2026-09-15, Y-2):** user types gain boolean (case-insensitive,
  anything else refuses), float, smallint, tinyint, binary (raw UTF-8 bytes),
  timestamp_ntz (no zone), and `array<primitive>` (always refuses as
  `INVALID_PARTITION_VALUE` with Spark's uppercase display); map/struct stay
  unsupported-type refusals.
  **Round 7 (2026-09-15, Z-1):** inference gains the timestamp step after
  date (exactly `yyyy-MM-dd HH:mm:ss` infers session-zone `timestamp`; the
  `T` and fractional walls stay `string`); `discover_partitions` takes the
  session zone for the inferred values; `cast_raw_partition_value` splits
  the timestamp arm (zoned keeps X-1, `None` parses the naive wall); the
  X-1 wall grammar lives in `partition_timestamp.rs` with both parsers on
  it. New code and tests stay in the new module; this file keeps its
  ceiling with wiring only.
  pins: io-text-1/U-3, W-1, W-3, X-1, X-2, X-3, Y-2, Z-1
- `partition_timestamp.rs` — **IO-TEXT-1 round 7 (2026-09-15, Z-1):** the
  zone-free wall clock beside discovery (split from `partition_discovery.rs`
  at the 1000-line ceiling): `parse_wall_naive` carries the shared wall
  grammar (date-only, space/`T`, optional fraction),
  `parse_timestamp_ntz_micros` stamps the naive wall, and
  `looks_like_timestamp` admits exactly the space wall with no fraction for
  inference. Unit tests pin naive midnight/walls/refusals, the unchanged
  zoned walls, and discovery types and values under New York.
  pins: io-text-1/Z-1
- `partition_overwrite_mode.rs` — **ICE-DYN-OVERWRITE-1 (2026-09-17):** the
  `spark.sql.sources.partitionOverwriteMode` session knob beside the timezone
  and ANSI carriers: `PARTITION_OVERWRITE_MODE_KEY` (the one spelling),
  `PartitionOverwriteMode` (`Static` default via derive / `Dynamic`), the case-insensitive
  parse (unknown values refuse with Spark's
  `[INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS]` class), the
  `PartitionOverwriteModeConfig` live carrier (`repark.overwrite` prefix, unsettable),
  plus the build-map installer and the `SessionContext` reader. **ICE-OVERWRITE-MODE-1
  (2026-09-19):** re-exports `repark_iceberg::write::OverwriteIntent` as
  `repark_core::OverwriteIntent`, so the binding (no `repark-iceberg` edge) names the typed
  per-write intent. pins: ice-overwrite-mode-1/C-007
  pins: ice-dyn-overwrite-1/C-007, C-010, C-014
- `text_partition.rs` — **IO-TEXT-1 round 3 (2026-09-15, U-1+U-2):** the one-scan
  `partitionBy` text writer (`write_text_partitioned`, exported at the crate root).
  One `execute_stream` pass routes each row to its leaf writer by rendered key
  (bounded LRU of 256 open part writers, `TEXT_PARTITION_WRITERS_CAP`; round 4,
  ruling V-1: an evicted key reopens its `part-00000.txt` in append mode, so a
  shuffled write holds one part per leaf); partition columns drop from the body and
  the remaining column keeps the single-string check with Spark's verbatim 1290
  text. Leaf names use Hive `escapePathName` (`%XX` uppercase; space, non-ASCII
  and `}` literal); NULL and empty write `__HIVE_DEFAULT_PARTITION__`; decimals
  render plain (`1.50`), booleans lower-case, dates `yyyy-MM-dd`, timestamps in
  the caller-passed session zone with trimmed fractions, doubles and ints plain.
  Rust tests cover the escape set, decimal rendering, the fan-out, the 1290,
  and the append-on-evict row count. **Round 5 (2026-09-15, X-4):** the first
  eviction diverts the tail to `text_partition_fallback.rs` (sorted
  single-writer append); the eviction arm stays pinned by direct unit test.
  **Round 6 (2026-09-15, Y-1):** the divert hands over the live stream plus
  the frame's session task context.
  **Round 7 (2026-09-15, Z-2):** the writer returns the sort spill count
  (zero when the fallback never runs); the binding is unchanged.
  pins: io-text-1/U-1, U-2, V-1, X-4, Y-1, Z-2
- `text_partition_fallback.rs` — **IO-TEXT-1 round 5 (2026-09-15, X-4):**
  Spark's high-cardinality fallback: the tail past 256 distinct keys sorts by
  the partition columns and appends key by key with one open writer onto each
  key's existing part file. **Round 6 (2026-09-15, Y-1):** the remaining
  stream feeds the sort through a single-partition `TailSourceExec` under the
  session task context (session pool and disk manager, so the sort spills),
  and the single-writer walk consumes the sort output batch by batch —
  nothing is collected. The function returns the sort spill count. Rust tests
  pin one part per leaf past the cap, the unchanged below-cap layout, and a
  spill under a 16 MiB pool over a 54 MiB tail (spill count above zero, every
  row written, one part per key).
  **Round 7 (2026-09-15, Z-2):** the tail re-chunks from the session pool
  (one batch plus the sort reservation fits, capped reservation on a derived
  task context; oversized batches split by rows into deep copies with
  measure-verify) before the same sort and walk. Rust tests pin the 500k x
  4 KiB tail at 128/512 MiB pools, the pool-busting single batch (red
  without the split), and the rechunk contract.
  pins: io-text-1/X-4, Y-1, Z-2
- `text_schema.rs` — **IO-TEXT-1 round 4 (2026-09-15, W-1):** the user-schema
  overlay beside the scan (split from `text_scan.rs` at the 1000-line ceiling):
  the user schema is the data schema with discovered columns appended after it,
  named partition types override inference, bad casts refuse
  `INVALID_PARTITION_VALUE` `42846`. **Round 5 (2026-09-15, X-1):** a named
  column recasts from the raw directory text through one shared row builder.
  pins: io-text-1/W-1, X-1
- `spark_nullable.rs` — **CUTOVER-SCHEMA-1 (2026-09-04):** Spark-style nullability
  derivation. `relax_schema_to_nullable` marks every field nullable over
  struct/list/map (map keys stay required — Arrow forbids nullable map keys); the walk
  is iterative and unbounded since NULLABILITY-2 (below); both CTAS doors
  derive their Iceberg schema through it, so derived columns store optional the way
  Spark stores them. `read_parquet_nullable` infers the file schema, relaxes it, and
  re-reads with the relaxed schema as the DataFusion schema override — the plan keeps
  a single TableScan, so EXPLAIN output is unchanged. The double infer costs one extra
  listing plus footer reads; S3 registration still happens first in `session.rs`.
  pins: cutover-schema-1/C-001, C-002
  **NULLABILITY-2 (2026-09-05):** the relax walk is iterative (explicit pre-order stack,
  bottom-up rebuild — no depth bound, no recursion blow-up) with identical type
  coverage: struct/list/map relax, map keys and the entries flag pass through, unknown
  shapes keep their type. Structural pins at depth 40 and 200, 600-deep all-nullable;
  the facade pins depth 40 end to end and documents Arrow's own footer-depth ceiling
  past it. Registry `CUTOVER-NULLDEPTH-1`.
  pins: nullability-2/C-005
  **DYNFLATTEN-LISTNULL-1 (2026-09-06):** after the nullability relax, `promote_parquet_null_types`
  maps Arrow `Null` (parquet physical INT32 + Null logical type) to `Int32`, recursive over
  its own struct/list/map walk, depth-bound 32 (`MAX_NESTED_TYPE_DEPTH` now serves only this
  promote walk; the relax walk above is unbounded). Spark's parquet reader does this; DataFusion
  keeps `Null`. The `LargeList` / `FixedSizeList` / `ListView` / `LargeListView` promote arms
  mirror the relax walk's list arms and are unreachable from DataFusion's parquet inference
  today (only `List` is produced); they are kept for shape symmetry, not pinned
  (DYNFLATTEN-LISTNULL-1 critic F2). CTAS still uses `relax_schema_to_nullable` only, so an actual SQL `VOID`
  column does not become int32 on write. `drop_null_lists=True` still drops `List(Null)`
  that never went through this reader.
  pins: dynflatten-listnull-1/C-002, C-006
- `column_resolution.rs` — **ICE-MIXED-CASE-1 (2026-09-17, Q-20b-2):** Spark-door
  case-insensitive column fold with normalization ON
  (`plan_statement_with_column_repair` / `sql_with_column_repair` fold the parsed
  statement once against the valid fields under `spark.sql.caseSensitive = false`,
  `rewrite_fragment_case` does the same for DML fragments; both emit backticked
  stored-case spellings, collisions refuse `[AMBIGUOUS_REFERENCE]` / `42704`).
  Round 21b integration: this module owns no config carrier — `spark.sql.caseSensitive`
  has one home, `repark_functions::case_sensitive::SparkCaseSensitiveConfig` (landed on
  main by ICE-RTAS-BYNAME-1), and the Spark door passes `case_insensitive` in as an
  argument. A second carrier would go stale the moment `SET spark.sql.caseSensitive`
  wrote only one of them. Round 21b step 2 (V-01): the SELECT-alias guard is positional —
  each query level records its projection expressions (always foldable) and its alias-reference
  slots (GROUP BY, HAVING, QUALIFY, SORT BY, ORDER BY); only an ident inside an alias-reference
  slot that names a SELECT alias (ASCII case-insensitive, Spark's alias resolution) is left for
  DataFusion to bind to the alias. Round 21b step 3 (V-02): after a `FieldNotFound` the fold
  runs against every referenced relation's stored fields — catalog schemas resolved once per
  statement (`catalog_fields`) plus each miss's `valid_fields` for CTEs and derived tables —
  scope by scope (innermost query first, then outward for correlated references), and replans;
  the loop ends when a miss repeats or a fold changes nothing. An outer spelling is never
  rewritten into an inner scope. The ambiguity sentence carries one option per matching field
  in the requested spelling, qualified by the relation's written parts, `SQLSTATE: 42704`
  (Q-21b-1, Q-21b-2). The fold itself lives in `column_resolution/fold.rs`.
  Round 21b step 4 (V-04): JOIN USING columns fold inside every query the visitor reaches
  (INSERT … SELECT, CTAS, subqueries, CTE bodies), each against its own SELECT's relations;
  identical stored spellings across the joined relations fold to that spelling.
  Round 21b step 5 (L-08): `audit_plan_for_ambiguity` indexes each node's input fields once
  (`input_twins`, `DFSchema::iter`, no merge and no `columns()` clone, skipped when no input
  field has an upper-case ASCII byte) and refuses any written reference — bare or qualified,
  exact case included — whose resolved column has an ASCII case twin there. Options are one per
  matching field, qualified by the relation as written in FROM (`WrittenRefs::relation_parts`;
  the session's default `catalog.schema` prefix is dropped, so a temp view reads bare).
  Round 21b step 6 (R-02 / V-03 / R-05): a plan that succeeds first time is audited only when
  some node schema holds an upper-case ASCII field (`plan_has_upper_ascii_field`) — with none,
  case twins cannot exist, so neither the audit nor `written_references` runs. The written
  references walk the AST with the immutable `Visit` (no statement clone).
  Round 2 (2026-09-18): clippy `similar_names` — the audit's written-form hits are
  `relation_hit` / `bare_hit`.
  Round 2 N-02: a subquery expression's correlated outer references (`outer_ref_columns` of
  EXISTS / IN / set-comparison / scalar subqueries) are audited against the twins of the node
  that holds the subquery — the outer scope — so `EXISTS (… CAST(t.ID AS STRING))` over a twin
  input refuses instead of answering.
  Run 22b (2026-09-18, the debug-wheel segfault): `plan_statement_with_column_repair` wraps
  the repair future in `column_resolution/stack.rs`'s grown-stack poll, so the statement clone,
  the fold, DataFusion's planning (its per-level `Spanned::span` walk is unguarded upstream) and
  the final drop run on a stack sized to the statement's nesting depth. Both case modes go
  through it. IPI-51 PR6 slice 1 (2026-09-21): a `FieldNotFound` that survives the fold is
  stamped `UNRESOLVED_COLUMN.WITH_SUGGESTION` / `42703`.
  pins: ice-mixed-case-1/C-001, C-002, C-007, C-013, C-014, C-015, C-016, C-017, C-021, C-022
  pins: ice-error-conditions-1/C-011
- `column_resolution/stack.rs` — run 22b: `stack_bytes_for` (a `Visit` walk: open
  queries + their set-operation height + open expressions + open table factors, times
  32 KiB, plus 256 KiB) and `GrownStack`, a future whose every poll runs under
  `stacker::maybe_grow`. pins: ice-mixed-case-1/C-022
- `column_resolution/tests.rs` — the fold's unit battery (statement cells, fragment
  scoping, ambiguity shape, backticked exact under `true`, DataFrame filter alias
  binding). Split from `column_resolution.rs` under the file-size gate.
- `idents.rs` — table-identifier segment parse + path-escape refuse
  (`reject_path_escape_segment` delegates to `repark_iceberg::write::idents::path_escape_kind`
  — shared needles). **FNP-4B (2026-09-15):** segment unescaping generalized to the quote
  character (embedded backticks double). pins: fnp-4b/C-002
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
- `orc_footer.rs` — **IO-ORC-1 (2026-09-16):** the ORC footer attributes orc-rust drops:
  tail/postscript parse, block-framed decompress in all five codecs, and a minimal
  protobuf field walk returning per-column `spark.sql.catalyst.type` values plus the
  uniform stripe writer timezone (mixed zones read as absent). Best-effort: any decode
  failure yields an empty map and the normal footer validation errors surface.
  pins: io-orc-1/C-002
- `orc_scan.rs` — **IO-ORC-1 (2026-09-16):** the read-only ORC scan over orc-rust 0.8.0
  (owner ruling Q-15B-1). `OrcReadOptions` carries mergeSchema, pathGlobFilter,
  recursiveFileLookup, modifiedBefore/After, basePath, ignoreCorruptFiles, and the user
  schema; paths reuse `text_glob` plus partition discovery; the `TableProvider`
  projects through `ProjectionMask` and converts columns recursively (instant columns
  undo the stripe writer zone per file). Schema inference and the user-schema overlay
  live beside it in `orc_schema.rs` (split at the 1000-line ceiling).
  **Round 2:** `read_orc`/`expand_orc_paths` take a path list — per-item expansion
  merged into one file set (partition fields union by name, values Null-padded),
  so a list answers exactly like a directory; listings keep `*.orc` names only
  (repo bookkeeping is never data; a directly addressed file still
  footer-checks). **Round 3:** the filter is reverted (R-18b-12 — the scan reads
  every match, hidden names only); `OrcPartition::execute` streams batches from
  the ArrowReader through `OrcBatchStream` (R-18b-14, text_scan pattern), never
  a collected Vec. pins: io-orc-1/C-002, C-003, C-004, C-005, C-006, C-007, C-008
- `orc_schema.rs` — **IO-ORC-1 (2026-09-16):** the ORC schema half beside the scan:
  footer-attribute mapping (LONG→`timestamp_ntz`, instant→UTC-stamped `timestamp`,
  local-tz-kind→naive, recursive through struct/list/map), schema union by name under
  mergeSchema, and the user-schema select (case-insensitive names, missing→NULL,
  int→string arm, other mismatches loud). pins: io-orc-1/C-002, C-006, C-007
- `backend.rs` — the `ExecutionBackend` seam + `SingleNodeBackend`, its only implementation. One
  method, returning the concrete DataFusion `SessionContext`: the **trait boundary** is the
  load-bearing part, not the surface, which would have to widen (with its call sites) before a
  distributed backend could exist. Distribution is deferred by decision
  ([../../../docs/adr/0004-server-prep-disciplines.md](../../../docs/adr/0004-server-prep-disciplines.md)).
- `pre_execute.rs` (+ `pre_execute/tests.rs`) — **the shared pre-execute belt (round 5, Z-2):**
  `PreExecute` = plan (`create_logical_plan`, no execution) → `guard` (the ONE choke point for
  pre-execute refusals; today the tighten DDL-sink refuse and CFG-2's `refuse_source_ddl`,
  which refuses `LogicalPlan::Ddl` naming a registered database source — DataFusion's
  `drop_table`/`drop_view` swallow provider errors into "doesn't exist", and `CREATE CATALOG`
  would silently replace the source's provider, so the guard is the seam that refuses loud)
  → `execute`. The native door
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
- `range_table.rs` (+ [range_table/](range_table/map.md)) — **RANGE-TVF-ID-1
  (2026-09-18, round 1):** `SparkRangeFunc`, RePark's `range` table function —
  non-nullable `id: Int64` column, 1–4 literal arguments (the 4th is
  `numPartitions`, accepted and ignored for rows), `Utf8` bounds parsed as
  integers, zero step refused as a planning error; row generation reuses
  DataFusion's `GenerateSeriesTable` with `include_end = false`.
  `ReparkSessionBuilder::build` registers it over the built-in `range` after
  extension `register`, so the facade door and the native door share the one
  registration; `generate_series` keeps its `value` column.
  pins: range-tvf-id-1/C-001, C-002, C-003
  **RANGE-TVF-ID-2 (2026-09-19, round 1):** NULL bounds refuse as a planning
  error carrying `UNEXPECTED_INPUT_TYPE`; every Spark-accepted width coerces
  to `i64` (narrow and unsigned ints, decimal and float truncated toward
  zero, strings parsed with Spark's `CAST_INVALID_INPUT` text on malformed
  input); row generation is RePark's own `RangeTable` provider with a
  `RangePartition` stream over `StreamingTableExec` — the element count is
  Spark's `ceil((end - start) / step)` in `i128` arithmetic so overflow bounds
  emit exactly their rows, batches follow the session batch size, projection
  and the scan limit thread through, and the step-direction ordering stays;
  the 4th argument coerces to `i32` and a non-empty range with a non-positive
  partition count refuses with `IllegalArgumentException`.
  pins: range-tvf-id-2/C-001, C-002, C-003, C-004
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
  `SessionExtension` with three defaulted hooks (`configure` pre-assembly,
  `configure_analyzer_rules` during context assembly, and `register` post-context) at v1's inline
  registration positions; `NoopSessionExtension` is the
  no-extension baseline. `configure` takes a `SessionBuildConf` — the builder's raw conf map PLUS
  the values `build()` has already resolved from it (today: the session timezone, H-1a split B).
  A door reads the resolved value instead of re-parsing the map, which is what keeps
  "resolved once, at construction" literally true rather than approximately true.
  **FNP-8 (2026-09-07):** the analyzer hook receives the core-owned guarded rule list and returns
  it unchanged by default. The Spark door uses it to insert one HOF preparation rule before the
  first default type-coercion pass. pins: fnp-8/C-003, C-004
- `catalog_kind.rs` — **ICE-CATALOG-SESSION-1 S7 (2026-09-20):** short-form kind
  resolution, moved out of `catalog_config.rs` (`kind_from_type` with the `hadoop` →
  `Memory` arm, `kind_from_catalog_impl` with the `InMemoryCatalog` → `Memory` arm,
  plus the two alias unit pins).
  pins: ice-catalog-session-1/C-025, C-026
  **PR-B hadoop naming (2026-09-24):** also holds `kind_from_bare_catalog_value` (moved from
  `catalog_config.rs`), `is_hadoop_type`, and `with_type_naming`. `with_type_naming` inserts
  the fork's `metadata-naming` key with `hadoop` only when the key is absent.
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
  **ICE-CATALOG-SESSION-1 S4 (2026-09-20):** `catalog_names` lists every registered
  name (entries + database sources + read-only catalogs, sorted, deduped) for
  `SHOW CATALOGS`.
  pins: ice-catalog-session-1/C-015
  **ICE-CATALOG-SESSION-1 S6 (2026-09-20):** entries carry the `table-default.*` /
  `table-override.*` / `warehouse` side map (`merge_table_props`,
  populated at register and by later runtime sets); `table_creation_properties`
  merges override > user > default at CREATE.
  pins: ice-catalog-session-1/C-024, C-027
  **ICE-CATALOG-SESSION-1 R6 (2026-09-20):** the registry carries the
  session-defaults box (`current_defaults` / `set_defaults`, `Arc`-shared across the
  per-statement clones, seeded `spark_catalog` / `default`): the Spark door's `USE` /
  `SHOW` / short-name completion reads it while planner `default_catalog` /
  `default_schema` stay at the DataFusion builtins.
  **MAINT-POLICY-1 step 2 (2026-09-10):** the registry also carries the stamped
  `[<profile>.maintenance]` policy (`set_maintenance_policy` / `maintenance_policy`, profile
  name plus optional typed policy), the per-execute channel the `run_maintenance` dry run
  reads so it never touches the process environment at query time.
  **MAINT-POLICY-1 step 3 (2026-09-10):** the builder stamps that channel from the loaded
  `repark.toml` at `build()` (active profile name plus the resolved policy, `None` when the
  profile carries no table); a build with no config file leaves the registry unstamped, so
  existing sessions behave exactly as before.
  **CFG-2 step 1 (2026-09-13):** the registry also carries `database_sources` — the
  auto-registered `Arc<SourceSpec>`s keyed by source name (Arc'd so the per-query
  `catalogs_snapshot()` clone stays keys-plus-Arcs, S2-21 P2-1) — `is_registered`, the
  catalog-or-source claim check `register_memory_catalog` /
  `register_iceberg_catalog` consult for the duplicate-name refusal, and
  `database_source(name)`, the `pub(crate)` lookup `refuse_source_ddl` uses to rebuild
  the D-1 refusal for a DDL plan naming a source.
  pins: cfg-2/C-012
  **ICE-VIEWS-1 (2026-09-20):** view lookup beside table lookup (`is_view`),
  the installed-wrapper directory (`view_wrapper_for` / `note_view_wrapper`)
  and the nesting counter behind `view_expansion_guard`.
  **R2 (2026-09-21):** `is_view` returns a `Result` — genuine catalog errors
  propagate (fail-closed); `FeatureUnsupported`/`NamespaceNotFound` stay false.
  pins: ice-views-1/C-006, C-007, C-012
- `named_sources.rs` (+ [named_sources/](named_sources/map.md)) — **CFG-2 step 1
  (2026-09-13):** named database sources. `register_configured_sources()` (called wherever
  `register_configured_catalogs` runs) installs one `RefusingSourceCatalogProvider` per
  auto-registered `SourceSpec` — a `CatalogProvider`/`SchemaProvider` whose every table
  lookup answers the D-1 `NotImplemented` message (source key path, kind, "its connector
  arrives with roadmap 1.10 (Postgres, SQL Server, Trino)") — records the spec on the
  registry, and opens no connection. `sources()` returns one `SourceRow` per declared
  source (name, kind spelling, key path, `auto_register`, props redacted through
  `redact_value`); `source(name)` returns a `NamedSource` handle whose `ping()` answers
  the same refusal and whose unknown name refuses listing the declared sources.
  `auto_register = false` specs list but never register, so SQL under the name answers
  the engine's ordinary not-found. The schema provider also overrides
  `register_table`/`deregister_table` with the same refusal, and `refuse_source_ddl`
  (wired into `PreExecute::guard`, both doors) refuses DDL naming a registered source —
  the only seam that can carry D-1 through DROP, since `SessionContext::drop_table`
  discards provider errors, and the only thing stopping SQL `CREATE CATALOG` from
  silently replacing the source's provider.
  pins: cfg-2/C-001, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-011
- `time_travel/incremental.rs` (+ `time_travel/incremental/tests.rs`) — **ICE-CHANGELOG-1
  (2026-09-20):** the Iceberg
  incremental-read door. `IncrementalWindow::from_options` parses the four reader-option
  strings the facade forwards verbatim (`start-snapshot-id`, `end-snapshot-id`,
  `start-timestamp`, `end-timestamp`, case-insensitive); `append_boundaries` ports Java
  `SparkReadConf.incrementalAppendScanBoundaries` — a timestamp bound on a plain table load
  refuses ``Only changelog scans support `start-timestamp` and `end-timestamp`…`` and an end
  without a start refuses ``Cannot set only `end-snapshot-id` for incremental scans…``.
  `read_incremental` refuses a time-travel pin beside a window (`Cannot use time travel in
  incremental scan`, Java `SparkScanBuilder`; a legacy `snapshot-id` keeps Spark's
  "no longer supported" message, raised after the boundary checks as Java orders them) and
  hands `TimeTravelSpec::Incremental { from, to }` to `time_travel::read_table_at`.
  **Phase 4:** `TimeTravelSpec::Changelog(IncrementalWindow)` carries the raw window for a
  `t.changes` read, resolved against the table's metadata at provider-build time because Java's
  timestamp rule needs the ancestry (`oldestAncestorAfter`), and `read_incremental` routes an
  identifier whose last part is `changes` onto it.
  pins: ice-changelog-1/C-001, C-002, C-004, C-005, C-006, C-009, C-010
- `lineage_columns.rs` — **V3-4:** `prepare_lineage_sql` rewrites **single-table** queries
  that name `_row_id` / `_last_updated_sequence_number` onto a v3
  `LineageColumnsTableProvider` temp view (qualified/aliased FROM, unquoted case-fold,
  schema-order `*` expand). JOIN / CTE / subquery / time-travel naming a lineage column
  refuse `[V3-ROWID-2]`. v1/v2 stay unresolved (`No field named _row_id`). Both SQL doors
  call it.
  pins: v3-4-serve-lineage-columns/C-002, C-003, C-011, C-012, C-013, C-014, C-015, C-016
- `metadata_columns.rs` — **ICE-METADATA-COLS-1 (2026-09-20):** `prepare_metadata_column_sql`
  rewrites queries that name `_file` / `_pos` / `_spec_id` / `_partition` / `_deleted` onto a `MetadataColumnsTableProvider` temp
  view (qualified/aliased FROM, unquoted case-fold, schema-order `*` expand serves user
  columns only). Only the
  Spark door calls it; the ANSI door does not serve metadata columns in this unit.
  **WO-R2 (2026-09-22):** both refusal strings advertise the served three; a served
  `_spec_id` beside an unserved name refuses naming the unserved one.
  **WO-R3 (2026-09-22):** `_partition` joins the served set as a NULLABLE union struct;
  only `_deleted` refuses `[ICE-MC-1]`, advertising the served four.
  **IPI-20 (2026-09-23):** an unquoted `input_file_name` word followed by `(`
  is a second trigger into the same path; inside a SELECT whose own single
  relation IS the rewritten Iceberg table — `sole_input_file_name_relation`
  accepts a `TableFactor::Table` only when its written name is a rewrite's
  original, so a relation merely aliased like the table (a CTE or derived
  relation carrying the table's name as its alias) does not qualify — a
  zero-argument `input_file_name()` (no FILTER/OVER)
  rewrites to `<alias>._file` in the projection and WHERE, the bare projection
  item gains the alias `` `input_file_name()` ``, calls inside a listed
  aggregate's arguments stay untouched, the per-SELECT call visitor leaves a
  query nested inside a projection or WHERE expression alone while every
  nested SELECT is visited on its own and rewritten against its own single
  relation, and every other shape keeps the unresolved-routine answer —
  including a non-query statement, which returns `Ok(None)` rather than the
  `[ICE-MC-1]` refusal when only this trigger fired. The wildcard path's
  `sole_rewritten_relation` keeps main's alias fallback untouched — the two
  helpers differ on purpose. **IPI-20-R4 (2026-09-23):** collection itself is
  CTE-aware — every `With` clause in the statement (nested ones included)
  contributes its alias names, and a one-part `TableFactor::Table` matching a
  CTE name on the planner's own fold (unquoted folds case-insensitively,
  quoted stays exact) is never treated as the physical table and falls
  through unrewritten; qualified names are never CTE references and stay
  collected.
  **WO-R3 (2026-09-22):** `_partition` joins the served set as a NULLABLE union struct.
  **mcdel-r1 (2026-09-23):** `_deleted` joins the served set — the projected name
  reaches the pinned fork unchanged, whose include-deleted scan mode marks
  merge-on-read deleted rows `true`; the unserved-token machinery
  (`UNSERVED_METADATA_COLUMN_NAMES`, the unserved scan and its refusal) is deleted,
  and the composed refusal message now names all five served columns. A metadata
  column over a time-travel read keeps the planner's unresolved-column error — the
  pinned static provider does not advertise metadata columns (RePark refuses every
  metadata column there; KNOWN DIVERGENCE for `_file` and `_deleted`, which Spark was
  measured serving — the other names are unmeasured on Spark).
  **mcdel-r4 (2026-09-23):** before registering a table's temp view,
  `prepare_metadata_column_sql` refuses when the statement names a served metadata
  column (its canonical tokens, `referenced_metadata_names`) that the table's user
  schema also carries: `Table column names conflict with names reserved for Iceberg
  metadata columns: [<names>]. Please, use ALTER TABLE statements to rename the
  conflicting table columns.` (Spark's text, as `DataFusionError::Plan`) instead of
  leaking `__repark_mc_N` in a duplicate-field schema error. A statement that does
  not name the colliding column is still served.
  **mcdel-r5 (2026-09-23):** measured on both engines — the bracket lists the
  colliding names the statement references in the table's declaration order,
  whatever the query order (`(id, _pos, _file)` → `[_pos, _file]`), and a query naming
  no colliding column (another metadata column, a plain `SELECT id`) answers on Spark
  too. Still divergent (`R-MC-RESERVED-NAME-SCAN`): Spark refuses `SELECT *` and the
  copy-on-write `DELETE` and prints a different `WHERE`-shape error. Because the check
  keys on the statement's tokens rather than the relation, a join naming `p._deleted`
  beside a table `uc` whose user schema has `_deleted` refuses, where Spark answers
  (residue candidate `R-MC-RESERVED-NAME-JOIN`).
  **mcdel-r6 (2026-09-23):** the collision check left this file. It now runs in
  `MetadataColumnsTableProvider::scan` (`repark-iceberg`) on the columns the scan
  reads, so the token check described in the mcdel-r4/r5 notes is gone.
  `referenced_metadata_names` only routes a statement onto the rewrite; a reserved
  word used as an alias, CTE name or table alias answers, and the join above now
  answers Spark's rows.
  **mcdel-r8 (2026-09-24):** `expand_wildcards` resolves a qualified wildcard through
  `qualified_rewrite` — first this SELECT's aliased FROM relations (joins included),
  qualifying by the alias as written, then the rewrite's own alias — and qualifies a
  bare `*` by `sole_relation_alias`. So `x.*` under a FROM alias expands to the user
  columns instead of every provider field, which had returned deleted rows on
  merge-on-read tables. The unit leg
  `a_qualified_wildcard_under_a_from_alias_expands_to_user_columns` pins the rewritten
  SQL.
  pins: ice-metadata-cols-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-015, C-016, C-017,
  C-018, C-019, C-020, C-021, C-022
  pins: ipi-20-input-file-name-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-009, C-010, C-011,
  C-012, C-013
  pins: ice-metadata-cols-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-015, C-016, C-017, C-018
  pins: ice-metadata-cols-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
  pins: u10-mc-deleted-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009, C-010, C-011, C-012, C-013, C-014, C-015, C-016, C-017, C-018
  **ICE-VIEWS-1 (2026-09-20):** `prepare_lineage_sql` takes `&(dyn Dialect + Sync)`
  so the view read path's `Send` future can route through it; no behavior change.
- `time_travel.rs` (+ `time_travel/tests.rs`) — `TimeTravelSpec` + `TimeTravelOpts` (moved
  here from `session.rs` in CFG-1 step 3, next to the spec its `into_spec` builds) + parsers
  (`parse_version_value`, `parse_timestamp_to_ms`), snapshot resolution, `read_table_at`
  (snapshot-pinned static provider via `iceberg-datafusion`, or — **ICE-CHANGELOG-1
  (2026-09-20)** — `repark_iceberg`'s `IncrementalAppendTableProvider` for the
  `TimeTravelSpec::Incremental { from, to }` variant, on one table load instead of two), and
  **`next_temp_view_name` — the ONE minter of the `__repark_tt_` namespace** (H-1b fix pass,
  2026-08-11). SQL-text rewriting
  remains deferred with the phase-2 router.
  **ICE-TT-RESOLVE-1 (2026-09-19):** the module split at the 1000-line ceiling into
  `time_travel/sql_text.rs` (string/zone parsing, token extraction), `time_travel/sql_ast.rs`
  (column-refusal check), `time_travel/sql_eval.rs` (constant-expression
  evaluation); the root keeps the spec types plus `resolve_reader_spec` (the ONE resolver both
  SQL doors and the reader options share — `versionAsOf`/`timestampAsOf` raw strings, integer
  means seconds).
  **IPI-23-MT-READER-1 (2026-09-22):** `read_table_at` routes a four-part metadata-table
  name to `time_travel/metadata_at.rs` before the three-part loader, and the un-pinned
  `read_iceberg_table` arm quotes the same shape so it rides the router path; `session.rs`
  holds its 1000-line ceiling.
  pins: ipi-23-mt-reader-1/C-001, C-002, C-004, C-005
  pins: ice-tt-resolve-1/C-010
  **ICE-TT-RESOLVE-1 round 3 (2026-09-19):** every string resolves through the
  engine `CAST(... AS TIMESTAMP)` in the session zone; the hand parser and the AST leaf
  rewrite are gone and `resolve_reader_spec` is async over the session context.
  pins: ice-tt-resolve-1/C-002
  **ICE-TT-RESOLVE-1 round 3 (2026-09-19):** `RefSelector` names the fourth-segment
  ref (`Branch` / `Tag` / `None`); a built-in beside a selector refuses with Spark's
  text before resolving. pins: ice-tt-resolve-1/C-002
  **ICE-TT-RESOLVE-1 round 2 (2026-09-19):** the moved names resolve only behind the
  `time_travel` module; the stale root re-exports are gone. pins: ice-tt-resolve-1/C-010
  **ICE-CHANGELOG-1 round 1 (2026-09-20):** `read_table_at`'s Incremental arm re-raises a
  `DataInvalid`-kind fork refusal through `illegal_argument_error`, so a bad window raises
  `IllegalArgumentException` as Spark's does; the global `DataInvalid` mapping is untouched.
  pins: ice-changelog-1/C-007
  **ICE-TT-RESOLVE-1 round 2 close (2026-09-19):** the root block narrows to the six
  externally used names; `lib.rs` sits at the 150 default ceiling. pins: ice-tt-resolve-1/C-012
  **ICE-TT-RESOLVE-1 round 2 (2026-09-19):** `EngineContext::new` is 3-arg again (zone
  defaults); `new_with_time_zone` carries an explicit zone. pins: ice-tt-resolve-1/C-003
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
  **xo55-bs R1 (2026-09-22):** `read_table_at`'s pinned arm builds a `VersionRef` provider
  through the fork's `try_new_from_table_ref` (current schema for a branch, snapshot schema
  for a tag) after `resolve_snapshot_id`, so the reader-options branch read tracks the live
  schema and unknown refs keep the pinned refusal.
  pins: ipi-07-branch-read-schema-1/C-002, C-006
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
  **SET-ANSI-RUNTIME-1 (2026-09-15):** `parse_runtime_session_zone_value` (the runtime
  gate: IANA ids plus Java `ZoneOffset` / `GMT|UTC|UT`-prefixed forms inside ±18:00 —
  a sign-led value that fails the offset arm never falls through to Arrow, which accepts
  past ±18:00 that Java refuses; refusals carry Spark's `INVALID_CONF_VALUE.TIME_ZONE` as
  `Error::IllegalArgument`). **R-17c-4:** `canonical_session_zone_id` (the companion every
  value consumer parses; the snapshot keeps the raw echo) and the seconds-form refusal;
  `text_scan.rs` reads the canonical id. pins: set-ansi-runtime-1/C-002
  **R-17c-6 (batch-17 oracle):** `ZoneId` matching is case-sensitive, padded input refuses
  with the raw text echoed, zero-seconds forms canonicalise to `±HH:MM`, nonzero-seconds
  stays refused (divergence SET-ANSI-RUNTIME-4); the shared table lives beside the parser.
  pins: set-ansi-runtime-1/C-002
  and `ReparkSession::set_runtime_zone` (the live zone behind `RwLock<Arc<_>>`, shared by
  clones; `session_time_zone` now returns the snapshot `Arc`). Pedantic-clean (nested
  or-patterns, method-ref digit checks).
  pins: set-ansi-runtime-1/C-002
- `temp_view.rs` (+ `temp_view/tests.rs`) — **the temp-view NAME choke point (round 6, R6-1):**
  `TempViewHome` (the build-time `catalog.schema` a session's temp views live in, snapshotted
  once), `build_temp_view_home` (the one `build()`-time capture, moved here from `session.rs`
  in CFG-2 step 1 to hold `build()` under the function-length lint) + `temp_view_ref`,
  which every temp-view entry point resolves names through. A QUALIFIED
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
  census)
  and `tests/conf_unread.rs` (CONF-UNREAD-1 step 1: the four accepted-but-unread
  keys — coalesce refusal plus three wiring pins).
  **DF-SUBQUERY-1 (2026-09-16):** `session/df_guards/subquery.rs` adds the subquery
  family — scoped expression/plan resolution (`resolve_bound_expr` /
  `resolve_scoped_expr` / `resolve_subquery_plan`, innermost-first per Spark
  classic's recorded quirk), the `__repark_single_row` guard UDAF (multi-row
  scalar → 21000 at execution), and three optimizer rules
  (`repark_projection_exists`, `repark_scalar_subquery_guard`,
  `repark_lateral_projection_hoist`) spliced ahead of `scalar_subquery_to_join`
  and `decorrelate_lateral_join`; `session/df_guards.rs` installs them and
  `tests/subquery.rs` pins them at plan level. Round 3 added the
  `__repark_any_row` sibling UDAF (a correlated `LIMIT` is stripped before
  wrapping) and made the hoist preserve a `SubqueryAlias` qualifier on lifted
  outputs. `session.rs` re-exports the resolvers for the Python bindings.
  pins: df-subquery-1/C-001, C-002, C-003, C-004, C-008, C-009

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
- **FNP-AGG-1 slice (d) (2026-09-22):** `error_map.rs` adds `grouping_refusal_message`, the
  grouping-refusal peel. Only the `Analysis` arm of `engine_err` calls it, and
  `unwrap_or(display)` passes the full display through when it declines. The peel reads the
  display's final segment (after the last `caused by` separator, with a leading
  `Error during planning: ` stripped). When that segment starts with
  `[GROUPING_ID_COLUMN_MISMATCH]` or `[UNSUPPORTED_GROUPING_EXPRESSION]` and carries
  `SQLSTATE: ` plus 5 alphanumeric characters, the message keeps only the text up to and
  including those 5 characters, so the wrapper context peels off and the bare refusal remains.
  Every other message passes through unchanged: a foreign bracketed head keeps the full
  wrapped display, and a grouping head without a `SQLSTATE: ` segment keeps the full display.
  pins: `grouping_rule_wrap_peels_to_the_bare_refusal`,
  `grouping_unsupported_peels_without_the_rule_wrap`,
  `foreign_bracketed_wrap_keeps_the_full_display`,
  `grouping_head_without_sqlstate_keeps_the_full_display`.
