# map — repark-iceberg/src/catalog

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002). Clippy doc_markdown backticks added.

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Iceberg catalog wiring for DataFusion (v1 `repark-catalog`, ported byte-faithful). Build AWS
Glue (primary) and S3 Tables (secondary) catalogs and register any `iceberg::Catalog` as a
DataFusion `CatalogProvider`, so `glue_catalog.namespace.table` resolves with zero translation.
Source comments retain only API and safety contracts; implementation narration is omitted.

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
  `FileIO`; prop helpers + the `iceberg_to_datafusion` error map (`External(iceberg::Error)`
  so `classify_datafusion_error` still peels Iceberg). Hadoop `vN.metadata.json` writes now
  bump to `v(N+1).metadata.json` (registry `V3-ADOPT-1` FIXED, RP-3 / fork #235). S3 Tables
  `register_table` still refuses naming fork row R126 (pins: rp-3-fork-repin/C-008).
  Catalog-edge spans record **prop key names only**, never values.
- `caches.rs` — **PERF-ICE-CATALOG-IO-1 (2026-09-05):** the session-scoped Iceberg cache handles
  and their knobs. `IcebergCacheSettings::from_config_map` reads
  `repark.iceberg.metadataCache` (default **true**) and `repark.iceberg.metadataCacheEntries`
  (default 512), both with an underscore alias; a bad value fails loud naming BOTH the key the
  user set and the canonical spelling it aliases.
  `CatalogCaches` owns the fork's opt-in `TableMetadataCache` (`catalog/table_metadata_cache.rs`)
  keyed by **metadata-file location string**. What makes that safe across a commit is
  **evict-on-commit plus evict-on-drop**: `MemoryCatalog` drops the pointer it replaced and seeds
  the new one, and `drop_table` evicts. It is NOT the uuid in the file name — round 2 measured a
  Hadoop pointer adopted through `CALL register_table` committing deterministic
  `v(N+1).metadata.json` with no uuid, and the safety property holds there too
  (`a_hadoop_pointer_adopted_by_register_table_stays_correct_across_commits`).
  The cache never decides WHICH location is current — the catalog pointer read still happens on
  every `load_table`; the cache only skips the body GET and the re-parse for a location already
  parsed. `CatalogCaches::disabled()` is the pre-unit path the before/after measurement runs in
  the same process.

  **The bound's scope is the statement door, not the load.** The fork's cache is an unbounded
  `HashMap`, so `trim()` clears it once the retained-location count passes the knob, and the
  session calls `trim` at the statement door (`session.rs::sql_with`). That bounds what a session
  ACCUMULATES across commits — measured: eight CREATEs at `entries=1` leave 2 retained and the
  next door clears to 0. It does NOT bound retention *within* one statement: an 8-way `UNION ALL`
  at `entries=1` retains 8 (measured; pinned by
  `one_statement_over_many_tables_retains_one_entry_each_until_the_next_door` and its Python
  twin). That residue is one entry per distinct table the statement names — live working set the
  planner needs, bounded by the statement's table count and cleared at the next door — not the
  accumulation the knob exists to stop. Bounding within a statement needs a hook on cache INSERT,
  which is fork-side; the RePark-side alternative is a `SchemaProvider` decorator carrying a
  permanent forwarding-audit duty, which is not worth a bound on working set. Registry row
  `PERF-CATALOG-CACHE-BOUND-1` / fork ask `F-CATIO-BOUND` carries the real fix: a bounded LRU
  inside the fork's cache bounds within a statement by construction.

  **`memory_catalog(warehouse)` keeps its v1 signature but is no longer cache-free** — it now
  builds a private, always-on `CatalogCaches` per call, which nothing trims because no session
  owns it. `memory_catalog_cached(warehouse, caches)` is the session's entry; a caller wanting
  the pre-unit behaviour passes `CatalogCaches::disabled()`.

  **PERF-ICE-CATALOG-IO-2 (2026-09-05, landed default-OFF per the round-2 ruling):**
  a third knob, `repark.iceberg.manifestCacheBytes` (default `0` = off; set bytes to opt
  A value that is not an integer in `[0, 2^64)` is refused naming both spellings (a u64 overflow is refused as such, not as a negative).
  in — `33554432` is the measured value) with an underscore alias, sizes the fork's shared
  manifest `ObjectCache` for the memory catalog; a bad value fails loud naming both
  spellings, exactly like the entries knob. `builders.rs` passes nonzero bytes to
  `MemoryCatalogBuilder::with_shared_object_cache_bytes`, which builds ONE cache per
  catalog that every table the catalog materializes carries. The unit HALTED mid-flight on
  a fork-side cache-key defect and landed with the default OFF so main serves no wrong
  answer: the manifest FILE is immutable at its path, but the PARSED object embeds the
  caller's list-entry lineage range, which the fork's `(path, schema)` key does not carry
  — so a v2-context parse poisons later v3 reads of the same path and upgrade-boundary
  tables serve `_row_id` NULL (4 facade tests red with the knob on, green with it off).
  Filed `PERF-CATALOG-LINEAGE-CACHE-1` / `F-CATIO-KEY`; the default-ON flip waits for the
  fork key fix plus the four red tests passing knob-on, and the two lineage detector pins
  in `test_perf_ice_catalog_io_1.py` hold the defect's shape meanwhile. The 10×
  measurement (115.81 → 10.95 ms) was taken with the knob set explicitly to 32 MiB.
  Carries, not always reads: the fork's scan path (`plan_files`) consults the
  cache, but the fork's transaction, maintenance and inspect paths load manifests straight
  from `FileIO` (0 cached reads against 166 direct loads in `transaction/` at this pin) — so
  a repeated SELECT opens nothing while a DML statement keeps its commit-side opens and saves
  only its read-side repeats. Measured: DELETE 4/8 → 3/6 lists/manifests, UPDATE 5/15 → 4/12,
  MERGE 4/8 → 4/8, INSERT 2/1 → 2/1. That is a filed fork ask
  (`PERF-CATALOG-COMMIT-CACHE-1` / `F-CATIO-COMMIT`), not a wiring defect: a bypassing path
  re-reads, it never serves stale. `memory_catalog(warehouse)` keeps its signature and
  builds NO shared manifest cache (the default is off); the always-on, never-trimmed shape
  IO-1 recorded now covers the metadata cache only. A caller that wants the shared manifest
  cache passes an explicit byte budget through `memory_catalog_cached`. The funnel was read
  at fork pin `79119643`: the
  memory catalog assembles a `Table` in exactly three places (`load_table_from_location`,
  `create_table`, `register_table`), and all three go through its `table_builder()`, which
  injects the shared cache; the only direct `Table::builder()` in the memory catalog is a
  `#[cfg(test)]` fixture. Named non-members, all correct-but-uncached (a private cache
  re-reads, it never serves stale): catalog-detached `StaticTable::from_metadata*`, staged
  create/replace transactions (RePark uses them write-side for CTAS/replace), the
  `delete_reachable_files` maintenance walk, and every non-memory catalog (Glue, S3 Tables,
  REST, HMS, SQL build per-table caches — their builders have no
  `with_shared_object_cache_bytes` at this pin, so `glue_catalog` / `s3tables_catalog`
  are unchanged again). RePark itself builds no `Table` directly: every read goes
  through `catalog.load_table`, and time-travel wraps the loaded table. **Repin duty:**
  re-read the funnel — a new assembly path that bypasses `table_builder()` silently
  un-shares the cache, and the delete-manifest pins in the Spark tests map are the detector;
  re-check the key — a fork key that carries the assignment input, with the four
  upgrade-lineage tests green knob-on, is what the default-ON flip unit waits for.
  pins: perf-ice-catalog-io-2/C-001, C-002, C-005, C-007

  **PERF-ICE-CATALOG-IO-3 (2026-09-05, red-first pins):** the default-enables pin
  (`the_default_sizes_the_shared_cache` in `caches.rs`) lands before the flip and reds
  while the default is `0`. The flip of `DEFAULT_MANIFEST_CACHE_BYTES` to 32 MiB, the size
  argument, and the per-session ceiling follow in this unit.
  pins: perf-ice-catalog-io-3/C-001

  **Glue and S3 Tables are NOT wired.** `glue_catalog` / `s3tables_catalog` are unchanged and take
  no `CatalogCaches`, because the fork's `GlueCatalogBuilder` / `S3TablesCatalogBuilder` have no
  `with_table_metadata_cache` at pin `189a73ed`. Every number in this unit is the memory catalog;
  the AWS call shape is **unchanged today**. Registry row `PERF-CATALOG-AWS-CACHE-1` / fork ask
  `F-CATIO-AWS`.

  **Creation is not cacheable.** `CREATE TABLE` and CTAS read back the metadata document they just
  wrote (the catalog proves reachability before claiming the pointer), so their census is 1
  metadata read with the cache ON and OFF — measured on both knob settings.

  Every reason for this module is written here rather than in the code, and the maps this unit
  touched move in the commits that touched their directories.
  pins: perf-ice-catalog-io-1/C-002, C-003, C-004, C-007
- `location.rs` — namespace-location key identity (`NAMESPACE_LOCATION_PROPERTY` `"location"` /
  `NAMESPACE_LOCATION_URI_PROPERTY` `"location_uri"`; `resolve_namespace_location` read
  precedence + `mirror_namespace_location_keys` unidirectional non-clobbering dual-write) and
  scheme-based FileIO selection (`storage_factory_for_location` / `file_io_for_location`:
  `s3://`/`s3a://` → the fork's OpenDAL S3 factory; `file://`/bare **absolute** path → LocalFs;
  anything else — unknown scheme, single-slash typo `s3:/…`, relative/empty path — fails loud,
  never a silent LocalFs).
- `lineage_columns.rs` — **V3-COV (2026-09-03):** the scan resolves its field→column projection
  once per batch schema (cached on `Arc::ptr_eq`) instead of a name scan per field per batch, and
  `conform_batch` strict-casts a scanned column
  whose Arrow type differs from the declared field's, so a `_row_id` projection after a widening
  `ALTER COLUMN … TYPE` returns rows instead of raising
  `lineage scan could not rebuild batch`; the ordinary read path already promoted, so the same
  table answered one query and failed its sibling. Registry `V3-COV-2` FIXED.
  pins: v3-cov-statement-coverage/C-004
  **V3-4:** `LineageColumnsTableProvider` serves `_row_id` and
  `_last_updated_sequence_number` on format-v3 **current-snapshot** reads (stored value
  else `first_row_id +` position / file sequence). Simple `col = lit` filters pass through
  to `table.scan().with_filter` (`TableProviderFilterPushDown::Inexact` residual still
  applies). Time-travel plus lineage is `V3-ROWID-2` at the SQL rewrite; snapshot-pinned
  scan is the follow-up. `SELECT *` stays user columns because the SQL doors only register
  this provider when a query names the columns.
  pins: v3-4-serve-lineage-columns/C-002, C-017, C-019, C-020 V3-COV pins in this file: a scan column left behind by a widening ALTER promotes instead of failing (V3-COV-2); the projection is resolved once per scan schema and reused for every batch; a scan that lost a lineage column names it rather than rebuilding a short batch.
- `metadata_projection.rs` — **retired at RP-5** (fork F-8 / R169 / R170). The fork honors
  metadata-table `projection` and lists catalog entries only. Pins remain in
  `crates/repark-spark/src/tests/metadata_tables.rs`.
  pins: rp-5-fork-repin/C-003
- `provider.rs` — `ReparkCatalogProvider` (mutable namespace→schema map) +
  `invalidate_catalog_namespaces` / `drop_catalog_namespace_from_provider` /
  `rebuild_catalog_provider`. Product DDL rebuilds only the touched namespace; empty invalidate
  is a no-op; DROP NAMESPACE is a zero-list map remove; invalidate/drop fail loud when the DF
  catalog name is not registered. Every schema snapshot/refresh **eager-lists** the fork's lazy name directory
  (`freeze_fork_name_directory`, pin `5e7b2e4` — `IcebergSchemaProvider::try_new` no longer
  `list_tables`; first access would otherwise freeze *after* an OOB create and drop T6
  residual). Hosts `NamespaceScopedCatalog` (G17 closed): 14 required
  + 13 of 16 defaulted `Catalog` methods are explicit forwards; 3 composition defaults are
  stated omissions at pin `5e7b2e4` (see crate-root map "Known limitations").
- [tests/](tests/map.md) — G17 wrapper pins and the file-backed unit battery (all AWS-free): CTAS reality, AWS-builder
  validation + namespace construction, live-list staleness pins, O(1) invalidation pins,
  scheme-selection + key-identity partitions, span secret-hygiene pins, the fork-patch
  proof test (`fork_patch_in_effect_deletefilter_is_public` — names a fork-only public symbol,
  cannot compile against crates.io iceberg 0.9.1), and **RP-1 / C-011** T6 residual pins
  against the fork's lazy name-directory (`full_rebuild_lists_every_namespace` plus the
  three OOB-invisibility tests).

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
| Turn the metadata-location cache off, or change its retained-entry bound | `repark.iceberg.metadataCache` / `repark.iceberg.metadataCacheEntries` (`caches.rs`) |
| Turn the shared manifest cache on, or resize its byte budget | `repark.iceberg.manifestCacheBytes` (default `0` = off; `33554432` measured) (`caches.rs`) |
| Pick a FileIO backend by location scheme | `file_io_for_location` / `storage_factory_for_location` in `location.rs` |
| Read / write a namespace's warehouse location | `resolve_namespace_location` / `mirror_namespace_location_keys` in `location.rs` |
| Serve `_row_id` / `_last_updated_sequence_number` on a v3 read | `lineage_columns.rs` (`LineageColumnsTableProvider`); SQL doors call `repark_core::prepare_lineage_sql` |
| Change what `SHOW TABLES` / `information_schema` enumerates | fork `IcebergSchemaProvider::table_names` at pin `00cdde0` (F-8); engine shim retired RP-5 |
| Change credential handling | not here — AWS SDK default chain *inside the fork* |

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| CTAS errors "does not support tables with data" | expected — decompose into `CREATE (cols)` + `INSERT INTO` (SQL layer) |
| Table created after register not found in SQL | provider name directory is snapshotted; product DDL invalidates the touched namespace; OOB DDL needs refresh / full rebuild. Facade listing is live (`list_table_names`) |
| Builder returns "requires a non-empty `…` property" | Glue needs `warehouse`, S3 Tables needs `table_bucket_arn` |
| A `load_table` reads `metadata.json` when you expected a cache hit | the location moved (any commit does that) or the entry bound tripped `CatalogCaches::trim`; `ReparkSession::iceberg_metadata_cache_stats` reports hits / misses / body fetches |
| Constructing Glue/S3 Tables in a test without AWS | pin `region_name` so SDK-config load skips the IMDS region probe; creds resolve lazily on first request |
| S3 Tables `301`/region errors | pass explicit `region_name`; ARN region must match SDK region |
| Hang on Glue/S3 catalog with no logs | enable `RUST_LOG=repark_iceberg=info`; expect `catalog.*` span close timings. Span fields are key names only |
| `SHOW TABLES` does not list `t$snapshots` | expected since ADR-0006 — hidden from enumeration on purpose, still queryable as `ns."t$snapshots"` (or the Spark door's `ns.t.snapshots`) |
| A `$`-metadata name reappears in `SHOW TABLES` after a fork repin | the fork changed the synthesized spelling; the filter matches `<base>$<MetadataTableType::as_str()>` exactly. See `crates/repark-iceberg/map.md` "Known limitations" (the repin duty) |
| `SHOW TABLES` lists `a$b$snapshots` (a `$` in the BASE table's name) | closed at RP-1: last-`$` + vocabulary hides it; `a$b` still lists. Inherent residue is a base literally named `foo$files`. Pin: `the_filter_keeps_names_the_fork_did_not_synthesize` |
| `VERSION AS OF` plus `_row_id` is a Schema error | Intended `V3-ROWID-2` at the SQL rewrite (`repark_core::prepare_lineage_sql`). Snapshot-pinned lineage scan is the follow-up, not `try_new_with_snapshot`. |

First checks: `cargo test -p repark-iceberg catalog::`. Escalate to: [../../map.md#debug](../../map.md).

- **EC-9 scrub (2026-08-08, phase-3 PR-5):** pre-existing private fixture/doc literals
  (a team/bucket name fragment) replaced with `example-team` equivalents — outcome-neutral
  (fixtures and their oracles changed together); enumerated in docs/history/port-v2/p3e-facade-ledger.md.
