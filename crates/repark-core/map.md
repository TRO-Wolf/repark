# map — repark-core

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

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
- `src/silver.rs` + [src/silver/](src/silver/map.md) — typed `SilverPlan` (SILVER-S1):
  strict TOML parse, closed enums, canonical identity, deterministic explain. Public from
  this crate, not wired to Python. Unstable until SIL-1..SIL-10.
  pins: silver-s1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
- `src/config_file.rs` + [src/config_file/](src/config_file/map.md) — the `repark.toml` loader
  (CFG-1, seed 2026-09-09): `ConfigFile`, the ruled `load()` entry, and the `toml` reader. The
  crate's dependency list gains `serde` (derive) and `toml` in the same commit — the workspace's
  first consumer of both. Empty stage files until steps 1–2; the directory map says which stage
  each owns.
- `src/session.rs` — `ReparkSession` + `ReparkSessionBuilder`: knob surface
  (`config`/`configs`, memory limit with the 1 MiB floor / RAM-relative `FairSpillPool`
  default `clamp(0.6 × cgroup-or-MemTotal, 1 MiB, 8 GiB)` — runtime SET of
  `datafusion.runtime.memory_limit` swaps a new FairSpillPool, see
  `src/session/spill.rs`;
  `batch_size`, `target_partitions`), sync `build()` + async
  `register_configured_catalogs()` finalize (two-phase lifecycle), catalog ops
  (`register_iceberg_catalog`, `register_memory_catalog`, `create_namespace` with the
  location/location_uri mirror and the G-6 Q1 contradictory-location refuse,
  `table_exists`, the listing families,
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
- `src/read_options.rs` — CSV/JSON Spark option-map helpers; `read_csv_path` (nullValue
  Utf8 scan; `utf8_columns` timestamp re-read). pins: nullability-2/C-006
  pins: csv-infer-perf-1/C-002, C-006
- `src/text_scan.rs` — text read: `TableProvider` over local files, plain dirs
  (hidden skipped, `key=value` dirs descended iteratively, partitioned leaves
  win over root files), and Hadoop globs (bare globs discover nothing, `basePath`
  discovers beneath it); universal / custom separators, wholetext, lossy UTF-8,
  ≤8 file-group partitions, limit threaded into the scanner.
  Battery beside it in [`src/text_scan/`](src/text_scan/map.md) (round 5,
  ruling X-1); per-file values ride one `Arc` into every scan partition
  (round 5, ruling X-5).
  pins: io-text-1/C-001, T-1, T-3, T-5, T-6, T-8, W-2, W-4, X-1, X-5
- `src/text_schema.rs` — **IO-TEXT-1 round 4 (2026-09-15):** the user-schema
  overlay beside the scan (split from `text_scan.rs` at the 1000-line ceiling):
  the user schema is the data schema with discovered columns appended after it,
  named partition types override inference, bad casts refuse
  `INVALID_PARTITION_VALUE` `42846`. Round 5 (ruling X-1): a named column
  parses the raw unescaped directory text under the user type — string keeps
  the raw, date parses `yyyy-MM-dd`, timestamp is midnight in the session
  zone, decimal keeps its scale. pins: io-text-1/W-1, X-1
- `src/partition_discovery.rs` — shared hive discovery (IO-ORC-1 reuses it):
  `%XX` unescape, default-marker NULL, int → bigint → double → date ladder,
  per-file raw texts beside the inferred values; name lists that differ at any
  depth refuse `CONFLICTING_PARTITION_COLUMN_NAMES` `KD009` (round 5, rulings
  X-1/X-2/X-3). pins: io-text-1/U-3, W-3, X-1, X-2, X-3
- `src/text_glob.rs` — hand-written Hadoop glob matcher (`*?[]{}`, no `/`
  crossing, char-aware) with no new dependency; unmatched globs answer
  `PATH_NOT_FOUND` from the scan. pins: io-text-1/T-5
- `src/text_io.rs` — streaming text part-file writer (`execute_stream` batches
  to sequential `part-*.txt`, empty frame keeps one empty part); offender-first
  schema check, Spark-verbatim 1290 count text, empty-`lineSep` refusal.
  pins: io-text-1/C-002, T-2, T-4, T-7
- `src/text_partition.rs` — one-scan `partitionBy` fan-out with Hive escaping;
  past the 256-writer cap evicted keys append to the same `part-00000.txt`
  (round 4, ruling V-1); past 256 distinct keys the tail diverts to the sorted
  single-writer fallback (round 5, ruling X-4). pins: io-text-1/U-1, U-2, V-1, X-4
- `src/text_partition_fallback.rs` — **IO-TEXT-1 round 5 (2026-09-15):**
  Spark's high-cardinality fallback beside the fan-out: the remaining stream
  sorts by the partition columns through DataFusion `SortExec` (spill-capable)
  and appends key by key with one open writer, reusing the fan-out's
  leaf-writer and body row. pins: io-text-1/X-4
- `src/error_map.rs` — DataFusion/iceberg error folds into `repark_common::Error`; public
  `engine_err` (the single `DataFusionError → Error` classifier).
- `src/namespace_create.rs` — G-6 Q1 location-conflict predicate shared by Session
  `create_namespace` and both SQL doors' `IF NOT EXISTS` paths.
- `src/idents.rs` — table-identifier segment parse + path-escape refuse (delegates to
  `repark_iceberg::write::idents::path_escape_kind` — single-source needles).
- `src/object_store_s3.rs` — `s3://` / `s3a://` `read_parquet` support:
  `AwsConfigCredentialProvider` (aws-config default chain → `object_store::CredentialProvider`),
  `build_amazon_s3_store`, `register_bucket_store` (one store under BOTH scheme URLs),
  `parse_s3_bucket` / `is_s3_scheme`.
- `src/dynamic_flatten.rs` — DF1 `dynamic_flatten` plan rewrite (free function +
  `DynamicFlattenOptions`; re-exported at the crate root). List-of-map and
  ListView refuse LOUD; Dictionary-of-List is cast before Unnest; LargeList /
  FixedSizeList explode.
- `src/stack/` — **PERF-UNPIVOT-1:** Spark `stack(n, expr…)` as `UnpivotExec`, linear in
  columns. `apply_stack` is the DataFrame entry; SQL rewrite is the Spark-door
  `StackRewrite`. Step-1 remediation: `interleave` + streaming poll.
  pins: perf-unpivot-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- `src/lib.rs` — the crate-root manifest (module declarations + re-exports; no logic).
- `src/dialect.rs` / `src/extension.rs` — the phase-2 seams: `SqlDialect` (+ `EngineContext`,
  default `DataFusionDialect`) and `SessionExtension` (configure/register hooks,
  `NoopSessionExtension`).
- `src/catalog_state.rs` — the hoisted `CatalogRegistry` + `LocationPolicy` (E-4 temp-root
  resolution at registration).
- `src/lineage_columns.rs` — **V3-4:** rewrite `SELECT _row_id` / `_last_updated_sequence_number`
  onto a `LineageColumnsTableProvider` temp view for format-v3 Iceberg tables (single-table
  only; JOIN/CTE/subquery/time-travel refuse `V3-ROWID-2`); v1/v2 stay unresolved. Both SQL
  doors call `prepare_lineage_sql`.
  pins: v3-4-serve-lineage-columns/C-002, C-003, C-011, C-012, C-013, C-014, C-015, C-016
- `src/time_travel.rs` — the hoisted `TimeTravelSpec` parsers + `read_table_at`
  (snapshot-pinned static provider).
- `src/map.md` — the per-file source inventory (authoritative detail for everything above, plus
  the file-backed test module dirs).

## I want to...

| ...do this | go to |
|---|---|
| Parse or explain a typed silver plan | `src/silver.rs` (`SilverPlan::parse` / `canonical` / `explain`) |
| Add a `ReparkSession` method / config knob | `src/session.rs` |
| Register a catalog / namespace | `register_iceberg_catalog` / `create_namespace` in `src/session.rs` |
| Map a `spark.sql.catalog.*` config block | `src/catalog_config.rs` (`parse_catalog_specs`) |
| Change `s3://` / `s3a://` read routing or the AWS credential bridge | `src/object_store_s3.rs` |
| Tune memory/spill/batch/partition defaults | `src/session.rs` (`FairSpillPool`, `target_partitions`, batch size) |
| Change `dynamic_flatten` / `dynamicFlatten` | `src/dynamic_flatten.rs` |
| Change error classification | `src/error_map.rs` (`engine_err` / `classify_datafusion_error`) |
| Add the distribution backend (later) | [../../ARCHITECTURE.md](../../ARCHITECTURE.md) first — the seam's surface (and its call sites) widens before a new-crate `impl` is the work |
| Plug a statement router / SQL front end | implement `SqlDialect` (`src/dialect.rs`) |
| Install door registrations at build time | implement `SessionExtension` (`src/extension.rs`) |

## Component contract

- **Owns:** `ReparkSession` + builder (the engine API); the `ExecutionBackend` / `SqlDialect` /
  `SessionExtension` seams; catalog & namespace ops; readers (parquet / csv / json / iceberg + time
  travel); temp views; `dynamic_flatten` (the DF1 plan rewrite); `*.sql.catalog.*` config parsing; `s3://` / `s3a://` read routing + the AWS
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
