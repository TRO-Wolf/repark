# map — docs/guide/

## Purpose

**User-facing** documentation: how to *use* repark, written for a data engineer adopting it as a
PySpark replacement. Plain Markdown, no site tooling.

This directory is the one place in the repo aimed at a **user** rather than a contributor. The
contributor spine ([../../AGENTS.md](../../AGENTS.md), [../../ARCHITECTURE.md](../../ARCHITECTURE.md),
[../../DEVELOPMENT.md](../../DEVELOPMENT.md), [../testing.md](../testing.md)) is not restated here,
and nothing here is authoritative: a guide **describes** behavior the engine already has and
**links** the document that owns each fact —
[../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) for every difference from Apache
Spark, [../../STATUS.md](../../STATUS.md) for release and delivery state, the ADRs for why a
boundary exists. A guide that restates one of those is wrong by construction.

**Truth rule for this directory:** every behavioral claim is verified against the tree, and every
code snippet shown was executed against a built module before it landed — outputs are real, never
illustrative. A claim with no verified basis does not go in.

## Contents

- [getting-started.md](getting-started.md) — install from PyPI (`pip install repark`, Python ≥ 3.12,
  abi3 wheel, the optional extras), the one-line import swap, the first session, `createDataFrame`,
  parquet / CSV / JSON round trips, a first `dynamicFlatten`, and the pointer to the tour notebook.
- [repark-toml.md](repark-toml.md) — `repark.toml` file configuration (CFG-1, 2026-09-10):
  one complete example file (`[default]`, `[prod]`, `[read]`, `[write]`, one catalog, one
  database source, the display, session, and maintenance tables); discovery order, `REPARK_ENV`,
  `REPARK_CONFIG` with the set-but-empty disable, `${VAR}` with `$$` escaping and the loud
  refusals, the builder > profile > default chain, and the redacted dump with its `source`
  column. States both live constraints: a non-empty `[<profile>.database]` table refuses at
  load until CFG-2, and `[<profile>.conf]` keys apply in sorted-key order. Every block and
  error was run in the clone. pins: cfg-1/C-030
- [maintenance-policy.md](maintenance-policy.md) — `[<profile>.maintenance]` and
  `CALL run_maintenance()` (MAINT-POLICY-1, 2026-09-10): the D-1 policy shape with
  per-table overrides, duration strings, the D-4 step order with the delete-ratio gate,
  the D-3 result frame with a worked dry run and a worked apply (both executed against
  the built module), the `session.run_maintenance` wrapper, the D-6 refusal, and the
  reserved `adaptive_partitioning` key. **AP-1 step 2 (2026-09-11):** adds the
  `plan_partitioning` section — the CALL, the D-1 frame columns, the footer-measured
  `byte_ratio` behind `projected_files_at_target` with its 0.55 fallback, and the
  plan-only boundary. **AP-2 (2026-09-11):** `apply_partitioning` — signature and
  defaults (`dry_run` true), re-derived `plan_id`, the same `target_file_size_bytes`
  for two-field plans (D-8), step order, result columns, one-commit-per-step warning,
  P-5 refusals, and a plan-then-apply example. **AP-3 (2026-09-12):** the projection
  now multiplies the footers' uncompressed sum by the ratio once (S2-23), and the
  section gains the S2-24 known-issues line — zstd compaction is a net-size loss
  until F-REWRITE-SIZE-1 lands.
  pins: maint-policy-1/C-026
  pins: ap-1/C-013
  pins: ap-2/C-007, C-008
- [session-and-conf.md](session-and-conf.md) — the `ReparkSession` builder; `getOrCreate` reuse
  semantics; F-Y10-1 notes SMALLINT wrap residue (2026-08-30); how `conf.get` / `conf.set` behave (unset keys raise; three tiers of key: build-time
  engine knob / live `datafusion.*` / facade-local); where the defaults live (`_SQLCONF_DEFAULTS`);
  and the keys users actually set — `spark.sql.pyspark.inferNestedDictAsStruct.enabled` (FA-4),
  `spark.sql.session.timeZone` (TZ-2 / TZ-3), `spark.sql.ansi.enabled`, target partitions, batch
  size, the one-truth memory pool, the Iceberg catalog caches (`metadataCache` /
  `metadataCacheEntries` / `manifestCacheBytes`, all build-time, memory-catalog-only),
  the four `repark.display.*` keys (polars default, `max_rows` / `max_cols` / `str_len`;
  DISPLAY-POLARS-1 step 5, 2026-09-09 — every transcript executed; pins: display-polars-1/C-006).
  DISPLAY-LAZY-1 step 2 (2026-09-10): the `repr` paragraph states the R-22 schema-only
  lazy default and the three ways to see rows.
  CONF-UNREAD-1 step 2 (2026-09-11): the `datafusion.*` paragraph names the
  measured forwarding set, the two repark-owned `datafusion.runtime.*`
  pseudo-keys, and the `coalesce_batches` refusal on DataFusion 54.1.0 with the
  verbatim message.
  pins: conf-unread-1/C-007
- [dataframe-guide.md](dataframe-guide.md) — the lazy model and what is schema-only; the
  D-1 lazy-`repr` block with measured bytes (DISPLAY-LAZY-1 step 2, 2026-09-10);
  the `explain` sections (Spark headers over verbatim DataFusion plan text, the five modes, and
  which of them execute); select /
  filter / groupBy / joins (incl. the semi family and the conditionless refusal, G4-3) / window
  functions; the action table (`collect` / `to_arrow` / `to_arrow_batches` / `toPandas` /
  `to_polars` / `toLocalIterator`) with peak-memory cost; ingestion shapes and map-vs-struct dict
  inference under the FA-4 default; struct-field addressing; `dynamicFlatten` flags
  (including `empty_as_null`) and mixed-case `explode`; and the limits worth knowing
  (FA-1, ID-1, ID-3, G10-1, TY-4/TY-5, FA-3).
- [dbt-on-repark.md](dbt-on-repark.md) — the `dbt-repark` adapter (DBT-1, 2026-09-04): dbt's
  compiled SQL in process through `repark.sql()`, no server and no JVM. What works
  (`materialized='table'` with `file_format='iceberg'`, `tblproperties`, `partition_by`, generic
  tests, `threads`, `dbt docs generate`) and what refuses at compile time (`view`, `incremental`,
  snapshots, `persist_docs`, `location_root`, `options`, `clustered_by`), each naming its
  registry row; the profile fields; why a memory catalog is per-session; and that there are no
  transactions. Package: [../../python/dbt-repark/map.md](../../python/dbt-repark/map.md).
- [sql-doors.md](sql-doors.md) — the two SQL surfaces honestly: the Spark-facade door
  (`spark.sql`, Spark dialect, your session) and the native door (`repark.sql`, stock
  DataFusion/ANSI, its own process-wide session); the no-blended-parser rule (ADR-0002); why two
  sessions; wrong-door sniffing at a high level and what the Python callable does *today*;
  identifier case (ID-1); how to choose.
- [ta-guide.md](ta-guide.md) — the `repark.ta` library: the un-`OVER`ed-column shape, `over_columns`
  and the `with_indicators` serving door, the `null_lookback` prefix rewrite, the 81 entry points
  over 68 kernels, the `ta_*` SQL spelling (Spark door only), the TA-Lib C 0.4.0 bit-exactness
  claim as the crate states it (`f64::to_bits` goldens, the `linearreg_angle` libm caveat), the
  SE-1 `declareSorted` door with its measured null-placement boundary and the
  `tightenNulls` opt-in (PR-D1), the benchmarking contract
  (default-conf primary vs single-core isolation), and the mimalloc wheel note.
- [ml-guide.md](ml-guide.md) — `repark.spark.ml`: fit/transform, the three natively-trained
  estimators and their loud refusals, the dense (`fixed_size_list`) and sparse
  (`{size, indices, values}` struct) vector cell shapes, the plan-built feature package,
  `Pipeline` persistence (params only, atomic, allowlisted on load), the `repark[ml-ext]`
  backends, and an explicit table of what is absent versus `pyspark.ml`.
- [iceberg-guide.md](iceberg-guide.md) — catalogs (Glue primary, S3 Tables secondary, the local
  in-memory one), the `spark.sql.catalog.<name>.*` keys, accepted warehouse locations
  (`s3://` / `s3a://` / `file://` / bare absolute path) and their refusals, reading and writing
  through the facade (CTAS-VIEW-1, 2026-09-03: unpartitioned parquet-view CTAS is named),
  partition overwrite (DML-1 FIXED), the write forms that refuse
  (DML-2 / `overwrite(condition)`), time
  travel both spellings plus the reader options, the sixteen metadata tables, maintenance `CALL`
  plus `register_table` adoption (V3-1, including the Spark-written v3 fixture numbers),
  and the registry sections that govern each. **MW-6** added "Compacting manifests"
  (`rewrite_manifests`: the current-spec default, the `spec_id` refusal, and the delete manifests
  Spark rewrites and this engine does not). **MW-8 (2026-08-24)** added "The maintenance
  runbook" — the seven-step Airflow-shaped cycle, the `expire_snapshots` cutoff and what it
  costs in time travel, the cadence, the load-bearing order, the day of latency on the orphan
  net, how to retry a step (the S3 Tables conflict and step 4's idle-cycle refusal), the six
  edits a migrating Spark DAG needs, and the limit the cycle cannot cross (registry `RDF-1` —
  rewritten 2026-09-02: the cycle now reclaims a delete-laden file whose delete file names it
  alone; a delete file naming several data files is the residue).
  **MW-10** adds the S3 Tables paragraph: automatic snapshot management stays on for scratch
  tables (no branch/tag/`history.expire.*`), engine expire and the service may run together,
  conflicts retry a bounded number of times, and the measured interplay slot.
  MW-7's numbers are cited to
  [../../task/ledgers/completed/mw-7-scale-measurement-ledger.md](../../task/ledgers/archive/2026-08/2026-08-24-mw-7-scale-measurement-ledger.md)
  §6, never restated.
  **SCALE-v3 (2026-09-02)** gave the runbook section its format version. Every number there was
  fitted to format v2, and three of them do not transfer, so the section now says v2 or v3
  before it says a figure:

  | Runbook claim | v2 | v3 |
  |---|---|---|
  | the cycle's first CALL | `rewrite_position_delete_files` folds 400 delete files to 8 | four zeros on live DVs (`B-MOR-3` FIXED); `rewrite_data_files` reclaims all 96 |
  | 2x cadence crossing | 19.6 merges (merge 20 already 2.05x) | partition probe between merge 30 and 40; point probe between 40 and 50 |
  | debt trigger | ~157 delete files (`partition` granularity) | the delete-file count plateaus at the data files carrying deletes (96); trigger on delete RECORDS |
  | cycle budget | ~2.5 minutes | ~6 minutes (353.9 s) |
  | after the cycle | 2.02x / 2.45x the COW control, 1.90x its bytes | 0.61x on the point probe, zero delete files, zero delete records |

  Two sentences elsewhere in the guide were false since V3-9 and are corrected and dated in the
  same pass: "the engine still cannot create a v3 table" (it does, behind
  `repark.sql.allowCreateFormatVersion3`) and "repark writes no v3 delete files itself" (a
  merge-on-read write writes one file-scoped Puffin DV per touched data file).
  SCALE-v3's numbers are cited to
  [../../task/ledgers/staging/scale-v3-mw7-ledger.md](../../task/ledgers/archive/2026-09/2026-09-02-scale-v3-mw7-ledger.md)
  §3, never restated.
- [troubleshooting.md](troubleshooting.md) — the gotchas in one page, symptom → why → what to do:
  dict-cell struct inference (FA-4), dotted-path `select`, euro-comma CSV decimals,
  `explode_outer` on `array<struct>` (now keeps null/empty rows), `count()` **and any
  narrowing `select`** on a deep `dynamicFlatten` — **FIXED, kept as a fixed entry**
  (DEFECT-2 2026-08-18: the trigger was DataFusion 54.1's `push_down_leaf_projections`, which
  the core session now wraps so it declines on the `Unnest` plans it miscompiles; the section
  states the mechanism, the measured perf numbers on both sides of the scope choice, and
  re-frames `cache()` as ordinary caching rather than a workaround),
  `smartCsv` delimiter auto-detect,
  the wrong-door `ParserError`, the UTC timezone default (TZ-2 / TZ-3), and the install smoke.
- `map.md` — this file.

## I want to...

| ...do this | go to |
|---|---|
| Install repark and run something | [getting-started.md](getting-started.md) |
| Find out which conf key does what, and when it takes effect | [session-and-conf.md](session-and-conf.md) |
| Configure a session from a `repark.toml` file | [repark-toml.md](repark-toml.md) |
| Understand why a `conf.set` appeared to do nothing | [session-and-conf.md](session-and-conf.md) "How `conf.get` / `conf.set` behave" |
| Learn the DataFrame API / flatten nested data | [dataframe-guide.md](dataframe-guide.md) |
| Work out which `sql()` to call | [sql-doors.md](sql-doors.md) |
| Compute technical indicators, or make a TA window stop re-sorting | [ta-guide.md](ta-guide.md) |
| Fit a model, or find out whether an estimator exists at all | [ml-guide.md](ml-guide.md) |
| Point a session at Glue / S3 Tables, or read an Iceberg table | [iceberg-guide.md](iceberg-guide.md) |
| Time-travel a table, or work out why a statement refuses | [iceberg-guide.md](iceberg-guide.md) |
| Schedule table maintenance, or port a Spark maintenance DAG | [iceberg-guide.md](iceberg-guide.md) "The maintenance runbook" — read its format-version line first; the v2 and v3 cycles differ in their first CALL. For the policy-driven single CALL, [maintenance-policy.md](maintenance-policy.md) |
| Diagnose a surprising result or a loud refusal | [troubleshooting.md](troubleshooting.md) |
| Find out how repark differs from Apache Spark, and why | [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) (authoritative) |
| Check release / delivery state | [../../STATUS.md](../../STATUS.md) (authoritative) |
| Run a worked example end to end | [../../examples/notebooks/datasets_tour.ipynb](../../examples/notebooks/datasets_tour.ipynb) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../examples/map.md](../../examples/map.md) — runnable examples; the guides link the
  tour notebook rather than duplicating it. [../../README.md](../../README.md) points here.

## Constraints

- No credentials, no real hosts (`example.com` only), no absolute user paths in any snippet.
- Snippets are executed before they land; an output block that was not produced by a real run does
  not belong here.
- Do not restate STATUS.md, the divergence registry, or the contributor spine — link them.

## Debug

| Symptom | First check |
|---|---|
| A guide and the divergence registry disagree | The registry wins — it is the authoritative home and carries the pin. Fix the guide |
| A guide describes a surface that no longer exists | The guide was not updated with the change; the pin that moved names the new behavior |
| A snippet's output does not reproduce | Rebuild the module (`make develop`) — a guide's outputs are recorded against a built wheel, not a source tree |
