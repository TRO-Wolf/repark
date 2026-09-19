# map — repark-spark/benches/ice_read_perf

## Purpose

The ICE-READ-PERF-0 bench bed (slate
[ice-read-perf-slate-2026-09-18.md](../../../../task/roadmap/mid-term/ice-read-perf-slate-2026-09-18.md)
unit 0). It writes a deterministic local Iceberg table (or, on the dispatch-only AWS leg, an S3 Tables
and a Glue table) and measures seven read queries in four modes through the product path, with request and byte counts from the session's counting
layer (`ReparkSession::iceberg_io_stats`, see
[repark-iceberg/src/catalog/map.md](../../../repark-iceberg/src/catalog/map.md)). Its baseline
table is the "before" of every later unit of the slate. No product behaviour lives here.

## Contents

- `main.rs` — the `[[bench]]` root (`harness = false`): reads `std::env::args` and
  `GITHUB_STEP_SUMMARY`, then calls `cli::main_with`.
- `pins.rs` — the `[[test]] ice_read_perf_pins` root: declares the same modules and pins them
  offline (see "Pins"). Every tested function is also on the bench's own path, so neither root
  carries dead code.
- `cli.rs` — hand-written argument parsing (no new dependency), the exit codes (`exit_code`
  maps an outcome to 0 / 1 / 3), and the runtime. `cargo bench` appends `--bench`; the parser
  drops it.
- `bed.rs` — `setup`, the Spark-door session builder, the bed DDL and the one-INSERT-per-file
  writer, the bed manifest, `BedShape::from_counts`, and the local re-registration a `run` uses.
- `remote.rs` — the AWS `setup --phase create|write` (namespace rules, the table-state checks,
  the closing R-3 check) and the Glue / S3 Tables catalog registration `run` shares. The
  registration goes through `register_late_configured_catalogs` with a
  `repark.sql.catalog.bench.type = glue|s3tables` block, the path a user's configuration takes,
  so S3 Tables gets its service-managed create location and both catalogs get the counted
  factories; the first AWS dispatch (run 35446174539) failed its S3 Tables create because the
  bench had registered the handle without that policy.
- `run.rs` — the queries, the four modes, repeats, the per-query measurement, the scan-predicate
  probe. Every session a run uses comes from one `SessionSource` (`ConfiguredSource` opens the
  local bed or the Glue / S3 Tables catalog). `run` resolves the target and calls `run_gated`,
  the one path every mode and catalog takes. It opens the gate session, runs the R-3 gate,
  and on the flag returns the live gate session's counters before any other session opens or
  any query plans or runs.
- `r3.rs` — the R-3 size flag and the `files`-table footprint (bytes, files, delete files,
  data rows).
- `report.rs` — the environment header, the I/O JSON (with the footer / page split), the RSS
  probes, the run-level I/O tally, sample medians and the identical-I/O check, the markdown
  table.

## Commands

```
cargo bench -p repark-spark --bench ice_read_perf -- setup --warehouse <dir> [--files 200] [--rows-per-file 50000]
cargo bench -p repark-spark --bench ice_read_perf -- run --mode cold|warm|concurrent|concurrent-cold --warehouse <dir> [--repeat N] [--out <file.json>] [--query Q1..Q7]
cargo bench -p repark-spark --bench ice_read_perf -- setup --catalog glue|s3tables --prop k=v … --table <ns.table> --phase create|write [--files 200] [--rows-per-file 50000]
cargo bench -p repark-spark --bench ice_read_perf -- run --mode … --catalog glue|s3tables --prop k=v … --table <ns.table> [--files 200] [--rows-per-file 50000] [--repeat N]
```

Glue needs `--prop warehouse=<s3 uri>`, S3 Tables `--prop table_bucket_arn=<arn>`; the AWS
region comes from the environment (`AWS_REGION`, set by the credentials action).

The bench profile inherits the default release profile; do not override it for a baseline.

## The bed (`setup`)

- **Layout:** a memory catalog over a local directory, namespace `perf` with location
  `<warehouse>/perf`, so the table is a plain path-based Iceberg directory
  (`<warehouse>/perf/events/{metadata,data}`), format v2. `run` re-registers it in a fresh
  session from the metadata file the manifest names (`Catalog::register_table`).
- **Exactly N files:** one `INSERT … SELECT … FROM range(first, first + rows) ORDER BY id` per
  data file (`--files`, default 200). Without the `ORDER BY` the writer splits an INSERT of more
  than 65,536 rows across eight writers (measured: 500,000 rows gave 8 files), so a large-file bed
  (`--rows-per-file 500000`) needs it; at the default 50,000 rows the bytes are unchanged. Setup counts the `files` metadata table afterwards and fails loud
  unless it holds exactly N data files and no delete file.
- **Columns** (all a pure function of `id`; no seed, no clock): `id BIGINT` (0‥N·rows, one
  contiguous range per file, so per-file min/max are tight); `ts TIMESTAMP` =
  `CAST(1700000000 + id * 7 AS TIMESTAMP)` (clustered with `id`, about 810 days at the
  default size, so a 1% window touches about 1% of files); `category STRING` =
  `cat_<pmod(id * 2654435761, 16)>` (16 values, spread through every file); `value DOUBLE` =
  `pmod(id * 40503, 1000003) / 1000.0` (spread through every file); `payload STRING` = four
  salted SHA-256 hex digests (256 characters; hex compresses to about half, so the column
  dominates the size). Data file names still carry the writer's UUIDv7, so bytes are equal
  across runs and names are not.
- **Size:** 50,000 rows per file measured 7.1 MB (2026-09-19), so the default bed is about
  1.4 GB (inside the slate's ≤ 2 GB) and writes in a few minutes. The writer's default page
  row limit gives several data pages per column; the manifest records the first file's row
  groups and pages per column (from the offset index) so a reader can check it.
- **Manifest:** `<warehouse>/ice_read_perf_bed.json` — metadata location, files, rows, bytes,
  the generator constants, the page census, setup seconds. Setup prints the metadata location,
  the total bytes and the file count.

## The queries

| name | SQL shape | what it measures |
|---|---|---|
| Q1 | `SELECT count(*)` | claim C-9: whether the count folds to metadata (data-file reads recorded) |
| Q2 | `sum/min/max(value)` | full scan of one column |
| Q3 | `id, value WHERE ts ∈ [45%, 46%)` of the `ts` span | selective 1% time window (file pruning; page pruning later) |
| Q4 | `* WHERE id = rows/2 + 17` | point lookup |
| Q5 | `id, value WHERE value ∈ [500, 505)` | range filter with no file pruning |
| Q6 | `id, payload WHERE category = 'cat_7'` | equality filter + wide projection |
| Q7 | `id, value WHERE id ∈ [45%, 46%)` of the rows | Q3's exact rows spelled so the window reaches `IcebergTableScan` |

**Q3 and Q7 (round 2, review finding P1-Q3-NO-PREDICATE).** Q3 stays exactly as users write it
(`CAST(<seconds> AS TIMESTAMP)`): the literal plans as `TimestampMicrosecond(…, "UTC")`, which
the fork's predicate conversion drops, so the scan shows `predicate:[]` and reads every file.
Q3 is the cell that moves when that gap is fixed (the 24b ask F-TS-PUSHDOWN-1). Q7 asks for the
same rows (`ts = base + id·7`, so the `ts` window is exactly an `id` window) through the one
spelling that reaches the scan today. Probed on 2026-09-19 and all left at `predicate:[]`:
`CAST(n AS TIMESTAMP)`, `TIMESTAMP '…'` (with and without `+00:00`), `CAST('…' AS TIMESTAMP)`,
`BETWEEN TIMESTAMP …`, `to_timestamp('…')`. Refused at the door: `TIMESTAMP_NTZ '…'` (TZ-6),
`TIMESTAMP_LTZ '…'`, `timestamp_seconds`. The `id` window plans as `predicate:[(id >= a) AND
(id < b)]`. Every run records each query's scan predicate in the JSON
(`iceberg_scan_predicate`, from the physical plan on the gate session after the R-3 check, `""`
= a scan with no predicate, `null` = no `IcebergTableScan` in the plan: the query was answered
from statistics, as `count(*)` is once ICE-COUNT-FOLD-1 lands). The pin holds Q3 at `""` and Q7 at the `id` range with the same row set. It is a
sentinel: the unit that fixes the timestamp conversion flips the Q3 half.

The concurrent modes issue Q2, Q3, Q5 and Q6 at once. Q7 is **not** in that set: a fifth query
would change what "four at once" measures for the other four (their timings and the group I/O).

## The modes and metrics

- **cold:** every query sample gets its own fresh session (new `ReparkSession`, catalog
  registered, table registered from its metadata file), runs once, and is measured. It is
  **session-cold, not process-cold and not device-cold**: all samples share one process, and the
  OS page cache is not dropped (that needs root), so a local cold run's later queries read files
  the earlier ones pulled into the page cache. The JSON says so (`cold_kind:
  "new_session_same_process"`, `os_page_cache: "not_dropped"`). Local request and byte counts
  are exact. Local cold wall times are optimistic, so rank local latency on warm. On the AWS
  leg every read is a real request. `--query Qn` runs one query per process. The
  registration's own I/O is kept apart (`register_io`). With `--repeat N` the order is
  sample-major: all queries, then all queries again.
- **warm:** one session; each query runs once unmeasured, then N times measured.
- **concurrent:** one session; the four queries run once unmeasured, then N rounds of all four at
  once (`futures::future::join_all` on one driver task, so the `tokio::spawn` ban holds; the
  scan partitions still fan out on the multi-thread runtime). Each query has its own timings;
  I/O, cache and RSS belong to the round (`concurrent_group`). `concurrent_kind:
  "join_all_four_warmed_queries_one_session_one_driver_task"`. This mode is not a 64-way scan
  and cannot see concurrent cache misses.
- **concurrent-cold:** N rounds; each round opens a fresh session, runs no warm-up, and issues the
  four queries at once. This is the concurrent-miss case ICE-CATALOG-CACHE-1 must move: the
  group reads manifests, which the warm concurrent group does not (pinned). `concurrent_kind:
  "join_all_four_queries_fresh_session_no_warmup_one_driver_task"`. The round's registration I/O
  is in the group sample's `register_io`. Concurrent misses can race, so the group's I/O may
  differ between rounds. When it does, the mismatch is printed (see repeats) and not averaged.
- **Per sample:** `planning_ms` (`session.sql` + `create_physical_plan`, which includes the
  Iceberg scan's manifest planning), `first_batch_ms` (from the SQL start, so it INCLUDES
  planning), `execute_to_first_batch_ms` (first batch − planning), `total_ms`, rows, requests and
  bytes by operation kind and by file class (counters reset before each measured query or
  round), metadata-cache hits / misses / body fetches (a delta of
  `iceberg_metadata_cache_stats`), `rss_at_reset_kib`, and `peak_rss_kib`. RSS reset: the bench
  writes `5` to `/proc/self/clear_refs`, which resets `VmHWM` to the CURRENT RSS, not to zero.
  `peak_rss_kib` is therefore `max(rss_at_reset, peak during the query)`: memory left over from
  earlier queries is a floor, and a query cannot show a peak below it. Read the two figures
  together. Use the peak as a growth detector, not as a per-query allocation figure
  (`peak_rss_kind` in the header). Linux only.
- **Footer vs page (review finding P1-FOOTER-VS-PAGE).** Data-file (and delete-file) ranged
  reads are split into `footer_read` and `ranged_read` by the counting layer's tail-magic rule
  (a read that ends on `PAR1` / `PFA1` ended at the end of the file; see
  [repark-iceberg/src/catalog/map.md](../../../repark-iceberg/src/catalog/map.md)). Each query's
  `io` carries `data_file_ranged` / `delete_file_ranged` `{footer, page}` and the table shows
  both columns. ICE-FOOTER-CACHE-1 moves the footer column (Q1 reads footers only), and
  ICE-PAGE-PRUNE-1 moves the page column (Q7, and Q3 once its predicate reaches the scan). Proved
  on the three-file bed in every mode that measures per query: Q1 = one footer read per data
  file and zero page reads; Q2 = one footer read per data file plus page reads. On the 20-file
  smoke Q1 was 20 footer reads of exactly 524,288 bytes (the fork's 512 KiB prefetch hint) and
  no page read. Footer counts exceed the file count when the fork's scan re-pack (PERF-ICE-SCAN-1,
  byte ranges of `max(total / CPUs, 64 KiB)`) splits a file into several tasks: each task
  fetches the tail again. The 20-file smoke on 26 CPUs gave Q2 40 footers and Q4 and Q7 26
  (their one surviving file split 26 ways). So the footer cells depend on `cpu_count` and on
  the table size; compare them only at an equal CPU count.
- **Repeats (`--repeat N`, default 1).** Every measured query (and every concurrent round) runs N
  times; cold takes N fresh sessions per query. The JSON keeps every sample (`samples`) and a
  `median` block (timings and RSS). Requests and bytes are sample 1's. Every other sample's I/O
  and rows are compared with it. A difference prints `IO-MISMATCH mode=… query=… sample=…` to
  stdout and stderr, lands in the JSON's `io_mismatches`, sets `io_identical_across_samples:
  false`, and shows as **NO** in the table's `io same` column. It is never averaged. The
  markdown table shows medians. The orchestrator's local baseline uses `--repeat 5`, and the
  AWS leg uses `--repeat 1` (`BENCH_REPEAT`, ruling Q-24a-1).
- **Run total:** `run_io_total` sums every request and byte the run made through the counters,
  across every session it opened: the R-3 gate, the scan-predicate probe, registrations,
  warm-ups and every sample. On the AWS leg that is what the dispatch paid to read. The workflow
  sums it into its step summary.
- **Bed check:** after the R-3 check, every `run` requires the table to hold exactly the
  expected data files and rows (from the manifest locally, from `--files` / `--rows-per-file` on
  AWS) and no delete file. Otherwise the query constants would be wrong, so it fails loud.
- **Output:** one JSON document (`--out`, else stdout) with the H-3 environment header (git head
  and dirty flag, fork pin read from the workspace `Cargo.toml`, profile, CPU count and model,
  kernel, load average, UTC date, `rustc_version` — `rustc --version` resolved in the workspace
  at run time — `cold_kind`, `concurrent_kind`, `os_page_cache`, `peak_rss_kind`,
  `first_batch_kind`), then the markdown table and `run_io_total` on stdout.

## The AWS leg (`setup --catalog glue|s3tables --phase create|write`, `run --catalog …`)

- **create** is idempotent. It creates the namespace the way the acceptance module does
  (`python/repark/tests/_acceptance.py`): on Glue with `location = <warehouse>/<namespace>`, and
  after the create it fails loud unless the stored location is exactly that (a missing or other
  location refuses to adopt, as `assert_glue_scratch_namespace_location` does); on S3 Tables
  with no location, because the table bucket is the storage. Then it creates the empty table
  with the bed DDL. If the table exists, it checks that the schema is exactly the bed's (`id
  long, ts timestamptz, category string, value double, payload string`) and reports it exists.
  It fails loud on any other schema.
- **write** writes the N files (one INSERT each, as locally) ONLY when the table holds no file.
  When it holds exactly N data files and N × rows data rows and no delete file, it skips and
  says so. In any other state it fails loud before writing (pinned: zero write requests). A
  table that a failed write left part-written cannot be repaired from CI (the role has no drop):
  an owner drops it with owner credentials, see
  [docs/tier2-aws.md](../../../../docs/tier2-aws.md).
- Both phases end with the R-3 check and exit 3 on the flag (pinned through `run_phase` with an
  injected size).
- `run --catalog glue|s3tables` needs no local manifest. The query constants derive from
  `--files` / `--rows-per-file` (defaults 200 × 50,000), and they are pinned equal to the
  constants a local manifest of the same counts records. `--manifest` with an AWS catalog is a
  usage error.
- Offline pins cover every failure path with no AWS call: missing `warehouse` /
  `table_bucket_arn`, a missing or malformed `--table`, `--manifest` on AWS, and every table
  state, on a local memory-catalog stand-in.

## R-3 — the size flag (owner ruling R-3, 2026-09-18)

- Before the first read of every `run`, and at the end of `setup`, the bench sums
  `file_size_in_bytes` over the table's `files` metadata table (its current snapshot's live
  data **and** delete files; pinned below) and prints `R3 table=<name> bytes=<n>`.
- Above `R3_TABLE_SIZE_LIMIT_BYTES` = 3 × 1024³ = 3221225472 (a constant, never an option or
  an environment override) it prints `R3-SIZE-FLAG table=<name> bytes=<n> limit=3221225472`,
  appends the same line to `$GITHUB_STEP_SUMMARY` when that is set, runs no scan, and exits
  **3**. At exactly the limit it passes.
- Exit codes: 0 done, 1 runtime failure, 2 usage error, 3 the R-3 flag.
- On the AWS leg every `setup` phase and every `run` makes this check (the workflow runs them
  without `continue-on-error`, so exit 3 fails the job at once). The Slack note to the owner is
  not wired yet. It is keyed on exit 3.
- The size query spells the metadata table with backticks (`` `bench`.`perf`.`events`.`files` ``).
  On the bare Rust Spark door an unquoted `cat.ns.tbl.files` is rewritten to an unquoted
  `tbl$files` that the Databricks tokenizer splits (`Expected: end of statement, found:
  $files`); the Python facade quotes names first and does not see it. That is a product
  defect outside this unit, reported in the unit's hand-back.

## Pins (`pins.rs`)

- The decision at limit − 1, limit and limit + 1 (and 0, `u64::MAX`); the constant; exit 3 is
  distinct; a flag appends exactly one line to the step summary and a pass appends none; no
  `--limit` option parses. pins: ice-read-perf-0/C-007
- `a_fake_size_above_the_limit_stops_every_mode_and_catalog_before_any_query` (round 3,
  F-MUT-4D / F-MUT-4E). It drives `run_gated` for all four modes on all three catalog choices,
  so twelve cases, on a three-file bed through a stand-in `SessionSource`. The stand-in
  registers the local bed as `bench.perf.events`, the name a `--catalog glue|s3tables --table
  perf.events` run resolves to, and records every session it opens. The injected size is
  limit + 1 (`Some(size)`, a parameter no CLI flag reaches). First, at exactly the limit, Q2
  runs on each catalog choice. Then the bed's data files are deleted, and a Q2 run at the limit
  now fails on each catalog choice. That makes the probe live: any query that executes after the
  gate errors. Each flagged case must return `SizeFlagged` rather than an error. The stand-in
  must have opened exactly one session. The boxed counters must equal that live session's
  counters: zero data-file and delete-file requests, manifest reads above zero. `cli::exit_code`
  must give 3, and the step summary must hold one flag line. The production `run::run` path
  repeats the flag for every local mode. It reds under "return the flag after `measure()` with
  the pre-scan snapshot", and under a flag skipped for concurrent-cold, cold, Glue or S3 Tables
  (2026-09-19, round 3). pins: ice-read-perf-0/C-008, C-019
- The `files` metadata table counts a merge-on-read position-delete file: the footprint of a
  table with one data and one delete file is 2 files, 1 delete file, and the byte sum equals
  the two files on disk, read without a data-file request. pins: ice-read-perf-0/C-009
- `setup` writes exactly N files; every mode runs on them and writes its JSON (Q3 returns the
  1% window, Q4 one row, Q2 reads data-file ranges); the parser reads every flag and drops
  `--bench`; the Glue and S3 Tables legs fail loud on a missing `warehouse` /
  `table_bucket_arn` / `--table` before any AWS call, and both register through a
  `repark.sql.catalog.bench` config block that parses back to the right catalog kind; a
  source-binding pin holds `register_remote_catalog` to `register_late_configured_catalogs` and
  keeps the bare-handle builders out of `remote.rs` (Grok verification, PR #724).
  pins: ice-read-perf-0/C-006
- Q3's scan predicate is `""` today and Q7's is `(id >= 540) AND (id < 552)` on the three-file
  bed, with the same twelve ids; the plan-line parser. pins: ice-read-perf-0/C-013
- Q1 reads only footers (3 footer reads, 0 page reads) and Q2 reads 3 footers plus pages, in
  cold and warm. pins: ice-read-perf-0/C-014
- Every mode (four) with `--repeat 2`: two samples per query, medians, identical I/O across
  samples in cold and warm, `rss_at_reset_kib` and `execute_to_first_batch_ms` present, the
  environment header's `rustc_version`, `os_page_cache`, `cold_kind` and `concurrent_kind`, and
  `run_io_total`; the concurrent-cold group reads manifests while the warm concurrent group reads
  none. The parser reads `--repeat`, `--files`, `--rows-per-file`, `concurrent-cold` and the
  remote `setup` flags and refuses `--repeat 0` and every mixed local / remote setup.
  pins: ice-read-perf-0/C-015
- The derived constants of `--files 3 --rows-per-file 400` equal the local manifest's (rows,
  files, `ts` base and step, categories, and every query's SQL). pins: ice-read-perf-0/C-016
- The remote phases on a memory-catalog stand-in: create, then create again (exists); write into
  the empty table, then skip; three refusals at other counts or rows; a table with a delete
  file refuses; zero write requests on every skip and refusal; a wrong schema refuses; a
  namespace at another location or with none refuses under the Glue rule; the S3 Tables rule
  creates without a location; both phases end with the R-3 flag under an injected size. The AWS
  failure paths (missing props, `--table`, `--manifest`) fail before any call.
  pins: ice-read-perf-0/C-017

## Pointers

- Up: [../map.md](../map.md)
- Baseline file (filled by the orchestrator):
  [docs/perf/ice-read-perf-baseline-2026-09-19.md](../../../../docs/perf/ice-read-perf-baseline-2026-09-19.md)
- Ledger: [task/ledgers/staging/ice-read-perf-0-ledger.md](../../../../task/ledgers/staging/ice-read-perf-0-ledger.md)
