# map — repark-spark/benches/ice_read_perf

## Purpose

The ICE-READ-PERF-0 bench bed (slate
[ice-read-perf-slate-2026-09-18.md](../../../../task/roadmap/mid-term/ice-read-perf-slate-2026-09-18.md)
unit 0). It writes a deterministic local Iceberg table and measures six read queries in three
modes through the product path, with request and byte counts from the session's counting
layer (`ReparkSession::iceberg_io_stats`, see
[repark-iceberg/src/catalog/map.md](../../../repark-iceberg/src/catalog/map.md)). Its baseline
table is the "before" of every later unit of the slate. No product behaviour lives here.

## Contents

- `main.rs` — the `[[bench]]` root (`harness = false`): reads `std::env::args` and
  `GITHUB_STEP_SUMMARY`, then calls `cli::main_with`.
- `pins.rs` — the `[[test]] ice_read_perf_pins` root: declares the same modules and pins them
  offline (see "Pins"). Every tested function is also on the bench's own path, so neither root
  carries dead code.
- `cli.rs` — hand-written argument parsing (no new dependency), the exit codes, and the
  runtime. `cargo bench` appends `--bench`; the parser drops it.
- `bed.rs` — `setup`, the Spark-door session builder, the bed manifest, and the local
  re-registration a `run` uses.
- `run.rs` — the queries, the three modes, the per-query measurement.
- `r3.rs` — the R-3 size flag.
- `report.rs` — the environment header, the I/O JSON, the peak-RSS probe, the markdown table.

## Commands

```
cargo bench -p repark-spark --bench ice_read_perf -- setup --warehouse <dir> [--files 200] [--rows-per-file 50000]
cargo bench -p repark-spark --bench ice_read_perf -- run --mode cold|warm|concurrent --warehouse <dir> [--out <file.json>] [--query Q1..Q6]
cargo bench -p repark-spark --bench ice_read_perf -- run --mode … --catalog glue|s3tables --prop k=v … --table <ns.table> --manifest <bed.json>
```

The bench profile inherits the default release profile; do not override it for a baseline.

## The bed (`setup`)

- **Layout:** a memory catalog over a local directory, namespace `perf` with location
  `<warehouse>/perf`, so the table is a plain path-based Iceberg directory
  (`<warehouse>/perf/events/{metadata,data}`), format v2. `run` re-registers it in a fresh
  session from the metadata file the manifest names (`Catalog::register_table`).
- **Exactly N files:** one `INSERT … SELECT … FROM range(first, first + rows)` per data file
  (`--files`, default 200). Setup counts the `files` metadata table afterwards and fails loud
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

The concurrent mode issues Q2, Q3, Q5 and Q6 at once.

## The modes and metrics

- **cold:** a fresh process; every query gets its own fresh session (new `ReparkSession`,
  catalog registered, table registered from its metadata file), runs once, and is measured.
  The registration's own I/O is recorded apart (`register_io`). The OS page cache is not
  dropped (that needs root); on the AWS leg every read is a real request.
- **warm:** one session; each query runs once unmeasured, then once measured.
- **concurrent:** one session; the four queries run once unmeasured, then run at once
  (`futures::future::join_all` on one task — the `tokio::spawn` ban holds); each query has its
  own timings, and the I/O, cache and RSS figures belong to the group.
- **Per query:** planning time (`session.sql` + `create_physical_plan`), time to first batch,
  total time, rows, requests and bytes by operation kind and by file class (the counters are
  reset before each measured query), metadata-cache hits / misses / body fetches (a delta of
  `iceberg_metadata_cache_stats`), and peak RSS (`VmHWM`; the bench writes `5` to
  `/proc/self/clear_refs` before each measured query so the mark is per query, and records
  whether the reset worked). Linux only.
- **Output:** one JSON document (`--out`, else stdout) with the H-3 environment header (git
  head and dirty flag, fork pin read from the workspace `Cargo.toml`, profile, CPU count and
  model, kernel, load average, UTC date), then a markdown table on stdout.

## R-3 — the size flag (owner ruling R-3, 2026-09-18)

- Before the first read of every `run`, and at the end of `setup`, the bench sums
  `file_size_in_bytes` over the table's `files` metadata table (its current snapshot's live
  data **and** delete files; pinned below) and prints `R3 table=<name> bytes=<n>`.
- Above `R3_TABLE_SIZE_LIMIT_BYTES` = 3 × 1024³ = 3221225472 (a constant, never an option or
  an environment override) it prints `R3-SIZE-FLAG table=<name> bytes=<n> limit=3221225472`,
  appends the same line to `$GITHUB_STEP_SUMMARY` when that is set, runs no scan, and exits
  **3**. At exactly the limit it passes.
- Exit codes: 0 done, 1 runtime failure, 2 usage error, 3 the R-3 flag.
- The Slack note to the owner is the workflow's job (the later AWS-leg PR), keyed on exit 3.
- The size query spells the metadata table with backticks (`` `bench`.`perf`.`events`.`files` ``).
  On the bare Rust Spark door an unquoted `cat.ns.tbl.files` is rewritten to an unquoted
  `tbl$files` that the Databricks tokenizer splits (`Expected: end of statement, found:
  $files`); the Python facade quotes names first and does not see it. That is a product
  defect outside this unit, reported in the unit's hand-back.

## Pins (`pins.rs`)

- The decision at limit − 1, limit and limit + 1 (and 0, `u64::MAX`); the constant; exit 3 is
  distinct; a flag appends exactly one line to the step summary and a pass appends none; no
  `--limit` option parses. pins: ice-read-perf-0/C-007
- The pre-scan path on a real three-file local bed with an injected size of limit + 1
  (`run::run(…, Some(size))`, a parameter no CLI flag reaches) returns the flag, and the gate
  session's counters show zero data-file and zero delete-file requests (and manifest reads, so
  the counter was live). The same path at exactly the limit runs Q2. pins: ice-read-perf-0/C-008
- The `files` metadata table counts a merge-on-read position-delete file: the footprint of a
  table with one data and one delete file is 2 files, 1 delete file, and the byte sum equals
  the two files on disk, read without a data-file request. pins: ice-read-perf-0/C-009
- `setup` writes exactly N files; every mode runs on them and writes its JSON (Q3 returns the
  1% window, Q4 one row, Q2 reads data-file ranges); the parser reads every flag and drops
  `--bench`; the Glue and S3 Tables legs fail loud on a missing `warehouse` /
  `table_bucket_arn` / `--table` / `--manifest` before any AWS call.
  pins: ice-read-perf-0/C-006

## Pointers

- Up: [../map.md](../map.md)
- Baseline file (filled by the orchestrator):
  [docs/perf/ice-read-perf-baseline-2026-09-19.md](../../../../docs/perf/ice-read-perf-baseline-2026-09-19.md)
- Ledger: [task/ledgers/staging/ice-read-perf-0-ledger.md](../../../../task/ledgers/staging/ice-read-perf-0-ledger.md)
