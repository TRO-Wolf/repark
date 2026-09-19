# Iceberg read-performance baseline (ICE-READ-PERF-0)

**Status:** skeleton, 2026-09-19. The orchestrator fills it with the 200-file local run on the
unit's merged head. It is the "before" of every unit of the
[read-performance slate](../../task/roadmap/mid-term/ice-read-perf-slate-2026-09-18.md); the
re-measure gate (slate unit 4) adds its "after" beside it. The AWS rows arrive with the
dispatch-only AWS leg.

Bench: [crates/repark-spark/benches/ice_read_perf/](../../crates/repark-spark/benches/ice_read_perf/map.md)
(queries, modes, metrics and the R-3 size flag are defined there, not here).

pins: ice-read-perf-0/C-006

## Machine and profile (H-3)

Copy from the `environment` object of each run's JSON.

| key | value |
|---|---|
| git head (dirty?) | |
| fork pin | |
| profile | default release (`cargo bench`) |
| cpu model / count | |
| kernel | |
| load average (start of each run) | |
| date (UTC) | |
| rustc version | |
| cold kind / OS page cache | `new_session_same_process` / `not_dropped` |
| repeats | `--repeat 5` (timings and RSS are medians; I/O is sample 1's, checked equal) |

## The bed

| key | value |
|---|---|
| command | `cargo bench -p repark-spark --bench ice_read_perf -- setup --warehouse <dir>` |
| files / rows | 200 / 10,000,000 |
| bytes (R-3 sum) | |
| pages per column (first file) | |
| setup seconds | |

## Results

Paste each run's markdown table. The columns are defined in the bench map: medians over the
samples, footer and page split, execute-to-first-batch beside first-batch-from-SQL-start, RSS
at reset beside the peak. Record any `IO-MISMATCH` line verbatim. Record `cpu_count`: the
footer cells depend on it (the scan re-pack splits files by the CPU count; see the unit ledger).
Commands: `run --mode <mode> --warehouse <dir> --repeat 5 --out <mode>.json`.

### cold

| query | n | plan ms | exec→first ms | first batch ms | total ms | rows | data footer req/bytes | data page req/bytes | delete req/bytes | meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | RSS at reset MiB | peak RSS MiB | io same |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|

### warm

| query | n | plan ms | exec→first ms | first batch ms | total ms | rows | data footer req/bytes | data page req/bytes | delete req/bytes | meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | RSS at reset MiB | peak RSS MiB | io same |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|

### concurrent (Q2, Q3, Q5, Q6 at once after a warm-up; I/O per round)

| query | n | plan ms | exec→first ms | first batch ms | total ms | rows | data footer req/bytes | data page req/bytes | delete req/bytes | meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | RSS at reset MiB | peak RSS MiB | io same |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|

### concurrent-cold (Q2, Q3, Q5, Q6 at once on a fresh session, no warm-up; I/O per round)

| query | n | plan ms | exec→first ms | first batch ms | total ms | rows | data footer req/bytes | data page req/bytes | delete req/bytes | meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | RSS at reset MiB | peak RSS MiB | io same |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
