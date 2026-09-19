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

## The bed

| key | value |
|---|---|
| command | `cargo bench -p repark-spark --bench ice_read_perf -- setup --warehouse <dir>` |
| files / rows | 200 / 10,000,000 |
| bytes (R-3 sum) | |
| pages per column (first file) | |
| setup seconds | |

## Results

Per query: plan ms, first-batch ms, total ms, rows, data-file ranged requests / bytes, metadata
JSON / manifest-list / manifest requests, total requests / bytes, metadata-cache hit / miss,
peak RSS MiB.

### cold

| query | plan ms | first batch ms | total ms | rows | data ranged req/bytes | meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | peak RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|

### warm

| query | plan ms | first batch ms | total ms | rows | data ranged req/bytes | meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | peak RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|

### concurrent (Q2, Q3, Q5, Q6 at once; I/O per group)

| query | plan ms | first batch ms | total ms | rows | data ranged req/bytes | meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | peak RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
