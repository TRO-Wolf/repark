# Iceberg read-performance baseline (ICE-READ-PERF-0)

**Status:** recorded 2026-09-19 by the run-24a orchestrator on the unit's head `b15f1d47`
(PR #720, based on main `0e3a899f`, fork pin `43fcd243`), before any read-performance unit
merged. It is the "before" of every unit of the
[read-performance slate](../../task/roadmap/mid-term/ice-read-perf-slate-2026-09-18.md); the
re-measure gate (slate unit 4) adds its "after" beside it. The AWS rows arrive with the
dispatch-only AWS leg.

Bench: [crates/repark-spark/benches/ice_read_perf/](../../crates/repark-spark/benches/ice_read_perf/map.md)
(queries, modes, metrics and the R-3 size flag are defined there, not here).

pins: ice-read-perf-0/C-006

## Machine and profile (H-3)

From the `environment` object of each run's JSON.

| key | value |
|---|---|
| git head (dirty?) | `b15f1d47` (clean) |
| fork pin | `43fcd243` (RP-34) |
| profile | default release (`cargo bench`) |
| cpu model / count | AMD Ryzen Threadripper 3970X 32-Core Processor / 26 (the box's `repark.slice` cap) |
| kernel | 6.8.0-139-generic |
| load average (start of each run) | cold 30.37 28.14 22.34; warm 30.34 28.17 22.38; concurrent 31.43 28.43 22.50; concurrent-cold 30.12 28.21 22.46 |
| date (UTC) | 2026-09-19 11:15, 11:15, 11:16, 11:16 |
| rustc version | rustc 1.96.0 (ac68faa20 2026-05-25) |
| cold kind / OS page cache | `new_session_same_process` / `not_dropped` |
| repeats | `--repeat 5` (timings and RSS are medians; I/O is sample 1's, checked equal) |

## The bed

| key | value |
|---|---|
| command | `cargo bench -p repark-spark --bench ice_read_perf -- setup --warehouse <dir>` |
| files / rows | 200 / 10,000,000 |
| bytes (R-3 sum) | 1,428,856,966 |
| pages per column (first file) | 1 row group; id 3, ts 3, category 3, value 3, payload 10 |
| setup seconds | 268 (under the load above) |

## Results

Paste each run's markdown table. The columns are defined in the bench map: medians over the
samples, footer and page split, execute-to-first-batch beside first-batch-from-SQL-start, RSS
at reset beside the peak. Record any `IO-MISMATCH` line verbatim. Record `cpu_count`: the
footer cells depend on it (the scan re-pack splits files by the CPU count; see the unit ledger).
Commands: `run --mode <mode> --warehouse <dir> --repeat 5 --out <mode>.json`.

### cold

| query | n | plan ms | exec→first ms | first batch ms | total ms | rows | data footer req/bytes | data page req/bytes | delete req/bytes | meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | RSS at reset MiB | peak RSS MiB | io same |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Q1 | 5 | 183.8 | 6.8 | 198.1 | 198.1 | 1 | 200/104857600 | 0/0 | 0/0 | 0 | 1 | 200 | 401/105637162 | 1/0 | 1364 | 1364 | yes |
| Q2 | 5 | 177.1 | 24.5 | 201.1 | 201.1 | 1 | 200/104857600 | 200/22157067 | 0/0 | 0 | 1 | 200 | 601/127794229 | 1/0 | 1364 | 1364 | yes |
| Q3 | 5 | 171.7 | 26.4 | 193.8 | 231.8 | 100000 | 200/104857600 | 200/65501593 | 0/0 | 0 | 1 | 200 | 601/171138755 | 1/0 | 1364 | 1364 | yes |
| Q4 | 5 | 156.6 | 6.2 | 162.0 | 162.0 | 1 | 26/13631488 | 2/1008817 | 0/0 | 0 | 1 | 200 | 229/15419867 | 1/0 | 1364 | 1364 | yes |
| Q5 | 5 | 190.5 | 25.9 | 216.4 | 242.4 | 50000 | 200/104857600 | 200/65501593 | 0/0 | 0 | 1 | 200 | 601/171138755 | 1/0 | 1364 | 1364 | yes |
| Q6 | 5 | 156.2 | 377.3 | 535.0 | 752.8 | 625000 | 200/104857600 | 400/1428284966 | 0/0 | 0 | 1 | 200 | 801/1533922128 | 1/0 | 1364 | 1535 | yes |
| Q7 | 5 | 129.7 | 2.7 | 132.3 | 132.5 | 100000 | 27/14155776 | 4/323875 | 0/0 | 0 | 1 | 200 | 232/15259213 | 1/0 | 1535 | 1535 | yes |

`run_io_total requests=17768 bytes=10708646685`

### warm

| query | n | plan ms | exec→first ms | first batch ms | total ms | rows | data footer req/bytes | data page req/bytes | delete req/bytes | meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | RSS at reset MiB | peak RSS MiB | io same |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Q1 | 5 | 4.2 | 6.5 | 10.3 | 10.3 | 1 | 200/104857600 | 0/0 | 0/0 | 0 | 0 | 0 | 200/104857600 | 1/0 | 127 | 128 | yes |
| Q2 | 5 | 5.3 | 24.9 | 30.0 | 30.0 | 1 | 200/104857600 | 200/22157067 | 0/0 | 0 | 0 | 0 | 400/127014667 | 1/0 | 146 | 146 | yes |
| Q3 | 5 | 5.8 | 31.6 | 37.4 | 81.9 | 100000 | 200/104857600 | 200/65501593 | 0/0 | 0 | 0 | 0 | 400/170359193 | 1/0 | 130 | 150 | yes |
| Q4 | 5 | 4.8 | 9.2 | 13.9 | 14.5 | 1 | 26/13631488 | 2/1008817 | 0/0 | 0 | 0 | 0 | 28/14640305 | 1/0 | 134 | 135 | yes |
| Q5 | 5 | 6.5 | 27.3 | 33.8 | 68.1 | 50000 | 200/104857600 | 200/65501593 | 0/0 | 0 | 0 | 0 | 400/170359193 | 1/0 | 164 | 192 | yes |
| Q6 | 5 | 5.2 | 409.3 | 415.0 | 637.0 | 625000 | 200/104857600 | 400/1428284966 | 0/0 | 0 | 0 | 0 | 600/1533142566 | 1/0 | 1589 | 1653 | yes |
| Q7 | 5 | 4.4 | 2.8 | 7.3 | 7.5 | 100000 | 27/14155776 | 4/323875 | 0/0 | 0 | 0 | 0 | 31/14479651 | 1/0 | 1745 | 1745 | yes |

`run_io_total requests=12757 bytes=12810831980`

### concurrent (Q2, Q3, Q5, Q6 at once after a warm-up; I/O per round)

| query | n | plan ms | exec→first ms | first batch ms | total ms | rows | data footer req/bytes | data page req/bytes | delete req/bytes | meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | RSS at reset MiB | peak RSS MiB | io same |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Q2 | 5 | 13.7 | 225.9 | 239.6 | 239.6 | 1 | (group) | (group) | - | - | - | - | - | - | - | - | yes |
| Q3 | 5 | 10.8 | 96.2 | 106.6 | 237.1 | 100000 | (group) | (group) | - | - | - | - | - | - | - | - | yes |
| Q5 | 5 | 9.1 | 96.5 | 105.4 | 223.1 | 50000 | (group) | (group) | - | - | - | - | - | - | - | - | yes |
| Q6 | 5 | 9.3 | 481.1 | 490.4 | 706.3 | 625000 | (group) | (group) | - | - | - | - | - | - | - | - | yes |
| concurrent | 5 | 0.0 | - | - | 714.5 | 775001 | 800/419430400 | 1000/1581445219 | 0/0 | 0 | 0 | 0 | 1800/2000875619 | 4/0 | 1967 | 2046 | yes |

`run_io_total requests=11203 bytes=12006966644`

### concurrent-cold (Q2, Q3, Q5, Q6 at once on a fresh session, no warm-up; I/O per round)

| query | n | plan ms | exec→first ms | first batch ms | total ms | rows | data footer req/bytes | data page req/bytes | delete req/bytes | meta json req | manifest-list req | manifest req | total req/bytes | cache hit/miss | RSS at reset MiB | peak RSS MiB | io same |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Q2 | 5 | 58.5 | 198.3 | 264.9 | 264.9 | 1 | (group) | (group) | - | - | - | - | - | - | - | - | yes |
| Q3 | 5 | 54.6 | 85.2 | 135.5 | 298.4 | 100000 | (group) | (group) | - | - | - | - | - | - | - | - | yes |
| Q5 | 5 | 53.4 | 110.4 | 167.8 | 294.0 | 50000 | (group) | (group) | - | - | - | - | - | - | - | - | yes |
| Q6 | 5 | 51.9 | 506.8 | 562.7 | 784.0 | 625000 | (group) | (group) | - | - | - | - | - | - | - | - | yes |
| concurrent | 5 | 0.0 | - | - | 793.6 | 775001 | 800/419430400 | 1000/1581445219 | 0/0 | 0 | 1 | 200 | 2001/2001655181 | 4/0 | 1664 | 1826 | yes |

`run_io_total requests=10413 bytes=10010757865`

## Reading this table

- **Timings are a busy-box reference, not the before of a timing claim.** Four orchestrators
  shared the machine (load average about 30 on 26 cores). Every unit's before/after pair re-runs
  this bench on the baseline head and on the unit head back to back, in one window, and reports
  both. Requests and bytes do not depend on load; they were identical across the five samples of
  every query.
- **C-9, `count(*)`:** Q1 reads one 512 KiB footer per data file (200 requests, 104,857,600 B)
  although the scan reports `Rows=Exact(10000000)`. Unit ICE-COUNT-FOLD-1 takes it.
- **Q3 against Q7:** the same 1 % time window. Q3, spelled `ts >= CAST(n AS TIMESTAMP)`, reaches
  the scan with no predicate and reads every file (200 footers + 200 page reads). Q7 reaches it
  with one and reads 27 footers + 4 page reads. The gap is fork ask F-TS-PUSHDOWN-1.
- **Pages per file:** three data pages per column at 50,000 rows per file, so within-file page
  pruning (ICE-PAGE-PRUNE-1) has little to skip on this bed. That unit's pair adds a
  large-file bed (`setup --files 20 --rows-per-file 500000`) and records it here beside this one.

## RP-36 pair (fork `7bd2fea3` → `fa77fb2b`: F-TS-PUSHDOWN-1 #312, F-PAGE-PRUNE-1 #310)

Both heads built and run back to back on 2026-09-19 (11:02–11:14 EDT, load average 12–25),
default release profile, `--repeat 5` (medians; requests and bytes identical across samples).
Beds: the 200-file bed above (4 pages per narrow column) and a large-file bed
(`setup --files 20 --rows-per-file 500000`, 1,427,929,166 B, 25 pages per narrow column).
Ledger: [rp-36-fork-pin](../../task/ledgers/staging/rp-36-fork-pin-ledger.md).

| bed, mode | query | total ms | footer requests | page bytes | all bytes |
|---|---|---|---|---|---|
| default-cold | Q1 | 200.8 → 195.1 | 200 → 200 | 0 → 0 | 105,637,162 → 105,637,162 |
| default-cold | Q2 | 228.8 → 220.6 | 200 → 200 | 22,157,067 → 22,157,067 | 127,794,229 → 127,794,229 |
| default-cold | Q3 | 234.8 → 189.0 | 200 → 27 | 65,501,593 → 985,929 | 171,138,755 → 15,921,267 |
| default-cold | Q4 | 175.4 → 176.7 | 26 → 26 | 1,008,817 → 1,008,817 | 15,419,867 → 15,419,867 |
| default-cold | Q5 | 236.6 → 239.8 | 200 → 200 | 65,501,593 → 65,501,593 | 171,138,755 → 171,138,755 |
| default-cold | Q6 | 833.7 → 999.7 | 200 → 200 | 1,428,284,966 → 1,428,284,966 | 1,533,922,128 → 1,533,922,128 |
| default-cold | Q7 | 171.5 → 154.9 | 27 → 27 | 323,875 → 323,875 | 15,259,213 → 15,259,213 |
| default-warm | Q1 | 11.0 → 11.1 | 200 → 200 | 0 → 0 | 104,857,600 → 104,857,600 |
| default-warm | Q2 | 33.5 → 29.9 | 200 → 200 | 22,157,067 → 22,157,067 | 127,014,667 → 127,014,667 |
| default-warm | Q3 | 84.7 → 11.8 | 200 → 27 | 65,501,593 → 985,929 | 170,359,193 → 15,141,705 |
| default-warm | Q4 | 14.0 → 13.9 | 26 → 26 | 1,008,817 → 1,008,817 | 14,640,305 → 14,640,305 |
| default-warm | Q5 | 61.6 → 56.1 | 200 → 200 | 65,501,593 → 65,501,593 | 170,359,193 → 170,359,193 |
| default-warm | Q6 | 659.3 → 776.4 | 200 → 200 | 1,428,284,966 → 1,428,284,966 | 1,533,142,566 → 1,533,142,566 |
| default-warm | Q7 | 8.3 → 7.9 | 27 → 27 | 323,875 → 323,875 | 14,479,651 → 14,479,651 |
| large-cold | Q1 | 33.7 → 30.1 | 20 → 20 | 0 → 0 | 10,565,325 → 10,565,325 |
| large-cold | Q2 | 51.1 → 42.3 | 40 → 40 | 21,271,973 → 21,271,973 | 42,323,058 → 42,323,058 |
| large-cold | Q3 | 93.8 → 42.5 | 40 → 26 | 64,603,029 → 2,335,277 | 85,654,114 → 16,046,330 |
| large-cold | Q4 | 36.6 → 33.2 | 26 → 26 | 2,542,390 → 2,114,116 | 16,253,443 → 15,825,169 |
| large-cold | Q5 | 79.1 → 85.4 | 40 → 40 | 31,486,183 → 31,486,183 | 52,537,268 → 52,537,268 |
| large-cold | Q6 | 1226.1 → 1187.7 | 40 → 40 | 1,373,195,126 → 1,373,195,126 | 1,394,246,211 → 1,394,246,211 |
| large-cold | Q7 | 42.5 → 34.9 | 26 → 26 | 1,196,696 → 1,094,733 | 14,907,749 → 14,805,786 |
| large-warm | Q1 | 8.0 → 6.8 | 20 → 20 | 0 → 0 | 10,485,760 → 10,485,760 |
| large-warm | Q2 | 19.1 → 27.6 | 40 → 40 | 21,271,973 → 21,271,973 | 42,243,493 → 42,243,493 |
| large-warm | Q3 | 55.9 → 21.4 | 40 → 26 | 64,603,029 → 2,335,277 | 85,574,549 → 15,966,765 |
| large-warm | Q4 | 21.7 → 13.2 | 26 → 26 | 2,542,390 → 2,114,116 | 16,173,878 → 15,745,604 |
| large-warm | Q5 | 62.1 → 54.6 | 40 → 40 | 31,486,183 → 31,486,183 | 52,457,703 → 52,457,703 |
| large-warm | Q6 | 1174.4 → 1233.1 | 40 → 40 | 1,373,195,126 → 1,373,195,126 | 1,394,166,646 → 1,394,166,646 |
| large-warm | Q7 | 19.1 → 12.7 | 26 → 26 | 1,196,696 → 1,094,733 | 14,828,184 → 14,726,221 |

- **Q3** (the `CAST(n AS TIMESTAMP)` window) now reaches the scan: 200 → 27 footers on the
  200-file bed and 65.5 MB → 0.99 MB of pages; warm 84.7 → 11.8 ms.
- **Page selection** trims page bytes where a file's pages differ on the filter column (Q4, Q7 on
  the large bed); `value` (Q5) and `category` (Q6) are spread through every page, so nothing is
  skipped there.
- **Q6 regression** on the 200-file bed: the same bytes, warm +18 %, cold +20 % — the cost of
  loading and evaluating page indexes that cannot prune. Fork ask F-PAGE-PRUNE-2.

