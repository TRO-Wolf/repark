# Iceberg scan baseline (PERF-ICE-SCAN-1)

Measured 2026-09-05 on the lane `$HOME/repark-lanes/lanes/oc-icescan` (branch
`perf/ice-scan-1`, base `origin/main` `8f40ce46`). Cites
[engine-iceberg-analysis-2026-09-04.md](engine-iceberg-analysis-2026-09-04.md) §2 rows 4 and 5,
§5 items 4 and 5, §6 and §7.4, and re-runs the `count_star`, `count_id`, `sum_all` and
`string_len` shapes before and after.

pins: perf-ice-scan-1/C-008a, C-008b

## Machine and profile

| key | value |
|---|---|
| cpu | AMD Ryzen Threadripper 3970X, 64 threads |
| ram | 125 GiB · governor `schedutil` · kernel 6.8.0-138-generic |
| native | `_native.abi3.so` 164,057,152 B before, 164,075,704 B after, `__debug_assertions__ is False` (every probe refuses otherwise) |
| build | `CARGO_BUILD_JOBS=8 maturin develop --release` |
| repark / DataFusion / arrow | 1.0.1 / 54.1.0 / 58.4.0 |
| fork pin | `79119643` (unchanged; the after leg names its temporary path override in §5) |
| threads | `spark.sql.shuffle.partitions = 8` |
| iterations | 5 timed after 1 warm-up; median, spread per cell; 1-minute load at start and end |

**Not a quiet box.** Every timing run below carries its own 1-minute load and its own re-measured
parquet floor, and a cost is only ever read against the floor of the run it came from. No sibling
`cargo` build was live during the timed runs (load 6.8–8.3).

## 1. The bed

[gen_bed.py](../../python/repark-parity/bench/icescan/gen_bed.py) writes two fixed eight-file zstd
seeds over the analysis seven-column shape (`id`, `ts`, `v`, `vi`, `s`, `cat`, `part`): 1e6 rows
in 1e5-row row groups, 1e7 rows in 1e6-row row groups. Each measurement run then CTASes, in its
own process, `t_plain` (1e6), `t_part` (1e6, partitioned by `part`), `t_plain7` (1e7), and a V3
MoR `t_dv` (1e6 rows, 1% deleted — 8 data files plus 8 puffin DVs, `count(*)` 990,000). The
memory catalog is process-local, so no bed survives between runs; the seeds persist and are
reused, and every leg rebuilds identical-shape tables (file identities differ, row and byte
counts do not). CTAS files match the seeds byte-shape (1e6: 125,000 rows/file, zstd,
1.27 MB vs 1.29 MB).

## 2. Before and after (ms, median of 5)

`N` is the `IcebergTableScan N=` partition count from the physical plan. `× pq` reads the
after median against the after-run parquet floor. Full samples, loads, answers and plans:
`cells-before.json` / `cells-after.json` beside the bed (untracked — §4 regenerates them).

### 2.1 `t_plain`, 1e6 rows

| cell | before | N | after | N | parquet floor | × pq | analysis §7.4 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `count_star` | 86.5 ± 30.1 | 1 | **2.0** ± 0.6 | 0 (folded) | 1.8 | 1.1× | 93 |
| `count_id` | 30.4 ± 37.8 | 1 | **14.0** ± 3.5 | 8 | 1.7 | 8.3× | 25 |
| `sum_all` | 89.5 ± 34.4 | 1 | **36.2** ± 8.5 | 8 | 19.8 | 1.8× | 80 |
| `string_len` | 48.7 ± 9.0 | 1 | **20.4** ± 4.5 | 8 | 9.2 | 2.2× | 56 |

### 2.2 `t_plain7`, 1e7 rows

| cell | before | N | after | N | parquet floor | × pq | analysis §7.4 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `count_star` | 686.0 ± 37.7 | 1 | **2.5** ± 0.3 | 0 (folded) | 1.8 | 1.4× | 457 |
| `count_id` | 150.5 ± 26.2 | 1 | **52.1** ± 13.3 | 8 | 1.7 | 31× | 100 |
| `sum_all` | 596.5 ± 113.5 | 1 | **161.9** ± 9.3 | 8 | 66.1 | 2.4× | 412 |
| `string_len` | 365.0 ± 121.0 | 1 | **109.4** ± 13.9 | 8 | 30.8 | 3.6× | 247 |

### 2.3 `t_dv`, 1e6 rows, V3 MoR, 1% deleted

| cell | before | N | after | N | answer |
|---|---:|---:|---:|---:|---:|
| `count_star` | 73.6 ± 23.0 | 1 | **4.6** ± 1.5 | 1 (no fold; zero-column scan) | 990,000 |
| `sum_all` | 57.6 ± 21.1 | 1 | **19.3** ± 0.9 | 8 | matches the pre-DELETE sums minus the deleted rows |

## 3. Verdict against the targets

**`count(*)` at 1e6 ≤ 5 ms: HIT.** 86.5 ms → 2.0 ms (44×), at parquet parity (1.8 ms).
At 1e7 the fold answers in 2.5 ms against a 686 ms before — the answer no longer depends on
the row count. The `t_dv` leg proves the complementary half: with delete files present the
fold correctly does NOT fire (N=1 scan) and the zero-column batches still cut the count
73.6 ms → 4.6 ms (16×) with the deleted-row-aware answer 990,000.

**`sum_all` / `string_len` within 1.5× of parquet: MISSED**, honestly and with the
decomposition below. The full scans improve 2.5–3.7× (N=1 → N=8) but land at 1.8–3.6× of
the same-run parquet floor:

- Planning is ~1 ms of the gap: EXPLAIN on the 1e6 `sum_all` costs 3.8 ms (Iceberg) vs
  3.0 ms (parquet).
- A fixed ~10 ms per Iceberg query sits outside planning and outside the executors'
  compute metrics (the fork scan emits no execution metrics): the 1e6 `count_id` over one
  8 MB column costs 14.0 ms against a 1.7 ms parquet leg.
- The remainder is per-byte, ~2×: the 1e7 `sum_all` gap (95.8 ms over 10× the bytes of
  the 1e6 gap of 16.5 ms) grows with the bytes, not the files.

**`count(col)` is a pushdown gap, not a decode gap.** The parquet `count_id` leg folds
entirely from statistics (the plan is a bare `ProjectionExec` over `PlaceholderRowExec` —
no scan) in 1.7 ms at both scales; the Iceberg leg scans the column (14.0 / 52.1 ms).
No Iceberg-side statistics pushdown for `count(col)` exists at either pin. Filed as
`PERF-ICE-SCANPART-1` residue, not implemented here.

## 4. Commands

All from the lane root, on a release module (§5 for which fork each build carried):

```sh
.venv/bin/python python/repark-parity/bench/icescan/gen_bed.py ~/repark-lanes/beds/oc-icescan
.venv/bin/python python/repark-parity/bench/icescan/run_cells.py \
  ~/repark-lanes/beds/oc-icescan ~/repark-lanes/beds/oc-icescan/cells-before.json
.venv/bin/python python/repark-parity/bench/icescan/run_cells.py \
  ~/repark-lanes/beds/oc-icescan ~/repark-lanes/beds/oc-icescan/cells-after.json
```

`run_cells.py` rebuilds the CTAS tables in-process (the memory catalog is process-local)
and reuses the persisted seeds. The JSON rows it writes carry samples, median, min,
spread, load at start and end, the answer, the physical plan's scan count, and the plan.

## 5. What the after build carried

The before leg ran the pinned fork (`79119643`, F-26 + F-CATIO). The after leg ran the
same tree with a temporary, never-committed path override of the five `iceberg*` crates
to the fork lane `$HOME/repark-lanes/lanes/icescan-fork` at `c18f15c96` (F-27a `5d45e040b`
+ F-27b `c8a3c922f` + F-27d `c18f15c96`, on fork main `16639b87c`). `git diff
origin/main -- Cargo.toml Cargo.lock` is empty at hand-back; the override script lives at
`scratch/probes/fork_override_scan.sh` (git-excluded) and the pin bump is the
orchestrator's RP-13 step.
