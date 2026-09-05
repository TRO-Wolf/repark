# Iceberg write baseline — PERF-ICE-WRITEPATH-1

The write cells of [engine-iceberg-analysis-2026-09-04.md](engine-iceberg-analysis-2026-09-04.md)
§7.5, re-measured before and after this unit, on both fork builds. Candidates 7
(`PERF-ICE-FANOUT-1`, the partitioned fanout's per-row `Literal::Struct`) and 8
(`PERF-ICE-WRITEPAR-1`, unpartitioned writers that were cooperative futures in one task) are
measured against the same CTAS pair, so they share one page.

Two runs from different hours are not one table. Every table below records the build it came
from, the fixture, the iteration count and the 1-minute load at the start and end of each cell;
a cost is read against the floor of the run it came from.

## 1. Builds

| build | RePark tree | iceberg fork | what it isolates |
|---|---|---|---|
| B0 | `origin/main` | pinned `189a73ed` | the state PERF-ANALYSIS-1 measured |
| B1 | `origin/main` | path override on `f-28-vectorized-partition-splitter` | the fork half alone |
| B2 | `perf/ice-writepath-1` | path override on `f-28-vectorized-partition-splitter` | both halves |

The override is a temporary `[patch.crates-io]` `path =` rewrite of the five `iceberg*` entries.
It is never committed: `git diff origin/main -- Cargo.toml Cargo.lock` is empty at hand-back, and
the pin moves only through the fork-sync procedure ([../fork-sync.md](../fork-sync.md)) after the
fork change lands.

## 2. Method

Release module only (`repark._native.__debug_assertions__ is False`; the probe refuses to run
otherwise), `spark.sql.shuffle.partitions = 8`, memory catalog on the local filesystem, fresh
table per iteration, 5 timed iterations after 1 warm-up, median reported with min and spread.

## 3. Commands

```bash
lane=$HOME/repark-lanes/lanes/oc-writepath
fork=$HOME/repark-lanes/lanes/writepath-fork
cd "$lane" && .venv/bin/python scratch/probes/gen_bed.py 1e6 scratch/synth_1000000.parquet
# per build: point the venv's editable install at THIS tree, then measure
cd "$lane/python/repark" && VIRTUAL_ENV="$lane/.venv" CARGO_BUILD_JOBS=8 \
  "$lane/.venv/bin/maturin" develop --release
cd "$lane" && .venv/bin/python scratch/probes/probe_write.py scratch/synth_1000000.parquet <build>
# the fork override, on and off
bash scratch/probes/fork_override.sh on
bash scratch/probes/fork_override.sh off
# the fork half in isolation, in the fork lane
cd "$fork" && cargo test --release -p iceberg --lib arrow::record_batch_partition_splitter
```

The probe sources live under `scratch/probes/` and are excluded from git
(`.git/info/exclude`); they carry no comments.

## 4. The fork half in isolation (F-28)

Measured in the fork lane, `cargo test --release -p iceberg`, so no RePark rebuild and no disk is
involved: 16 batches of 65,536 rows (1,048,576 rows) over the analysis' seven-column bed, an
identity partition on `part` with eight values, both implementations run back to back six times
in one process on the same batches.

| splitter | median (ms) | min (ms) | samples (ms) |
|---|---:|---:|---|
| row-wise — one `Literal::Struct` per row, `HashMap<&Struct, _>`, a `BooleanArray` per group | **171.39** | 161.85 | 161.85, 162.68, 171.24, 171.39, 192.63, 200.30 |
| Arrow kernels — `lexsort_to_indices` + `arrow_ord::partition`, one literal per group, `take` | **28.33** | 27.41 | 27.41, 27.74, 28.00, 28.33, 28.36, 36.21 |

**6.0×, and 143 ms saved per 1e6 rows.** The same pair on a three-column batch is 172.34 → 17.98
(9.6×): the row-wise cost is per row and does not care how wide the batch is, while the group
materialization does, so a wider batch narrows the ratio and widens neither cost much.

This corrects the analysis' arithmetic, and it is the finding that matters more than the number:
PERF-ANALYSIS-1 read **813 ms** off the partitioned-minus-unpartitioned CTAS delta and proposed
the splitter as its cause. The splitter's whole isolated cost at that scale is 171 ms. The rest of
that delta is the fanout itself — a partitioned CTAS writes 8× the data files of an unpartitioned
one (32 against 4 before this unit, 64 against 8 after), each with its own parquet footer and
`fsync`. **The fork half cannot deliver the brief's "≥ 600 ms" target because that target was
built on attributing the whole delta to the splitter.** A unit that wants the rest of the delta
has to reduce the file count — Spark's answer is `write.distribution-mode = hash`, which sends one
partition value to one task, and RePark has no such rule.

## 5. The RePark cells

Per-cell isolated probe (`scratch/probes/probe_cell.py`): one process per cell, a fresh temp
warehouse per process, one warm-up then 5 timed statements with a fresh table each, three passes
per cell per build. Reported as the minimum across all 15 samples and the median of each pass,
because **the box was not quiet**: sibling lanes ran `rustc` builds throughout (1-minute load 13
to 22, `vmstat` 82-87 % idle CPU but 13-22 kB/s block-in and 54-86 kB/s block-out), and a
partitioned CTAS at this scale is `fsync`-bound. Under that contention a median is a measurement
of the neighbours; the minimum is the closest thing to the engine.

| cell | B0 min | B0 pass medians | B3 min | B3 pass medians | B2 min | B2 pass medians |
|---|---:|---|---:|---|---:|---|
| `ctas` | **880.50** | 2208.89, 1839.90, 886.91 | 735.42 | 861.65, 1372.83, 1356.68 | **478.03** | 691.20, 853.11, 1146.40 · 1548.89, 2678.87, 2406.24 |
| `ctas_partitioned8` | **1611.25** | 9241.14, 2078.69, 7163.95 | 2213.01 | 4111.71, 2930.59, 4248.53 | **917.22** | 2821.71, 4023.67, 3002.39 · 3702.49, 5725.12, 6193.68 |
| `df_write_parquet_zstd` (control) | 143.90 | 259.79, 168.87, 200.14 | 89.98 | 159.24, 200.63, 210.42 | 100.73 | 201.00, 200.74, 180.58 · 125.84, 195.45, 240.10 |
| 1-minute load at each pass | | 16.4, 21.0, 21.7 | | 14.6, 14.0, 13.5 | | 18.6, 18.7, 18.8 · 19.2, 17.1, 19.0 |

B2 was measured twice; both pass sets are listed and the minimum is taken across both.

| cell | B0 → B2 (min) | ratio |
|---|---|---:|
| `ctas` | 880.50 → 478.03 ms | **1.84×** |
| `ctas_partitioned8` | 1611.25 → 917.22 ms | **1.76×** |
| `ctas` against the parquet sink measured in the same build | 6.12× → 3.65× | |
| `ctas_partitioned8` against the same control | 11.20× → 7.01× | |

**What these walls do not show.** B3 (the RePark half alone) does not sit between B0 and B2 on
either cell — it reads slower than B0 on `ctas_partitioned8` and slower than B2 on `ctas`, where
the splitter cannot matter at all. The three builds could not be measured in one window (each is
an 8-minute release rebuild of a 163 MB module) and the disk contention moved more than the
change did. So this table supports the direction and the size of the end-to-end gain and
**nothing about which half produced it**; the fork half is resolved by §4 instead, where the
measurement needs no disk. The analysis' targets (`ctas` ≤ 150 ms, `ctas_partitioned8` ≤ 300 ms)
are not met on this box: the `df.write.parquet(zstd)` control itself reads 90 to 144 ms here, so
those targets ask for parity with the DataFusion sink, and the engine is at 3.65× of it.

## 6. Layout

| build | `ctas` data files | `ctas_partitioned8` data files |
|---|---:|---:|
| B0 | 4 | 32 |
| B2 / B3 | 8 | 64 |

One data file per DataFusion partition (8 at `spark.sql.shuffle.partitions = 8`) replaces the four
round-robin writers, and a partitioned write multiplies that by its partition values. Row set,
`sum(id)` and `sum(vi)` are identical on every build (1,000,000 rows, 499,999,500,000,
499,596,708). `repark.write.max-concurrent-files = 1` still writes exactly one file.
