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

| build | RePark tree | iceberg fork | what it isolates | where its numbers may be quoted |
|---|---|---|---|---|
| B0 | `6eaccd5e` (this unit's base) | pinned `189a73ed` | the state before the unit | the before column of `PERF-ICE-WRITEPAR-1` |
| B3 | `perf/ice-writepath-1` (the SHIPPED tree) | pinned `189a73ed` | the RePark half, as it will merge | the after column of `PERF-ICE-WRITEPAR-1` |
| B1 | `6eaccd5e` | path override on `f-28-vectorized-partition-splitter` | the fork half alone | the pending fork row only |
| B2 | `perf/ice-writepath-1` | path override on the same fork branch | both halves | the pending fork row only |

**B1 and B2 carry an uncommitted `[patch.crates-io]` path override and are NOT the shipped tree.**
Round 1 quoted B0 → B2 in the registry; round 2 moved the registry to B0 → B3 and left the
override builds to the fork row, which is where a number that depends on an unlanded fork belongs.
The override is never committed: `git diff origin/main -- Cargo.toml Cargo.lock` is empty at
hand-back, and the pin moves only through the fork-sync procedure
([../fork-sync.md](../fork-sync.md)).

## 2. Method

Release module only (`repark._native.__debug_assertions__ is False`; the probe refuses to run
otherwise), `spark.sql.shuffle.partitions = 8`, memory catalog on the local filesystem, fresh
table per iteration, 5 timed iterations after 1 warm-up, median reported with min and spread.

## 3. Commands

```bash
lane=$HOME/repark-lanes/lanes/oc-writepath
fork=$HOME/repark-lanes/lanes/writepath-fork
cd "$lane" && .venv/bin/python python/repark-parity/bench/writepath/gen_bed.py 1e6 scratch/synth_1000000.parquet
# per build: point the venv's editable install at THIS tree, then measure
cd "$lane/python/repark" && VIRTUAL_ENV="$lane/.venv" CARGO_BUILD_JOBS=8 \
  "$lane/.venv/bin/maturin" develop --release
cd "$lane" && bash python/repark-parity/bench/writepath/run_cells.sh <build>
# the fork override, on and off
# the fork override is a temporary [patch.crates-io] path rewrite, never committed
# the fork half in isolation, in the fork lane
cd "$fork" && cargo test --release -p iceberg --lib arrow::record_batch_partition_splitter
# the grouping refutation and the invariants it left standing
.venv/bin/python python/repark-parity/bench/writepath/probe_grouping.py 10 4
.venv/bin/python python/repark-parity/bench/writepath/probe_invariant.py 10 4
```

The probes are tracked at
[python/repark-parity/bench/writepath/](../../python/repark-parity/bench/writepath/map.md) — round
2 cited them from an untracked `scratch/` directory, which the round-2 critic filed, since a
number nobody can re-derive from the tree is not a baseline.

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

Per-cell isolated probe (`python/repark-parity/bench/writepath/probe_cell.py`): one process per cell, a fresh temp
warehouse per process, one warm-up then 5 timed statements with a fresh table each, three passes
per cell per build. **Round 2 re-measured B0 and B3 back to back on a quiet box** (1-minute load
7.7–14.4, against 13–22 in round 1); the `df.write.parquet(zstd)` control reads within 5 % on both
builds, which is what makes the pair comparable. The floor is the spread of the three pass
medians.

| cell | B0 min | B0 pass medians | B3 min | B3 pass medians | floor B0 / B3 |
|---|---:|---|---:|---|---|
| `ctas` | 1,312.39 | 1,556.88, 1,490.63, **1,384.80** | **127.54** | **135.48**, 150.36, 149.45 | 172.1 / 14.9 |
| `ctas_partitioned8` | 4,628.11 | **4,901.75**, 5,444.76, 5,714.64 | **283.07** | 305.20, 293.92, **293.19** | 812.9 / 12.0 |
| `df_write_parquet_zstd` (control) | 93.96 | 110.08, **107.37**, 110.66 | 98.37 | **105.56**, 118.66, 109.14 | 3.3 / 13.1 |
| 1-minute load per pass | | 8.98, 8.27, 7.98 | | 7.71, 13.66, 14.40 | |

| cell | B0 → B3 (best median) | B0 → B3 (min) | |
|---|---|---|---:|
| `ctas` | 1,384.80 → 135.48 ms | 1,312.39 → 127.54 ms | **10.2×** |
| `ctas_partitioned8` | 4,901.75 → 293.19 ms | 4,628.11 → 283.07 ms | **16.7×** |

**The durable result is the ratio to the control, not the millisecond.** Against the
`df.write.parquet(zstd)` measured in the same passes:

| cell | B0 | B3 (this box) | B3 (round-2 critic's box, load 11.8-12.3) |
|---|---:|---:|---:|
| `ctas` / control | 12.90× | 1.28× | 1.38× |
| `ctas_partitioned8` / control | 45.65× | 2.78× | 3.00× |

The two independent measurements of the shipped tree agree to within 8 % on the ratio and differ
by 28-29 % on the absolute (135.48 / 293.19 ms here at load 7.7-14.4 against 173.80 / 377.04 ms
there at load 11.8-12.3, control 105.56 against 125.73). So **the 10× and 17× gains stand and are
reproduced independently; the analysis' absolute targets (`ctas` ≤ 150 ms,
`ctas_partitioned8` ≤ 300 ms) are met on this box at this load and were NOT met on the critic's**.
Round 3 therefore reports the gain and drops the unqualified "targets met" that round 2 claimed:
on this hardware those targets sit inside the load-induced spread, which makes them a property of
the afternoon rather than of the engine.

## 6. Layout, against Spark

| engine / build | `ctas` data files | `ctas_partitioned8` data files |
|---|---:|---:|
| Spark 4.1.2 (`write.distribution-mode = hash`, the Iceberg default) | 2 | 8 |
| repark B0 | 4 | 32 |
| repark B3 (shipped) | 8 | 64 |
| repark B4 (`perf/write-distribution-1`, WRITE-DISTRIBUTION-1, 2026-09-06) | 8 | **8** |

At `spark.sql.shuffle.partitions = 8` the shipped tree writes **4× Spark's unpartitioned count and
8× its partitioned count**, averaging 328 KB per data file. Spark gets its counts from a
distribution rule that sends one partition value to one task before the write; repark has none.
That gap was filed as **`WRITE-DISTRIBUTION-1`** (fixed in §8, 2026-09-06) with both rejected alternatives measured — capping
the writers below the partition count costs 738 ms against 547 ms and buffers unconsumed
partitions whole, and a round-robin `RepartitionExec` destroys the content ordering §5 establishes. The row set, `sum(id)` and `sum(vi)` are identical on every
build (1,000,000 rows, 499,999,500,000, 499,596,708), and
`repark.write.max-concurrent-files = 1` still writes exactly one file — the key is binary on this
node, measured 1 / 8 / 8 / 8 files at cap 1 / 2 / 4 / 8.

## 7. Determinism — what is true, after three attempts

| ordering | claim | verdict |
|---|---|---|
| round 1 — by writer index | repeated CTAS commit the same manifest and `_row_id` | **REFUTED**: the DataFusion partition index is not stable across executions |
| round 2 — `stable_commit_order`, by content | same claim | **REFUTED** by CI's 4-core runner and reproduced here: the file GROUPING is not stable either |
| round 3 — the same order, a narrower claim | the commit is an ORDERING, not a layout | holds at 3, 4, 8 and 16 partitions |

`stable_commit_order` is a total order on the files it is handed, but **the file SET is not a
function of the statement**: DataFusion packs the same source files into writers differently from
run to run. Measured over eight unequal source files, one process, one registered view:

| `target_partitions` | distinct manifest sequences | distinct `_row_id` maps | runs |
|---:|---:|---:|---:|
| 3 | 3 | 3 | 5 |
| 4 | 4-6 | 4-6 | 10 |
| 8 (one file per partition — round 2's own configuration) | 1 | 1 | 5 |
| 16 | 1 | 1 | 10 |

Round 2's pin was green only because its `shuffle.partitions = 8` met its eight source files
one-to-one. What holds at every count, and is what the pin asserts now: the manifest ascends by
content, `_row_id` tiles it contiguously from zero, the row set and its sums are invariant, and two
runs that produce the same grouping produce the same `_row_id` ranges. Ten consecutive runs at
3/4/8/16 on four cores and ten on all cores: 20 of 20 green. The residual is
**`WRITE-GROUPING-CTAS-1`**, and it is a scan defect: the rows land in different files before any
writer sees them.

## 8. WRITE-DISTRIBUTION-1 — the hash distribution rule (2026-09-06)

`write/distribution.rs::hash_distribution` places a `RepartitionExec` under the CTAS write node,
`Partitioning::Hash` over one `PartitionTransformExpr` per partition field, when the table is
partitioned and there is more than one writer. Build **B4** is `perf/write-distribution-1` on the
pinned fork; **B3'** is `origin/main` `57f21b9b` (B3 as merged, PERF-APPROXPCT-1 and later on
top) with the same release native swapped in for the pair. Same probes, same bed, same method
(§2–§3, `probe_cell.py` now also prints `max_rss_kb`). The box was loaded throughout (1-minute
load 11–17); the control reads within 1 % on both sides, which is what makes the pair
comparable. Every pass median is listed; the floor is the spread of the pass medians.

| cell | B3' min | B3' pass medians | B4 min | B4 pass medians | files B3' / B4 |
|---|---:|---|---:|---|---|
| `ctas` | 284.68 | **337.13**, 337.77, 371.34 | 131.82 | 429.12, **134.07**, 137.26 | 8 / 8 |
| `ctas_partitioned8` | 348.16 | **361.16**, 376.24, 389.88 | 191.38 | 208.93, 206.07, **204.91** | 64 / **8** |
| `df_write_parquet_zstd` (control) | 94.63 | 109.66, 106.12, **104.96** | 100.60 | 117.23, **104.60**, 107.74 | — |
| 1-minute load per pass | | 12.15, 11.42, 15.93 | | 17.24, 16.90, 16.03 | |
| RSS peak, partitioned cell (MB) | | 760, 776, 768 | | 861, 842, 845 | |

| cell | B3' → B4 (best median) | B3' → B4 (min) | ratio to control, B3' → B4 |
|---|---|---|---|
| `ctas_partitioned8` | 361.16 → 204.91 ms (**1.76×**) | 348.16 → 191.38 ms (1.82×) | **3.44× → 1.96×** |
| `ctas` | 337.13 → 134.07 ms | 284.68 → 131.82 ms | 3.21× → 1.28× |

**Read the partitioned row, not the unpartitioned one.** The code path of `ctas` is untouched
(`hash_distribution` returns its input for an unpartitioned table, pinned), so its 337 → 134 is
the box, not the branch: an earlier B3' set on the same native at load 5–9 read `ctas` at
140.24 / 371.12 / 398.22 ms medians (control 95.13 / 106.38 / 104.10) — this cell is
`fsync`-bound here and swings 3× with the disk. Its ratio to control after, 1.28×, is exactly
PERF-ICE-WRITEPAR-1's shipped reading. That earlier set also read `ctas_partitioned8` at
490.57 / 554.95 / 464.99 ms (64 files), so the partitioned gain holds against both before sets.

**What the distribution costs.** RSS peak on the partitioned cell rises 760–785 → 842–861 MB:
the repartition channels hold the batches in flight between the eight input tasks and the eight
writers. Every output partition is consumed concurrently by its own writer task, which is the
case DataFusion's channel gate is built for; the unbounded case PERF-ICE-WRITEPATH-1 measured
(writers below the partition count, unconsumed outputs buffering whole) is not reintroduced.

**Spark.** Live Spark 4.1.2 (`iceberg-spark-runtime-4.1_2.13:1.11.0`, `shuffle.partitions = 8`)
over the facade pin's four-file seed writes the same eight-file layout as B4 — the same
partition value and record count per file (`test_partitioned_ctas_file_count_matches_spark`).
Spark's 2 unpartitioned files are its scan split count for one 29 MB parquet, not a distribution
rule; the unpartitioned cell stays at one file per plan partition (§6), by decision, not by
omission: a coalesce below the partition count is the shape §6 rejected.

**Determinism, as it stands after this unit.** The key is DataFusion's seeded
`REPARTITION_RANDOM_STATE` over the partition values, so the row-to-writer map is a function of
the data and two runs of one plan commit the same `(partition value, record count)` sequence
(pinned). Inside a data file the rows arrive in the order the eight input tasks reach the
channel, so the `_row_id` a given row receives is not reproducible run to run on the partitioned
path; the manifest still ascends by partition value and `_row_id` still tiles it contiguously
from zero (pinned through the v3 facade CTAS). The unpartitioned path keeps §7's claim unchanged.
