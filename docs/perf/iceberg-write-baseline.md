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
