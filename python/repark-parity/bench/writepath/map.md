# map — python/repark-parity/bench/writepath

## Purpose

The tracked probes behind [docs/perf/iceberg-write-baseline.md](../../../../docs/perf/iceberg-write-baseline.md).
Round 2 of PERF-ICE-WRITEPATH-1 cited these from an untracked `scratch/` directory, so the numbers
in the baseline could not be re-derived from the tree; the round-2 critic filed that, and this
directory is the answer. Every probe refuses to run against a debug module.

## Contents

- [gen_bed.py](gen_bed.py) — the 1e6-row seven-column bed (seed 42, zstd, 1e5-row row groups),
  the fixture the analysis' `iceberg_write/1000000/*` cells are measured on. Not committed as data:
  the script writes it.
- [probe_cell.py](probe_cell.py) — one cell in one process with a fresh temp warehouse:
  `ctas`, `ctas_partitioned8` or `df_write_parquet_zstd`, one warm-up then N timed statements
  against a fresh table each, printing a JSON row with samples, median, min, spread, the 1-minute
  load at start and end, the resulting data-file count and, since WRITE-DISTRIBUTION-1
  (2026-09-06), the process's RSS peak (`max_rss_kb`).
- [run_cells.sh](run_cells.sh) — three passes of the three cells for one build label, then the
  minimum, the per-pass medians and the floor (the spread of the pass medians).
- [probe_grouping.py](probe_grouping.py) — the round-3 refutation: N v3 CTAS over eight UNEQUAL
  source files at a given `target_partitions`, printing the manifest record-count sequence, a hash
  of the `id`-to-`_row_id` map, and how many distinct ones appeared. This is what showed the file
  GROUPING is not a function of the statement.
- [probe_invariant.py](probe_invariant.py) — the same fixture, asserting instead what survives any
  grouping: the manifest ascends by content, `_row_id` tiles it contiguously, the row set and sums
  are invariant. The shape `test_perf_ice_writepath_1.py`'s ordering pin took.

## Pointers

- Up: [../map.md](../map.md)
- Numbers: [docs/perf/iceberg-write-baseline.md](../../../../docs/perf/iceberg-write-baseline.md)
