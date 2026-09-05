# map — python/repark-parity/bench/icescan

## Purpose

The tracked probes behind [docs/perf/iceberg-scan-baseline.md](../../../../docs/perf/iceberg-scan-baseline.md).
Every probe refuses to run against a debug module or a module from another tree.

## Contents

- [gen_bed.py](gen_bed.py) — the read bed both builds measure: eight-file zstd seeds at 1e6
  (1e5-row row groups) and 1e7 (1e6-row row groups) over the analysis seven-column shape,
  then CTAS `t_plain`, partitioned `t_part`, `t_plain7`, and a V3 MoR `t_dv` with 1% of rows
  deleted. Prints every table's file layout and count. Not committed as data: the script
  writes it.
- [run_cells.py](run_cells.py) — builds the bed in-process through `gen_bed.build`
  (the memory catalog is process-local, so no bed survives between runs; the zstd seeds
  persist and are reused), then one warm-up plus five timed runs of each §7.4 read cell
  (`count_star`, `count_id`, `sum_all`, `string_len` over `t_plain`, `t_plain7` and the
  parquet controls, plus the DV count and sum), writing a JSON row per cell with samples,
  median, min, spread, the 1-minute load at start and end, the answer, and the physical
  plan's scan count.

## Pointers

- The baseline that quotes these cells:
  [docs/perf/iceberg-scan-baseline.md](../../../../docs/perf/iceberg-scan-baseline.md).
- The analysis they re-run:
  [docs/perf/engine-iceberg-analysis-2026-09-04.md](../../../../docs/perf/engine-iceberg-analysis-2026-09-04.md)
  §7.4.
