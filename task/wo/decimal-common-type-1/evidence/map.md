# task/wo/decimal-common-type-1/evidence/ — probes, raw cells, provenance

- [probes/](probes/map.md) — the cell definitions, the runners and the analysis (comment-free Python).
- [raw/](raw/map.md) — one JSON object per cell and engine, the verdicts, the summary and the grids.
- [built/](built/map.md) — the build's bar and cost (head `7bf32859`), their probes and raw data.

## Provenance

- **RePark:** the package `pkg-cu-88999454` in the orchestrator's other scratchpad (read only), built from
  `88999454`, whose product tree equals main `e50ed17f` (the orchestrator checked the diff is empty; this step
  re-checked that `git diff 88999454 e50ed17f` touches only `map.md` files under `crates/` and `python/repark`). Run
  by the `lane-nan-write-canonical-1` interpreter with the package on `PYTHONPATH`, never with `-I`; every run
  printed `repark._native.__file__`, and every raw file's provenance row names
  `<scratchpad>/ddu-work/pkg-cu-88999454/repark/_native.abi3.so`. Version `1.5.6`.
- **Spark:** PySpark 4.1.2 (the c26 oracle environment), Zulu JDK 17, `local[1]`, UI off, session zone UTC,
  shuffle partitions 1, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` passed as `spark.jars` only (read, never an
  output path) with its SQL extensions, a private hadoop catalog `lc` and warehouse per run.
- **When:** 2026-11-16, about 21:20–21:30 UTC. Every probe ran under the shared measure lock on cores 48–57 with a
  48 GiB address-space cap, and every Spark JVM under the Spark lock, one command at a time.
- **Data:** written by each engine from string literals cast to each column's type, then read back from Parquet, so
  no decimal common type is involved in the setup; the box's zone does not matter (no timestamps).
- **Commands** (from `probes/`, `<out>` a scratch directory, `<wh>` a scratch warehouse parent, `<jar>` the
  Iceberg runtime jar):
  - `run_cells.py <spark|repark> <false|true> <wh> <jar> <out>/<engine>-<ansi>.jsonl`;
  - `run_native.py <out>/native.jsonl`;
  - `run_side.py <spark|repark> <false|true> <wh> <jar> <out>/side-<engine>-<ansi>.jsonl`;
  - `explain_union.py <out>/explain-union-repark.txt <wh>`;
  - after the inventory critic, about 22:00–22:10 UTC: `run_revision.py <spark|repark> <false|true> <wh> <jar>
    <out>/revision-<engine>-<ansi>.jsonl`;
  - `compact.py <out> ../raw`, then `analyze.py ../raw ../raw` and `grids.py ../raw`.

Up: [../map.md](../map.md).
