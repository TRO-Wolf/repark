# task/wo/cast-literal-unwrap-1/evidence/ — probes, raw cells, provenance

- [probes/](probes/map.md) — the cell definitions, the runners and the analysis (comment-free Python).
- [built/](built/map.md) — the build's bar and cost (head `88999454`).
- [raw/](raw/map.md) — one JSON object per cell and engine, the verdicts, the summary tables, the `EXPLAIN` side
  observation on both engines, and the DataFusion 54.1.0 / 55.2.0 diff.

## Provenance

- **RePark:** the package `pkg-ddu1-7b21bde3` in the orchestrator's scratchpad (read only), built from
  `7b21bde3`, whose product tree equals main `83df6be1` (the orchestrator checked that `git diff a06c1088 83df6be1`
  is empty for `crates/`, `python/repark`, `Cargo.*` and `pyproject.toml`). Run by the
  `lane-nan-write-canonical-1` interpreter with the package on `PYTHONPATH`, never with `-I`; every run printed
  `repark._native.__file__`, and every raw file's provenance row names `<scratchpad>/pkg-ddu1-7b21bde3/repark/_native.abi3.so`.
  Version `1.5.6`.
- **Spark:** PySpark 4.1.2 (the c26 oracle environment), Zulu JDK 17, `local[1]`, UI off, session zone UTC, shuffle
  partitions 1, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` with its SQL extensions, a private hadoop catalog
  `lc` and warehouse per run.
- **When:** 2026-11-16, about 14:55–15:35 UTC (09:55–10:35 local), beside PR #1341's delta round. Every probe ran
  under the shared measure lock on cores 48–57 with a 48 GiB address-space cap, and every Spark JVM under the Spark
  lock, one command at a time.
- **Box zone:** the box's local zone, which is not UTC. Every session sets `spark.sql.session.timeZone` to UTC; see the DataFrame
  datetime note in [../domain-table.md](../domain-table.md#exclusions).
- **Commands** (from `probes/`, `<out>` a scratch directory, `<wh>` a scratch warehouse parent):
  - `run_cells.py repark <false|true> <wh> <jar> <out>/repark-<ansi>.jsonl`;
  - `run_cells.py spark <false|true> <wh> <jar> <out>/spark-<ansi>.jsonl`;
  - `run_native.py <out>/native.jsonl <the repark-false warehouse>`;
  - `run_held.py <spark|repark> <false|true> <out>/held-<engine>-<ansi>.jsonl`;
  - `explain_side.py <spark|repark> <wh>`, output kept as `explain-side-<engine>.txt`;
  - `compact.py <out> ../raw`, then `analyze.py ../raw ../raw`.

Up: [../map.md](../map.md).
