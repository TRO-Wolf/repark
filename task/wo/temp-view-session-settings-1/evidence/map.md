# task/wo/temp-view-session-settings-1/evidence/ — probes, raw cells, jar readings, provenance

- [probes/](probes/map.md) — the cell definitions, the runners and the analysis (comment-free Python).
- [raw/](raw/map.md) — one JSON object per cell and engine, the verdicts, the headline, the summary, the plans.
- [jar/](jar/map.md) — the `javap -c -p` excerpts of Spark 4.1.2's view-capture classes.

## Provenance

- **RePark:** main's package `pkg-dct-7bf32859` and #1355's package `pkg-avg-27d6149f` (built from `27d6149f`), both in
  the orchestrator's other scratchpad (read only). `git diff --name-only 7bf32859 c22d61a1 -- crates python/repark
  Cargo.toml Cargo.lock` lists only `map.md` files (checked in this step). Run by the `lane-nan-write-canonical-1`
  interpreter with the package on `PYTHONPATH`, never with `-I`; every run printed `repark._native.__file__`, and
  every raw file's provenance row names `<scratchpad>/ddu-work/<package>/repark/_native.abi3.so`. Version `1.5.6`.
  #1355's package ran the `avg` body only (`run.py … avg`).
- **Spark:** PySpark 4.1.2 (the c26 oracle environment), Zulu JDK 17, `local[1]`, UI off, shuffle partitions 1, a
  scratch warehouse, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` passed as `spark.jars` only (read, never an output
  path; its sha256 was the same before and after the runs), a hadoop catalog `lc` and an Iceberg in-memory catalog
  `mc` (RePark: `register_memory_catalog("mc", …)`).
- **Jar readings:** classes unzipped from the oracle environment's `spark-catalyst_2.13-4.1.2.jar` and
  `spark-sql_2.13-4.1.2.jar` into a scratch directory (the jars only read), then `javap -c -p`.
- **When:** 2026-11-17. Every probe ran under the shared measure lock on cores 48–57 with a 48 GiB address-space cap
  and `TZ=UTC`, every Spark JVM also under the Spark slot lock, one command at a time. After the probes were
  reformatted and lint-fixed (default-argument binding of loop values, `pathlib` I/O), the five `run.py` runs were
  repeated: every cell's answer or error condition matched the first pass (0 differences in 7,151 cells), and the
  committed raw files are the second pass. `explain.py` and `run_cs.py` outputs are the first pass (their later edits
  were formatting and `pathlib` only). `compact.py` drops the trailing partial word of error texts it cut at 260
  characters.
- **Data:** each engine writes its own fixtures from literals to Parquet (`fx`, `fpw`, `tzt`, `fmap`, `fmk`, `cs`,
  `fdec`; `cs2` under `caseSensitive=true`) and reads them back as DataFrame temp views; Iceberg tables `mc.ns.fx`,
  `mc.ns.tzt`, `lc.ns1.t1`, `lc.ns2.t1`. Timestamps are created under `UTC`; no answer collects a raw timestamp.
- **Commands** (from `probes/`, `<wh>` a scratch warehouse parent, `<jar>` the Iceberg runtime jar, `<out>` a scratch
  directory):
  - `run.py <spark|repark> true <wh> <jar> <out>/<spark|main>-true.jsonl` and, on #1355's package,
    `run.py repark true <wh> <jar> <out>/head-avg-true.jsonl avg`;
  - `run.py <spark|repark> unset <wh> <jar> <out>/<spark|main>-unset.jsonl`;
  - `explain.py <spark|repark> <wh> <jar> <out>/explain-<spark|main>.txt`;
  - `run_cs.py <spark|repark> <wh> <jar> <out>/cs-<spark|main>.json`;
  - `compact.py <out> ../raw`, then `analyze.py ../raw ../raw`.

Up: [../map.md](../map.md).
