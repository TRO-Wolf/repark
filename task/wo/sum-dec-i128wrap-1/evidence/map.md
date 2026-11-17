# task/wo/sum-dec-i128wrap-1/evidence/ — probes, raw cells, jar readings, provenance

- [probes/](probes/map.md) — the cell definitions, the runners and the analysis (comment-free Python).
- [raw/](raw/map.md) — one JSON object per cell, run and engine; the verdicts, the summary and the grids.
- [jar/](jar/map.md) — the `javap -c -p` excerpts of Spark 4.1.2's `Sum`, `TrySumExpressionBuilder$`,
  `CheckOverflowInSum`, `DecimalAddNoOverflowCheck` and `UnsafeRow.setDecimal`.

## Provenance

- **RePark:** main `6f80793b`. The package `pkg-avg-27d6149f` in the orchestrator's other scratchpad (read only),
  whose `BUILT_FROM` names `27d6149f`; `git diff --name-only 27d6149f 6f80793b -- crates python/repark/src
  python/repark/repark Cargo.toml Cargo.lock` lists only two `map.md` files (checked in this step). Run by the
  `lane-nan-write-canonical-1` interpreter with the package on `PYTHONPATH`, never with `-I`; every run printed
  `repark._native.__file__`, and every raw file's provenance row names
  `<scratchpad>/ddu-work/pkg-avg-27d6149f/repark/_native.abi3.so`. Version `1.5.6`.
- **Spark:** PySpark 4.1.2 (the c26 oracle environment), Zulu JDK 17, `local[1]`, UI off, the session zone pinned,
  shuffle partitions 1, a scratch warehouse, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` passed as `spark.jars`
  only (read, never an output path; its size and modification time were the same before and after every run) with
  its SQL extensions, a private hadoop catalog `lc` per run.
- **Jar readings:** the classes unzipped from the oracle environment's `spark-catalyst_2.13-4.1.2.jar` into a
  scratch directory (the jar only read), then `javap -c -p`.
- **When and how:** 2026-11-17, every probe under the shared measure lock on cores 58–63 with a 48 GiB
  address-space cap, every Spark JVM through the Spark slot, one command at a time, in this order: RePark ANSI on,
  RePark ANSI off, Spark on, Spark off, the native door, then the multi-partition pass (Spark on, RePark on, Spark
  off, RePark off).
- **Data:** written by each engine from string literals cast to each column's type, then read back from Parquet
  (`s` one file, `s4` four, `p2` two, `big` one); the controls `k` are an in-memory view (`kp` the same rows as
  Parquet, §10 of the domain table); the native door reads `VALUES` views of the same rows.
- **Commands** (from `probes/`, `<out>` a scratch directory, `<wh>` a scratch warehouse parent, `<jar>` the Iceberg
  runtime jar):
  - `run_door.py <spark|repark> <true|false> <wh> <jar> <out>/<engine>-<ansi>.jsonl`;
  - `run_native.py <out>/native.jsonl`;
  - `run_mp.py <spark|repark> <true|false> <wh> <jar> <out>/<engine>-mp-<ansi>.jsonl`;
  - `compact.py <out> ../raw`, then `analyze.py ../raw ../raw` and `tables.py ../raw/verdicts.jsonl`.
- **Checked:** after the runs the probes were reformatted by `ruff`, four long SQL strings split into implicit
  concatenations, two variables renamed for `typos`, and an unused path pattern dropped from `compact.py` (its output
  is byte-identical); `cells.sum_sql_cells()` and `EXPLAIN_CELLS` return the same 1,837 and 8 statements before and
  after, so the committed probes are the ones that ran.

Up: [../map.md](../map.md).
