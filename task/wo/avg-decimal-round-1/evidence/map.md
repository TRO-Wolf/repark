# task/wo/avg-decimal-round-1/evidence/ — probes, raw cells, jar readings, provenance

- [probes/](probes/map.md) — the cell definitions, the runners and the analysis (comment-free Python).
- [raw/](raw/map.md) — one JSON object per cell, run and engine, the verdicts, the summary and the grids.
- [built/](built/map.md) — the build's bar, cost and raw data (2026-11-17).
- [jar/](jar/map.md) — the `javap -c -p` excerpts of Spark 4.1.2's `Average`, `DecimalDivideWithOverflowCheck`,
  `Decimal`, `Divide`, `BinaryArithmetic`, `DecimalPrecisionTypeCoercion` and `DataTypeUtils`.

## Provenance

- **RePark:** the package `pkg-dct-7bf32859` in the orchestrator's other scratchpad (read only), built from
  `7bf32859`; `git diff --name-only 7bf32859 a792ce47 -- crates python/repark Cargo.toml Cargo.lock` lists only
  `map.md` files (re-checked in this step). Run by the `lane-nan-write-canonical-1` interpreter with the package on
  `PYTHONPATH`, never with `-I`; every run printed `repark._native.__file__`, and every raw file's provenance row
  names `<scratchpad>/ddu-work/pkg-dct-7bf32859/repark/_native.abi3.so`. Version `1.5.6`.
- **Spark:** PySpark 4.1.2 (the c26 oracle environment), Zulu JDK 17, `local[1]`, UI off, the session zone pinned,
  shuffle partitions 1, a scratch `spark.sql.warehouse.dir`, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` passed as
  `spark.jars` only (read, never an output path) with its SQL extensions, a private hadoop catalog `lc` and
  warehouse per run.
- **Jar readings:** classes unzipped from the oracle environment's `spark-catalyst_2.13-4.1.2.jar` and
  `spark-sql-api_2.13-4.1.2.jar` into a scratch directory (the jars only read), then `javap -c -p`.
- **When:** 2026-11-17. The main pass ran in one sequence of eleven commands (native door, then RePark and Spark for
  each conf and mode); `explain_paths.py` and `run_extra.py` ran after it. Every probe ran under the shared measure
  lock on cores 48–57 with a 48 GiB address-space cap, every Spark JVM under the Spark lock, one command at a time.
- **Data:** written by each engine from string literals cast to each column's type, then read back from Parquet
  (`t4` appended as four files); no timestamps.
- **Commands** (from `probes/`, `<out>` a scratch directory, `<wh>` a scratch warehouse parent, `<jar>` the
  Iceberg runtime jar):
  - `run_native.py <out>/native.jsonl`;
  - `run_door.py <spark|repark> <true|false> <base|apl_false|pmp_false> <wh> <jar> <out>/<engine>-<ansi>-<conf>.jsonl`;
  - `explain_paths.py <wh> <out>/explain-paths-repark.txt` (RePark, ANSI on);
  - `run_extra.py <spark|repark> true <wh> <jar> <out>/extra-<engine>-true.jsonl`;
  - `compact.py <out> ../raw`, then `analyze.py ../raw ../raw` and `grids.py ../raw`.
  - the revision (after the inventory critic): `run_revision.py <spark|repark> <true|false> <wh> <jar> <out>/rev`,
    then `compact.py <out>/rev ../raw/rev` and `analyze.py ../raw/rev ../raw/rev`; the Spark 4.1.2 `Pmod`, `Remainder`
    and `DecimalAddNoOverflowCheck` readings appended to `jar/javap-excerpts.txt`.
- **Checked:** the SQL text recorded with every raw cell equals the text `cells.py` rebuilds for its key (8,464 cells,
  0 mismatches), so the committed probes are the ones that ran (later edits were formatting only).

Up: [../map.md](../map.md).
