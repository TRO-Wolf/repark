# task/wo/temp-view-cache-lazy-parse-1/evidence/ — probes, raw cells, jar readings, provenance

- [probes/](probes/map.md) — the cell definitions, the trace, the cost cases and the analysis (comment-free Python).
- [raw/](raw/map.md) — the cells per engine, the verdicts, the summary, the trace, the cost samples.
- [jar/](jar/map.md) — the `javap -c -p` excerpts of Spark 4.1.2's cache and Python-exec paths.

## Provenance

- **RePark main:** `S/ddu-work/pkg-rp1d-52f468ce` (`BUILT_FROM` `52f468ce8864d046a1f077bc4c1d389f199d1448`, the tree
  of main `9f9650f5`), label `main`. **Main before unit 1:** `S/ddu-work/pkg-rp1base-8bc26cba` (`BUILT_FROM`
  `8bc26cba1b67522b15f0cc28a1ac8e1617ec9042`), label `base`. Run by the `lane-nan-write-canonical-1` interpreter with
  the package on `PYTHONPATH`, never with `-I`; every RePark log printed `repark._native.__file__` from its package
  ([raw/runs.txt](raw/runs.txt)). Nothing built here.
- **Spark:** PySpark 4.1.2 (the c26 oracle environment), Zulu JDK 17, `local[1]`, UI off, shuffle partitions 1, a
  scratch warehouse, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` as `spark.jars` (read only), a hadoop catalog `lc`.
  The oracle has no pandas: `PYTHONPATH` held a scratch directory of links to the RePark interpreter's pandas 3.0.5,
  NumPy 2.5.1 and `dateutil` (both interpreters Python 3.12.3; the oracle environment unchanged), with
  `PYSPARK_PYTHON` the oracle's interpreter.
- **Runs** (2026-11-20, about 04:00–04:20 UTC): one command per group and mode,
  `flock -s <measure lock> nice -n 10 taskset -c 48-57 prlimit --as=51539607552 [spark_slot.sh] <python> cells.py
  <engine> <scratch warehouse> <out> <group> <mode>`; groups `R`, `C`, `U`, `L` first, then `Q`, `N`, `T`; engines
  main, Spark, base. `trace.py` once on main. Cost: `flock -x`, one sample at a time, `cost.py <data> <out> <case>`,
  stopped after two rounds. Warehouses and the 2M-row data in the session scratchpad, deleted after the runs.
- **Analysis:** `python3 probes/cmp.py <raw> <out>` over the per-chunk files.

Up: [../map.md](../map.md).
