# task/wo/ice-held-frame-snapshot-1/evidence/ — probes, raw cells, jar readings, provenance

- [probes/](probes/map.md) — the cell, external-writer and cost probes, the verdict and matrix scripts (comment-free
  Python).
- [raw/](raw/map.md) — the cells per engine, group and ANSI mode, the verdicts, the summary, the cost samples.
- [jar/](jar/map.md) — the `javap -c -p` excerpts of Spark 4.1.2's execution-time table refresh and Iceberg 1.11's
  `SparkTable`, `SparkCatalog` and `CachingCatalog`.

## Provenance

- **RePark main:** `S/ddu-work/pkg-rp1d-52f468ce` (`BUILT_FROM` `52f468ce8864d046a1f077bc4c1d389f199d1448`, the tree
  of main `9f9650f5`, which `0436fdf9` leaves unchanged in product code), label `main`. **Main before unit 1:**
  `S/ddu-work/pkg-rp1base-8bc26cba` (`BUILT_FROM` `8bc26cba1b67522b15f0cc28a1ac8e1617ec9042`), label `base`. Run by
  the `lane-nan-write-canonical-1` interpreter with the package on `PYTHONPATH`, never with `-I`; every RePark log
  printed `repark._native.__file__` from its package. Nothing built here.
- **Spark:** PySpark 4.1.2 (the c26 oracle environment), Zulu JDK 17, `local[1]`, UI off, shuffle partitions 1,
  `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` as `spark.jars` (read only) with the Iceberg SQL extensions, a hadoop
  catalog `lc` over a scratch warehouse, session zone UTC.
- **Cells** (2026-11-20, about 05:25–05:41 UTC): one command per engine, group and ANSI mode,
  `flock -s <measure lock> nice -n 10 taskset -c 32-47 prlimit --as=51539607552 [spark_slot.sh] <python> cells.py
  <engine> <scratch warehouse parent> <out> <group> <true|false>`; the RePark `X` group ran inside `spark_slot.sh`
  because its external writer starts Spark. Two probe fixes after the first pass (the `writeTo().overwrite` filter now
  matches whole files, so the write happens on both engines; the pinned group's snapshot query names its columns,
  which RePark requires) were followed by a re-run of every affected group on all three engines; the raw files are the
  re-run's. Warehouses in the session scratchpad, deleted after the runs.
- **Verdicts:** `python3 probes/cmp.py <raw dir> raw/verdicts.jsonl`, then `probes/matrix.py raw/verdicts.jsonl`.
  Absolute scratch paths in error texts are replaced by `N` and `S` before the raw files are committed.
- **Cost:** `flock -x -w 600 <measure lock> nice -n 10 taskset -c 32-47 prlimit --as=51539607552 <python> cost.py main
  <scratch> <out> <round>`, one sample at a time, at most five rounds, main only (no seat is built). The first round
  waited from about 05:42 UTC for its 600 s while another lane's cargo build held the lock's shared side, and the series
  was stopped by hand at 05:52 UTC with no sample; [../sketch.md](../sketch.md) §6 gives read estimates.
- **Not measured:** Glue and S3 Tables (no external service was contacted; the sketch's §6 reads the path from the
  code), a local REST catalog (none available offline on this machine), and Spark's view of a second
  RePark process's commit (a RePark session cannot see a table another session created on a `type=hadoop` catalog,
  so that writer fails; domain table §6).

Up: [../map.md](../map.md).
