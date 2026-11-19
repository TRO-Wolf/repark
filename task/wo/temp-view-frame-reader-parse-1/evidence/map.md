# task/wo/temp-view-frame-reader-parse-1/evidence/ — probes, raw cells, jar readings, provenance

- [probes/](probes/map.md) — the cell definitions and the analysis (comment-free Python).
- [raw/](raw/map.md) — one JSON object per cell and engine, the verdicts, the summary, the plans.
- [jar/](jar/map.md) — the `javap -c -p` excerpts of Spark 4.1.2's JSON and CSV file sources.
- [raw-r1/](raw-r1/map.md) — the critic's round: the new cells (midnight data), per-column verdicts, the summary
  and the critic's verdict.

## Provenance

- **RePark main:** package `S/ddu-work/pkg-main-4c365814` (`BUILT_FROM` `4c36581409ef180eedd2a7275781f193e94cbdb1`,
  #1362 merged), label `main`. **Main before part 2:** `S/ddu-work/pkg-main-6286ba19` (`BUILT_FROM`
  `6286ba195f0c46e1617d92df1c95b140868d18b7`, part 1 only), label `part1`. Both run by the `lane-nan-write-canonical-1`
  interpreter with the package on `PYTHONPATH`, never with `-I`; every run's log printed
  `repark._native.__file__` as `S/ddu-work/<package>/repark/_native.abi3.so`. Built by the orchestrator; nothing built
  here.
- **Spark:** PySpark 4.1.2 (the c26 oracle environment), Zulu JDK 17.0.15, `local[1]`, UI off, shuffle partitions 1,
  a scratch warehouse, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` passed as `spark.jars` only (read, never an output;
  sha256 prefix `d6ea6c5d099288da` before and after), a hadoop catalog `lc`.
- **Runs** (2026-11-19, 02:01–02:20 UTC): each part one command, `flock -s <measure lock> nice -n 10 taskset -c 32-47
  prlimit --as=51539607552 [spark_slot.sh] <python> rp.py <engine> <scratch warehouse> <out> <part>`, the lock held
  for that one command only. Warehouses in the session scratchpad, deleted after the runs. Raw files are verbatim
  except that scratch paths read `<scratchpad>` and the `insertInto` cells' label was renamed `into` after the run
  (the spell check flags the first label).
- **Analysis:** `python3 probes/cmp.py <raw> <out>`, then `python3 probes/tables.py <out>/verdicts.jsonl`.
- **Round 1 (the critic's gaps), 2026-11-19, 03:09–03:18 UTC:** the same builds, interpreter, Spark and lock
  discipline, one command per part: `probes/rv.py <engine> <scratch warehouse> <out> <part>` for `X`, `XC`, `D`, `N`,
  `E`, then `probes/rp.py <engine> <scratch warehouse> <out> Z<i> midnight` for the eight zone passes, each engine
  `main`, `spark`, `part1`; every RePark log printed `repark._native.__file__` from its package. Analysis:
  `python3 probes/rvcmp.py raw-r1/r- raw-r1/m- raw/cells-verdicts.jsonl raw-r1` (run from this directory). Warehouses
  in the session scratchpad, deleted after the runs. The third zone of the nesting set is `Asia/Tokyo`, a test value.

Up: [../map.md](../map.md).
