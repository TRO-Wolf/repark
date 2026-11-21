# task/wo/asof-join-1/evidence/ — probes, raw cells, plans, jar readings, provenance

- [probes/](probes/map.md) — the cell, `merge_asof`, cost, model, verdict, matrix and tie probes (comment-free Python).
- [raw/](raw/map.md) — the cells per engine, mode and group (gzipped), the `EXPLAIN` texts, the cost samples.
- [jar/](jar/map.md) — the `javap -c -p` excerpts of Spark 4.1.2's as-of join.

## Provenance

- **RePark main:** `S/ddu-work/pkg-main-9a48932f` (`BUILT_FROM` `9a48932f0e083b568483fd077930e7cd3c126175`; its
  product equals main `a4d8857f`), run by the `lane-nan-write-canonical-1` interpreter with the package on
  `PYTHONPATH`, never `-I`; every RePark log printed `repark._native.__file__` from that package. Nothing built.
- **Spark:** PySpark 4.1.2 (the c26 oracle environment, pandas 2.3.3 installed by the owner's approval of
  2026-11-20, `PYARROW_IGNORE_TIMEZONE=1`), Zulu JDK 17, `local[1]`, UI off, shuffle partitions 1, session zone UTC,
  the Python process with `TZ=UTC`.
- **Cells** (2026-11-20, about 11:35–12:51 UTC): one command per engine, mode and group set,
  `measure.sh shared nice -n 10 taskset -c 32-47 prlimit --as=51539607552 [spark_slot.sh] <python> cells.py <engine>
  <out> <true|false> <groups> [type]`, and `pandas_cells.py <out> <true|false>` for Spark. Verdicts:
  `python3 probes/cmp.py <raw dir> <verdicts.jsonl>`, then `probes/matrix.py` and `probes/ties.py` (the source of
  [../domain-table.md](../domain-table.md)).
- **Cost:** `measure.sh exclusive nice -n 10 taskset -c 32-47 timeout 900 [spark_slot.sh] <python> cost.py …`, one
  case per command (Spark `local[16]`, 16 GB driver). A first Spark case at 2M × 2M with keys was withdrawn before it
  ran, once the plan showed it quadratic; the 2M no-key W2 case ran twice (both kept).
- **Ran later:** the critic ran Spark's grid with ANSI off on 2026-11-20 (2,268 records, `raw/critic-spark-false-G-*`);
  seat estimates are not samples.

Up: [../map.md](../map.md).
