# task/wo/avg-decimal-grouped-run-variance-1/evidence/ — probes, verdicts, `javap` excerpts

The probes carry no comments and take every path as an argument. Raw run files (per-run answers, logs, every
`repark._native.__file__` print) stay in `N/avg-variance-out/` (`raw/`, `logs/`).

- [tables.py](tables.py) — writes `t` (four files, six precisions, 13 groups), `big`, `t4` and the over-precision
  one- and two-file tables (copies of `S/verify-sum/overprec/files/`, plus a crafted one-row `0` file).
- [cells.py](cells.py) — the queries (1,180 per mode), the answer shape, the Iceberg CTAS cell, the 20-run loop.
- [repark_avg.py](repark_avg.py) — the RePark runner (stealing, `target_partitions` set before the read).
- [spark_avg.py](spark_avg.py) — the Spark 4.1.2 runner (`local[1]`, `local[4]`, Iceberg 1.11.0).
- [model.py](model.py) — the exact answer of every cell (seat A's answer), under a local `Decimal` context.
- [classify.py](classify.py) — EQUAL / WRONG / RAISE / VARIES per cell against both masters and the model.
- [table.py](table.py) — renders the domain table's counts, VARIES list and the kept-divergence classes.
- [narrow_native.py](narrow_native.py) — the `Decimal32` / `Decimal64` probe.
- [classes.md](classes.md) — seat A's kept divergence, cell for cell.
- [javap.md](javap.md) — the `javap -c -p` lines Spark's rule rests on.
- [verdicts/](verdicts/map.md) — the per-cell verdict tables, Spark's answers and the narrow probe's answers.

Up: [../map.md](../map.md).
