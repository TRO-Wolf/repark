# task/wo/string-hour-minute-second-1/evidence/probes/ — the cell definitions, runners and the classifier

Comment-free Python; every path is an argument. Run as in [../map.md](../map.md) (Provenance).

- [cells.py](cells.py) — the 16 text shapes (`SHAPES`), the 12-row multi-row table (`G_SHAPES`), the six
  configurations (`CONFIGS`: ANSI off/on × `UTC` / `America/Los_Angeles`, and two `TIMESTAMP_NTZ` ones), the 58
  projection functions (`FUNCS`: the 11 named extractors, `extract` and `date_part` for 12 fields each, 5 field
  aliases, 10 `date_trunc` units cast to `STRING`, `unix_timestamp`, two `date_format` patterns, five `CAST` controls),
  the 11 functions of the `WHERE` / `GROUP BY` / `ORDER BY` doors with their `WHERE` constants (`G_FUNCS`), the doors
  and the inputs.
- [probe.py](probe.py) — `probe.py <engine> <warehouse> <parquet dir> <out.jsonl> <part>`. `engine` is `spark` or
  `main` (RePark on `PYTHONPATH`; prints `repark._native.__file__`). Parts: `setup` (Spark: 17 Iceberg tables and the
  Parquet files both engines read), `<cfg>:proj[:<inputs>]` (the four projection doors, all 58 functions in one
  `SELECT`, one query per function when that raises), `<cfg>:g` (the six multi-row doors), `<cfg>:explain`. A RePark
  part makes its own Iceberg tables first (the `hadoop` type is a session catalog in RePark).
- [cost.py](cost.py) — `cost.py <warehouse> <out.json> <runs>`: a two-million-row Iceberg table (`c STRING`,
  `ts TIMESTAMP`), six `sum(<field>(…))` queries, round robin, the median.
- [cmp.py](cmp.py) — `cmp.py <raw dir> <out dir>`: pairs each cell with Spark's, writes `verdicts.jsonl` and
  `summary.md` (the counts quoted in [../../domain-table.md](../../domain-table.md)).

Up: [../map.md](../map.md).
