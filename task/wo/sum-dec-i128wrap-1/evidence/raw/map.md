# task/wo/sum-dec-i128wrap-1/evidence/raw/ — the compacted cells, verdicts and grids

One JSON object per line; `key` names the cell (`sum/<position>/g<group>/<column>`, `try/…`, `avg/…`, `int/…`,
`df/<form>/<scope>/<column>`, `ice/<statement>/<scope>/<column>`, `mp/…` for the multi-partition pass, `explain/…`);
the SQL text is rebuilt from the key by `../probes/cells.py`. Scratch paths read `<scratchpad>`.

- `spark-true.jsonl`, `spark-false.jsonl`, `repark-true.jsonl`, `repark-false.jsonl` — the main pass, per engine and
  ANSI mode (2,273 records each).
- `spark-mp-true.jsonl`, `spark-mp-false.jsonl`, `repark-mp-true.jsonl`, `repark-mp-false.jsonl` — the
  multi-partition pass (`data/s4_partitions` and `data/p2_partitions` hold Spark's split counts).
- `native.jsonl` — the native door's sample (594 records).
- `verdicts.jsonl` — one verdict per paired cell (6,252), with both answers for every non-equal cell and every
  Iceberg statement, the value tags and the file tags.
- `summary.md` — verdict counts per run, mode, family, door and position, with tag counts.
- `grids.md` — the group-by-column letter grids per run, mode, family, door and position.

Up: [../map.md](../map.md).
