# task/wo/nan-minmax-1/evidence/step0/raw/ — the step 0 legs

One JSONL file per leg, written by `probe.py`; the first line is the leg header. Local paths are replaced by
`<root>`, `<scratch>` and `<home>`.

- `spark-4.1.2-own.jsonl` — Spark 4.1.2's legs `reads`, `paths`, `roundtrip`, `ingest` (before the `= NaN` cell was
  added).
- `spark-4.1.2-reads-cross-631d283c.jsonl` — Spark's `reads` again with the `= NaN` cell, and `cross`: Spark's read of
  main's round-trip files and Iceberg tables.
- `repark-631d283c.jsonl` — main's legs `reads`, `paths`, `roundtrip`, `ingest`, `cross`.
- `repark-631d283c-paths.jsonl` — main's `paths` leg again, with the native `CREATE EXTERNAL TABLE` under a
  registered warehouse.
- `repark-head.jsonl` — the head's legs `reads`, `paths` (with the warehouse cell), `roundtrip`, `ingest`, `cross`.
- `spark-4.1.2-cross-head.jsonl` — Spark's `cross` leg after the head ran: its read of main's and the head's
  round-trip files and Iceberg tables.

Up: [../map.md](../map.md).
