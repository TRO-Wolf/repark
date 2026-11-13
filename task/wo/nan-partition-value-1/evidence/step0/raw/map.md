# task/wo/nan-partition-value-1/evidence/step0/raw/ — step 0's raw legs

One JSON record per line. `main` is main `a3f8c7d9`'s release module (`_native.abi3.so`); the first line of each RePark leg names it.

- `repark-main-writes.jsonl` — RePark writes and reads its own tables (`repark_write.py`): INSERT, CTAS, `writeTo`, `saveAsTable`, the infinity twin, the INT/STRING/unpartitioned controls.
- `repark-main-reads-spark.jsonl` — the read half: RePark adopts Spark's tables with `register_table` (`repark_read.py`).
- `spark-own.jsonl` — Spark 4.1.2 on its own tables, plus the Java `SingleValueParser` calls (`spark_probe.py`).
- `spark-reads-repark-main.jsonl` — Spark on its own tables again and on RePark's tables from `repark-main-writes.jsonl`.
- `manifests-repark-main.jsonl`, `manifests-spark-own.jsonl` — every manifest entry's stored partition value and bit pattern, read with fastavro (`dump_manifests.py`).

Up: [../map.md](../map.md).
