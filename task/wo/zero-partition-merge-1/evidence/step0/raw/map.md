# task/wo/zero-partition-merge-1/evidence/step0/raw/ — step 0's raw legs

One JSON record per line, written by `../probe/zero_doors_probe.py` and passed through `../probe/sanitize.py` (local
paths become `<local>/<file name>`, the module path `<package>`). The first line of each leg is its setup record:
engine, CPUs, shuffle partitions, build. `main-1d9d2808` is main's release module, `pr1325-948d7fb3` the #1325 head.

- `repark-main-c1.jsonl`, `repark-main-c2.jsonl`, `repark-main-c10.jsonl` — RePark main on 1, 2 and 10 CPUs: the 24 doors x 12 variants; the c1 leg adds the `bucket`/`truncate` creates, the native-door attempts and native `INSERT` (the c2 leg adds native `INSERT`).
- `repark-main-p16.jsonl` — RePark main on 10 CPUs with `spark.sql.shuffle.partitions = 16`.
- `repark-main-extra-c1.jsonl`, `repark-main-extra-c2.jsonl` — the two `_cross` overwrite spellings (static `PARTITION`, `writeTo().overwrite`) on 1 and 2 CPUs.
- `repark-1325-c1.jsonl`, `repark-1325-p16.jsonl` — the #1325 head on 1 CPU and at 16 partitions.
- `repark-main-reads-spark-c2.jsonl` — RePark main adopting Spark's `local[2]` tables with `register_table`.
- `spark-c1.jsonl`, `spark-c2.jsonl`, `spark-c16.jsonl` — Spark 4.1.2 on its own tables at `local[1]`, `local[2]`, `local[16]` (the `local[16]` leg includes the two `_cross` spellings).
- `spark-extra-c2.jsonl` — the two `_cross` spellings on Spark at `local[2]`.
- `spark-reads-repark-main-c1.jsonl` — Spark reading RePark's 1-CPU tables.
- `disk-repark-main-c1.jsonl`, `disk-repark-main-c2.jsonl`, `disk-repark-main-c10.jsonl`, `disk-repark-main-p16.jsonl`, `disk-spark-c1.jsonl`, `disk-spark-c2.jsonl`, `disk-spark-c16.jsonl` — every Parquet and ORC data file's stored `d`/`f` bits per row (`../probe/disk_bits.py`); Avro files are marked `not-read`.

Up: [../map.md](../map.md).
