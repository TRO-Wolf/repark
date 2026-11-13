# task/wo/nan-partition-value-1/evidence/step0/probe/ — step 0's probe scripts

- `repark_write.py` — `repark_write.py <warehouse> <out.jsonl>`: RePark writes the tables on a Hadoop catalog and reads them back.
- `repark_read.py` — `repark_read.py <spark-warehouse> <out.jsonl>`: RePark adopts each Spark table and runs the read-half checks.
- `spark_probe.py` — `spark_probe.py <warehouse> <repark-warehouse> <out.jsonl> <runtime-jar>`: Spark's own tables, Spark reading RePark's, and the Java JSON calls.
- `dump_manifests.py` — `dump_manifests.py <warehouse>/ns`: each manifest entry's partition value and bits (needs fastavro).
- `render_step0.py` — `render_step0.py <raw-dir> <out-dir> <build>`: the sanitized raw legs and the rendered table.

Up: [../map.md](../map.md).
