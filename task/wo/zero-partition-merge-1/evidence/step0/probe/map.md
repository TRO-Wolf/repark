# task/wo/zero-partition-merge-1/evidence/step0/probe/ — step 0's probe scripts

Each run went under the lane's shared measure lock, `nice -n 10`, a `taskset` CPU list and `prlimit --as`; Spark
runs also took the Spark JVM lock. RePark ran on the release package with `PYTHONPATH`, Spark on the oracle venv with
`JAVA_HOME` set to Zulu 17 and `ICEBERG_SPARK_JAR` naming the 1.11.0 runtime jar.

- `zero_doors_probe.py` — `zero_doors_probe.py {repark|spark} <warehouse> <out.jsonl> [--cores N] [--shuffle N] [--legs doors,transforms,native,native_insert,cross_read] [--only door,...] [--formats ...] [--other-root <hadoop-root>] [--other-records <jsonl>]`: the 25 doors on v2/v3 x Parquet/ORC/Avro x `PARTITIONED BY (d)`/`(f)`, the `bucket`/`truncate` creates, the native-door attempts, and a cross-read of another engine's tables (`register_table` on RePark, the shared Hadoop root on Spark). Each table records the outcome, each row's `d`/`f` bits as read, and `.files` (content, partition bits, rows, path).
- `disk_bits.py` — `disk_bits.py <run.jsonl> <disk.jsonl>`: reads every Parquet and ORC data file a run's `.files` lists with pyarrow and records each row's stored `d`/`f` bits.
- `render_step0.py` — `render_step0.py <run.jsonl>...` prints the status per door and run and every non-ok cell; `render_step0.py --layouts <variant> <run.jsonl>...` prints one variant's files and rows per door.
- `render_disk.py` — `render_disk.py <disk.jsonl>...`: the stored-bits summary of step0.md section 1.3.
- `sanitize.py` — `sanitize.py <in.jsonl> <out.jsonl> <build>`: replaces local absolute paths with `<local>/<file name>` and the module path with `<package>`, and stamps the build on the setup line.

Up: [../map.md](../map.md).
