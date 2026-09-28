# `python/repark/tests/fixtures/ntz_1_spark_table` map

Spark-written Iceberg table for WO NTZ-1 slice 3, recorded by
`../../_record_ntz_1_spark_table.py` on PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0 and copied byte-identical from the
canonical `/tmp/repark-ntz-1-spark-table/wh` path (`.crc` sidecars skipped).
The pin test copies the table back to its canonical path before
`CALL system.register_table`, so the absolute file URIs in the manifests stay
valid. The recorder replaces only the `metadata/` and `data/` directories on
`--rewrite` and keeps this map. Spark's read answers live beside the probes
(`ntz-xc-spark.json`), not here. pins: ntz-1/C-008

## Contents

- [`metadata/`](metadata) + [`data/`](data) — format-version 2,
  `(id INT, c TIMESTAMP_NTZ, z TIMESTAMP)`, one row
  `2024-01-01 12:34:56.123456` written by Spark (`v1` empty, `v2` the one row).
