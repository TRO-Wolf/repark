# map — tests/fixtures/v3-spark-mor

## Purpose

Checked-in **Spark-written** Iceberg format-v3 table used by V3-1 to pin
`CALL system.register_table` and registry row `B-MOR-3` in CI with no JVM.

Written by PySpark 4.0.1 + Iceberg 1.10.0 (Hadoop catalog, local disk): four
10-row appends, then three merge-on-read `DELETE`s, leaving four Parquet data
files and three Puffin deletion vectors. Spark's own summary on the current
snapshot: `total-records = 40`, `total-data-files = 4`, `total-delete-files = 3`,
`current-snapshot-id = 4803484336433650168`. Live row count after vectors
apply: **37**.

The warehouse prefix baked into every metadata/Avro/Puffin path is
`/tmp/repark-v3-1-spark-mor` (26 bytes — same length as the original Spark
path so length-prefixed Avro strings stay valid). Tests copy this tree onto
that path under a lock; they do not rewrite paths at runtime.

## Contents

- `data/` — four `.parquet` data files and three `-deletes.puffin` vectors.
- `metadata/` — Hadoop-convention `v1.metadata.json` … `v8.metadata.json`,
  Avro snapshot lists and manifests, `version-hint.text` (`8`). Adopt via
  `metadata/v8.metadata.json`.

Hadoop `.crc` sidecar files are omitted; LocalFs does not consult them.

## Pointers

- Tests: [../call_register.rs](../../call_register.rs)
- Ledger: [../../../../../../task/v3-1-charter-ledger.md](../../../../../../task/v3-1-charter-ledger.md)
- Up: [../../map.md](../../map.md)
