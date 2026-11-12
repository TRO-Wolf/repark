# Step-0 provenance

Measured 2026-11-11 on the unmodified tree at head
`976b4606e2c19ba46f0af39df18e0dcb9406bf9b`.

- RePark: this clone's release build (`maturin develop --profile release`),
  interpreter CPython 3.12.3, ANSI on, `repark.sql.allowCreateFormatVersion3`
  on. Session codec `zstd` on the F6 and K3 SQL cells only.
- Spark oracle: 4.1.2 with `iceberg-spark-runtime-4.1_2.13-1.11.0`, banner
  version 4.1.2, session timezone UTC, interpreter CPython 3.12.3, ANSI on.
- Cells: 228 RePark rows (F1 to F6 times seven writer spellings times hadoop
  and memory times v2 and v3, every K row, one PREPARE, one EXPLAIN ANALYZE),
  one cancel cell, three Spark CTAS cells and three Spark path reads of the
  RePark F1, F3 and F6 leftovers.
- Scrubbed values: machine paths render as `<warehouse>` and `<home>`; uuid
  file stems render as `*`.
