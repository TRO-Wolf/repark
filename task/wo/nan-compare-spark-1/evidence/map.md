# task/wo/nan-compare-spark-1/evidence/ — probes, raw cells, provenance

- [probes/](probes/map.md) — the source writers, the cell definitions, the runners and the renderer
  (comment-free Python).
- [raw/](raw/map.md) — the per-cell verdicts of every RePark build against Spark, the NaN producers, the
  file metadata, and the write logs.

## Provenance

- **RePark builds** (read only, scratchpad packages, run by the NAN-WRITE-CANONICAL-1 lane interpreter with the
  package on `PYTHONPATH`; every leg's first record names the loaded `repark/_native.abi3.so`):
  - **base:** `nwc/pkg-base`, built from main `1d9d2808`. Main `2707924e` differs from it in tests and maps
    only (`git diff --stat 1d9d2808 2707924e -- crates python/repark Cargo.lock Cargo.toml`, product files:
    none), so base is main's product.
  - **1325:** `pkg-1325`, `BUILT_FROM` `948d7fb3` (NAN-WRITE-CANONICAL-1, #1325 with fork `d6592644`).
  - **head3:** `pkg-head3`, `BUILT_FROM` `9d92adf6` (SIGNED-ZERO-NORMALIZE-1 part B, #1326).
- **Spark:** PySpark 4.1.2 (the c26 oracle environment), Zulu JDK 17, `iceberg-spark-runtime-4.1_2.13-1.11.0`,
  hadoop catalog, session zone UTC; writes on `local[1]` (one file per append), reads on `local[2]`.
- **When and how:** 2026-11-13, about 13:35–14:30 local time. Every command ran under the shared measure lock on
  cores 48–57 with a 48 GiB address-space limit, one command at a time; every Spark JVM also held the Spark lock.
- **The sources.** `write_sources.py repark` (base) writes the pyarrow files (`pa_l1`, `pa_l5`, `pa_part`), then
  the `rb_*` Iceberg tables and `rb_*` Parquet files from them; `write_sources.py spark` writes the `sp_*`
  tables and files from the same pyarrow files. Base keeps every NaN's bits in Parquet, ORC and Avro; Spark
  stores `7ff8…` in Parquet and ORC and keeps the bits in Avro (§5 of the domain table). Every leg reads the
  same files: RePark registers each table's latest metadata file with `CALL sc.system.register_table` in a
  private catalog (RePark's `hadoop` catalog does not list an existing warehouse); the native door cannot run
  `CALL`, so it reads Iceberg through `nat_parquet_l5`, a table each build writes itself with native
  `INSERT` from the `pa_l5` files.
- **Commands** (from `probes/`): `write_sources.py <repark|spark> <wh> <pq> <log>`; `stats.py <wh> <pq> <out>`
  on Spark; `cells.py <repark|spark> <wh> <pq> <out> --ansi <true|false> [--sources …] [--ops …]` (ANSI on:
  every source; ANSI off: `created`, `computed`, `rb_parquet_l5`, `rb_avro_l5`, `pq_pa_l5`, `rb_parquet_part`,
  and `nat_parquet_l5` on RePark; the supplementary legs: `part_created` with every operation, and the four
  grouped `min`/`max` operations over six sources); `analyze.py <runs> <out>` then `render.py <out> <tables>`.
- **Per-leg raw output** (about 3 MB per RePark leg) stays in the scratchpad; `raw/` holds the
  merged per-cell answers, which carry every answer each build and Spark gave.
- **Spark's runtime jar** is named by `ICEBERG_SPARK_JAR` (the runs used the c26 oracle's ivy copy of
  `iceberg-spark-runtime-4.1_2.13-1.11.0`). From about 14:15 the owner allowed two Spark JVMs; the later
  Spark legs took a free Spark slot instead of the single Spark lock.

Up: [../map.md](../map.md).
