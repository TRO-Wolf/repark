# PG integration report

**Status:** SKIP-LOUD -- `REPARK_PG_DSN` unset at agent run.
Default scale: 100_000 (`REPARK_PG_SCALE` integer opt-in for 1_000_000).
Battery paths (when live): registered-catalog `SELECT pg.schema.table`, `MERGE INTO ice… USING pg.…`, jdbc types-zoo, scale timing.
Units touch local memory-catalog Iceberg only (P9).
