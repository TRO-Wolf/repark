# Card C-2-CATALOG-DOORS-1: the neighbouring Spark-door catalog and DDL doors on a mounted source

**Date:** 2026-10-07. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief. **Source:** the S3 finding "Neighbouring Spark-door catalog and DDL doors answer a mounted source with misleading text" in the C-2d fold 1 re-verify verdict (PR #991).

**Status:** filed, not scheduled.

## Why

The verdict mounts `pg` with schema `s` and table `t`, and records what each Spark-door catalog and DDL door answers:

- `spark.catalog.getTable('pg.s.t')` raises `TABLE_OR_VIEW_NOT_FOUND` for a table that `tableExists` reports True.
- `spark.catalog.listColumns('pg.s.t')` raises `TABLE_OR_VIEW_NOT_FOUND` for a table that `tableExists` reports True.
- `spark.sql('SHOW VIEWS IN pg.s')` refuses as `read-only: DDL against it is not supported`, although it is a read.
- `spark.sql('SHOW COLUMNS IN pg.s.t')` refuses as `read-only: DDL against it is not supported`, although it is a read.
- `spark.sql('DESCRIBE TABLE pg.s.t')` is a `ParserError`.
- `spark.sql('TRUNCATE TABLE pg.s.t')` answers `TABLE_OR_VIEW_NOT_FOUND`, instead of the read-only text.
- `spark.sql('USE pg.s')` answers `SCHEMA_NOT_FOUND`.

None of these writes.

These doors sit outside N5's ruled list. N5 (S2, CLOSED in the verdict) covered `tableExists` True and False as expected, `SHOW TABLES` and `SHOW SCHEMAS` returning the declared empty listing, and `writeTo` and `saveAsTable` refusing with `CONNECT-DECL-pg-ddl`. The verdict says the neighbouring APIs outside that list are still rough.

## The ask

Each door does one of two things on a mounted source:

- the listing doors give the declared listing behaviour;
- the other doors refuse with a message that names the right registry row.

The verdict names `CONNECT-DECL-pg-ddl` only for `writeTo` and `saveAsTable`. For the other doors, the verdict does not name the right row.

## Gates

Not measured.

## Pointers

- The C-2d fold 1 re-verify verdict (PR #991), the S3 finding on the neighbouring doors and its N5 entry.
- The registry row `CONNECT-DECL-pg-ddl`, which the verdict names for `writeTo` and `saveAsTable` (PR #991 branch).
