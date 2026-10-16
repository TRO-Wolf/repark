# Card REPARK-SQL-LOCAL-DDL-1: `repark.sql()` has no opt-in for local DDL

**Date:** 2026-10-15. **Filed by:** Muse Spark, clerk, under owner ruling 104 (a).

**Status:** open. Not scheduled.

**Owner:** whether this card closes before 1.5.6 ships: open (owner field, ruling 104).

**Retires:** when an opt-in path for `repark.sql()` exists (or read-only registration is allowed) with pins.

**Registry row:** [REPARK-SQL-LOCAL-DDL-1](../../../docs/cutover/production-audit-2026-10-11.md) (dated row 2026-10-15, owner ruling 104 (a)).

## Risk

Once PA-LOCAL-FS-POLICY-1 lands, `repark.sql()` cannot run any local `COPY` or `CREATE EXTERNAL TABLE`, read-only registration included, and has no opt-in.

## Sequence

PR #1075 (PA-LOCAL-FS-POLICY-1, not yet merged) makes the public Python `repark.sql()` refuse every local `COPY` and `CREATE EXTERNAL TABLE` outside a registered warehouse, and that entry has no registered warehouse. This includes a read-only `CREATE EXTERNAL TABLE r1 STORED AS PARQUET LOCATION '<existing local file>'`, which main registers and `SELECT` reads. No opt-in exists on that entry: `SET repark.sql.allowLocalFilesystemDDL = true` answers `Could not find config namespace "repark"`, and a session built with that key in its config still refuses. No real opt-in path was found, so the card is filed now and the 1.5.6 note carries the exact statement as a dated loss.

## Note

Measured by the U-5 verifier on main and PR #1075's head.
