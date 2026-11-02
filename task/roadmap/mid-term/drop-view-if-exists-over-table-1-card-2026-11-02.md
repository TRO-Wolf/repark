# Card DROP-VIEW-IF-EXISTS-OVER-TABLE-1: `DROP VIEW IF EXISTS` over a name that is only a table raises on RePark and is a no-op on Spark

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the Opus 5.5 verifier's round-3 lead L8.

**Status:** open. Grade C, P3. Not built; not dispatched; awaiting the owner's charter.

**Why:** With an Iceberg table `t` in scope and no temporary view named `t`, `DROP VIEW IF EXISTS t` raises `VIEW_NOT_FOUND` on RePark. Spark 4.1.2 answers ok, as a no-op. The verifier measured this in round 3. The DROP route for a view name predates [SPARK-DOOR-TEMP-VIEW-DDL-1](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md), and that unit does not change it.

**Fix shape (not built):** measure `DROP VIEW IF EXISTS` and `DROP VIEW` over a name that is only a table, and over a name that is neither a view nor a table, on Spark 4.1.2 and on RePark, on both catalogs. Then make RePark answer as Spark does on the measured rows, with pins, or decide the refusal with the charter. Do not change the DROP route without that measurement.

**Retires:** the card when `DROP VIEW IF EXISTS` over a table-only name answers as Spark 4.1.2 does, pinned, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (lead L8, the round-3 verifier's finding; not in that unit's claim).
