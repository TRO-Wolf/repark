# Card ALTER-RENAME-QUOTED-DESTINATION-1: `ALTER TABLE ... RENAME TO "name"` with a double-quoted destination renames the view on RePark; Spark's answer is unmeasured

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the Muse MAX critic's round-3 finding R3F2 (S3) on `90c2cc0d`.

**Status:** open. Grade C, P3. Spark unmeasured; the round-4 verifier measures it. Not built; not dispatched; awaiting the owner's charter.

**Why:** With a temporary view `t` in scope and an Iceberg table `t` in the current namespace, `ALTER TABLE t RENAME TO "t2"`, with the destination double-quoted, renames the view on RePark, and the table is held. The inferred Spark 4.1.2 answer is `PARSE_SYNTAX_ERROR` at the double quote, because double quotes are not identifier syntax in Spark SQL; that is an inference, not a measurement. Fix round 2 refuses a double-quoted target (C-027) and fix round 3 refuses a double-quoted DROP TABLE or TRUNCATE TABLE target (C-034); the quoted destination of `RENAME TO` was in neither set of pins.

**Fix shape (not built):** the round-4 verifier measures `ALTER TABLE t RENAME TO "t2"` and the single-quoted destination on Spark 4.1.2 and on RePark, both catalogs, with the view and the table in scope, and records the answers. Then either RePark refuses a quoted destination as Spark does, with a pin, or the charter declines it. Do not change the RENAME TO route before that measurement.

**Retires:** the card when the quoted destination's Spark answer is measured and RePark matches it with a pin, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (clause C-037, the Muse critic's round-3 finding R3F2); family with [ALTER-TABLE-KEYWORD-VIEW-NAME-1](alter-table-keyword-view-name-1-card-2026-11-02.md).
