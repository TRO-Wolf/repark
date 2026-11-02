# Card ALTER-VIEW-DIGIT-NAME-1: `ALTER VIEW` over a digit-leading temporary view name answers differently from Spark 4.1.2

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 round-4 records round (`claude-haiku-5-5`), for the orchestrator, from the Opus 5.5 round-4 verdict's lead L12.

**Status:** open. Grade C, P3. Not built; not dispatched; awaiting the owner's charter.

**Why:** With a temporary view `1t` in scope over a table `1t`, `ALTER VIEW 1t RENAME TO t2` answers ``Cannot rename a table with ALTER VIEW. Please use ALTER TABLE instead.``, where Spark 4.1.2 renames the view. `ALTER VIEW 1t SET TBLPROPERTIES ('k1'='x')` answers ``[UNSUPPORTED_FEATURE.CATALOG_OPERATION]`` (catalog `hd` does not support views), where Spark answers `EXPECT_PERMANENT_VIEW_NOT_TEMP`. Both are refusals and both hold the table (verdict 4, Leads, L12; measured at `d35a792b`). The digit-leading `ALTER TABLE` forms are pinned by SPARK-DOOR-TEMP-VIEW-DDL-1 (C-031) and are not this card.

**Fix shape (not built):** measure `ALTER VIEW` over digit-leading names, on Spark 4.1.2 and on RePark, on both catalogs, with the view and the table each present or absent. Then pin the measured answers and make RePark match them, or decide the refusal texts with the charter. Do not change the `ALTER VIEW` route without that measurement.

**Retires:** the card when `ALTER VIEW` over a digit-leading view name answers as Spark 4.1.2 does, pinned, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (lead L12, carded in the round-4 records; not that unit's claim).
