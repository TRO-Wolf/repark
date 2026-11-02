# Card ALTER-VIEW-RENAME-OVER-TABLE-1: plain `ALTER VIEW ... RENAME TO` over a same-named table refuses where Spark 4.1.2 renames the view

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the round-5 Opus 5.5 delta verdict, lead L14, at `618df399`.

**Status:** open. Lead L14: the refusal is loud and changes no table. Carded, not fixed in SPARK-DOOR-TEMP-VIEW-DDL-1: the unit is past its cycle cap (owner rule 2026-11-02). Not built; not dispatched; awaiting the owner's charter.

**Why:** With a temporary view `t` over a table `t` in the current namespace, plain `ALTER VIEW t RENAME TO t2` answers ``Cannot rename a table with ALTER VIEW. Please use ALTER TABLE instead.`` on both catalogs. Spark 4.1.2 renames the view. Both objects are held on RePark. This is the plain-name analogue of the digit-leading `ALTER VIEW` case in card [ALTER-VIEW-DIGIT-NAME-1](alter-view-digit-name-1-card-2026-11-02.md) (lead L12).

**Fix shape (not built):** before any change, measure and pin `ALTER VIEW t RENAME TO t2` over a same-named table on both catalogs against Spark 4.1.2. Then route the plain name to the temporary view first, as the `ALTER TABLE ... RENAME TO` route does (C-005), with the table held.

**Retires:** the card when the plain `ALTER VIEW` rename over a same-named table matches Spark 4.1.2 on both catalogs, with pins, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md), clause C-005 (the `ALTER TABLE` rename) and the round-5 verdict, lead L14; family with ALTER-VIEW-DIGIT-NAME-1.
