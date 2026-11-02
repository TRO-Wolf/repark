# Card TEMP-VIEW-OVER-RENAMED-TABLE-1: a temporary view over a table reads differently from Spark after the table is renamed or the view it reads is dropped

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the builder's leads in that unit's hand-back.

**Status:** open. Grade C, P3. Not built; not dispatched; awaiting the owner's charter.

**Why:** A temporary view `u` is defined over the table `t` in the current namespace. After `ALTER TABLE t RENAME TO t2`, Spark 4.1.2 re-resolves the name `t` when `u` is read: `SELECT * FROM u` reads the table's rows (measured by the builder). RePark answers `TABLE_OR_VIEW_NOT_FOUND` for `t`, because `u` captured the home of `t` when it was created; RePark's answer was verified on the fixed wheel. The same gap exists after `DROP VIEW t`, and that case predates the unit. The refusal is loud and no wrong value is produced, so the grade is C and the priority P3.

**Fix shape (not built):** measure Spark 4.1.2's read of a view over a table after `RENAME TO` and after `DROP VIEW`, on the memory and hadoop catalogs, before any change. Then pin the measured outcome and make RePark match it, or decide the refusal text with the charter. Do not change the rename or drop routes without that measurement.

**Retires:** the card when a view over a renamed or dropped name reads as Spark 4.1.2 does, pinned for both shapes, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (this lead, L1; clause C-005 for the rename itself).
