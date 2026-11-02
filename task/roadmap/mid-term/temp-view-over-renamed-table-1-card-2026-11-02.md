# Card TEMP-VIEW-OVER-RENAMED-TABLE-1: a view over a temporary view reads differently from Spark after the name it reads is renamed or dropped

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the builder's leads and the Opus 5.5 verifier's round-1 finding F4a.

**Status:** open. Grade C, P3. Not built; not dispatched; awaiting the owner's charter.

**Why:** A temporary view `u` is defined over the temporary view `t` in the current namespace (`CREATE TEMPORARY VIEW u AS SELECT * FROM t`). `t` is itself a temporary view over a literal, shadowing an Iceberg table `t` of the same name. After `ALTER TABLE t RENAME TO t2`, which renames the view, Spark 4.1.2 re-resolves the name `t` when `u` is read: `SELECT * FROM u` reads the table's rows (1, 2 and 3 in the verifier's setup). RePark answers `TABLE_OR_VIEW_NOT_FOUND` for `t`. The same gap is measured after `DROP VIEW t`: Spark reads the table's rows, and RePark refuses. The cause is not traced here; the answer is measured, not explained. SPARK-DOOR-TEMP-VIEW-DDL-1 does not change the `DROP VIEW` route. The refusal is loud and no wrong value is produced, so the grade is C and the priority P3.

**Fix shape (not built):** before any change, measure both shapes on the memory and hadoop catalogs, after `RENAME TO` and after `DROP VIEW`: a view over the temporary view (measured by the verifier on Spark and RePark, as above, not yet pinned) and a view over the table itself (not measured). Then pin the measured outcome and make RePark match it, or decide the refusal text with the charter. Do not change the rename or drop routes without that measurement.

**Retires:** the card when a view over a renamed or dropped name reads as Spark 4.1.2 does, pinned for both shapes, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (this lead, L1; clause C-005 for the rename itself).
