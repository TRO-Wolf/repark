# Card SPARK-DOOR-DELETE-THROUGH-VIEW-1: Spark executes a DELETE through a temporary view over an Iceberg table, and RePark refuses it

**Date:** 2026-10-29. **Filed by:** the SPARK-DOOR-TEMP-VIEW-TARGET-1 clerk round (`claude-haiku-5-5`), from the builder's measured lead (the parent card's section on the audit's section 4 lead).

**Status:** open. Grade B, P2 (the builder's grading, copied). Not dispatched; awaiting the owner's charter.

**Why:** RePark refuses a statement that Spark answers. No wrong value is produced: before the parent unit the statement failed with `TableNotFound`, and after it the statement fails with the view refusal (`EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE`). The table is unchanged on RePark in both cases. P2, because the refusal is wrong, not silent.

**Measured (2026-10-29, builder's step 0 on Spark 4.1.2 with Iceberg 1.11.0, copy-on-write and merge-on-read):** with a temporary view `v` defined as `SELECT * FROM` a table in the current namespace, `DELETE FROM v WHERE id = 2` executes and takes the table from `1,2,3` to `1,3` on both table kinds. `DELETE FROM v WHERE id = 77` executes and matches no row. Spark's `UPDATE v SET …` refuses with `UNSUPPORTED_FEATURE.TABLE_OPERATION`, naming the underlying table. RePark at base `c48b1ad7`: `TableNotFound` for the DELETE, the UPDATE and the INSERT. RePark at `7984ed39`: `EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE` naming `v` for the DELETE, `UPDATE` refused naming `v`, and the INSERT refused as Spark does. The table is unchanged in every RePark case.

**Fix (not built):** make a DELETE whose target is a temporary view over an Iceberg table execute against that table, as Spark does, for matching and non-matching predicates and on both table kinds. UPDATE and MERGE through such a view keep Spark's refusal. The build must tell this case apart from a view over a literal, where Spark fails with an internal error and the parent unit refuses DELETE by ruling (the parent's ruling 4). The parent unit's refusal must not widen to the first case, and the first case must not be refused.

**Retires:** the card when a DELETE through a view over a table matches Spark on both table kinds, for a matching and a non-matching predicate, each case pinned, and the parent's DELETE refusal still holds for a view over a literal.

**Relation:** [SPARK-DOOR-TEMP-VIEW-TARGET-1 card](spark-door-temp-view-target-1-card-2026-10-29.md) (its lead paragraph is resolved there); [SPARK-DOOR-TEMP-VIEW-TARGET-1 ledger](../../ledgers/staging/spark-door-temp-view-target-1-ledger.md), clause C-009.
