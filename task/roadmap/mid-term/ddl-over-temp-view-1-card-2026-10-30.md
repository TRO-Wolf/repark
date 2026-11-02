# Card DDL-OVER-TEMP-VIEW-1: CREATE OR REPLACE TABLE and DROP TABLE with a temporary view of the same name in scope

**Date:** 2026-10-30. **Filed by:** the SPARK-DOOR-TEMP-VIEW-TARGET-1 records round 3 (`claude-haiku-5-5`), from the Muse MAX second verifier's round-3 leads on `bfce6032`.

**Status:** open. Grade C. Not built; not dispatched; awaiting the owner's charter. Spark's answer is unmeasured: measure before any build. Dated 2026-11-01 (the third audit, section 3): both shapes were measured on RePark `d0562058` and are Spark-equal. `DROP TABLE t` and `DROP TABLE IF EXISTS t` drop the view on both engines and leave the table; `CREATE OR REPLACE TABLE t AS SELECT 70` and `REPLACE TABLE t AS SELECT 70` replace the table on both engines, and `t` still reads the view. That measurement supersedes the 'unmeasured' sentence above; the pins, or the charter's decline, are still owed. Round 4 (2026-11-02): the F12 class, a doubled backtick in a view name reaching the same-named table through a write, is closed by the doubled-backtick scanner fix at `7331ce45`; the two shapes above are not part of that class.

**Why:** Muse MAX found two statements, with a temporary view `t` in scope and a table `t` in the current namespace, that the write-target refusal of [SPARK-DOOR-TEMP-VIEW-TARGET-1](spark-door-temp-view-target-1-card-2026-10-29.md) does not reach, because neither is one of its six write routes. Both are pre-existing, and no Spark answer was recorded:

1. `CREATE OR REPLACE TABLE t AS SELECT ...` replaces the table, while reads of `t` keep returning the view.
2. `DROP TABLE t` drops the view.

Muse rated both S2. If Spark refuses the first shape, RePark's replacement of the table is the same family of fault as AUD-006 of the second audit (a table changed by a statement Spark refuses), and the charter should weigh the grade against that.

**Fix shape (not built).** Measure Spark 4.1.2's answer to both statements, on copy-on-write and merge-on-read, with the view in scope and out of scope, before any change. Then pin each shape on the measured outcome and make RePark match it. Do not widen the view-target refusal to CREATE or DROP statements without that measurement.

**Retires:** the card when both shapes match Spark 4.1.2 on the probe and each is pinned, or the charter declines them.

**Relation:** [SPARK-DOOR-TEMP-VIEW-TARGET-1 card](spark-door-temp-view-target-1-card-2026-10-29.md) and its [unit ledger](../../ledgers/completed/spark-door-temp-view-target-1-ledger.md), clause C-028. The other round-3 leads are in [TEMP-VIEW-NAME-VALIDATION-1](temp-view-name-validation-1-card-2026-10-30.md) (name grammar) and [TEMP-VIEW-SURFACE-LEADS-1](temp-view-surface-leads-1-card-2026-10-29.md) (listings and classes).
