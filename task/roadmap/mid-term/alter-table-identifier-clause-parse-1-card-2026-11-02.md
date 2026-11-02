# Card ALTER-TABLE-IDENTIFIER-CLAUSE-PARSE-1: an `ALTER TABLE IDENTIFIER('t')` target is a parse error in RePark, where Spark accepts it

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the Opus 5.5 verifier's round-1 finding F2 (its remaining half after fix round 1).

**Status:** open. Grade C, P3. Not built; not dispatched; awaiting the owner's charter.

**Why:** RePark's parser rejects `ALTER TABLE IDENTIFIER('t') DROP COLUMN v` (the parse error names `(`), with or without a temporary view `t` in scope. Spark 4.1.2 accepts the clause. With the view in scope Spark refuses with `EXPECT_TABLE_NOT_VIEW` (measured for `DROP COLUMN` and `SET TBLPROPERTIES`) and the table is unchanged. Without a view Spark applies the change; RePark gives the parse error and the table is unchanged. No table changes, and the refusal is loud, but its text differs from Spark's. The parse gap predates SPARK-DOOR-TEMP-VIEW-DDL-1. Its `DEFAULT COLLATION` sibling answers Spark's text through a view name (fix round 1); without a view it still gives the parse error.

**Fix shape (not built):** measure again on the head before any change; the verifier's F2 probe (2026-11-02) is the only measurement so far. Then either accept the `IDENTIFIER` target in `ALTER TABLE` as Spark does, running the view check on the resolved name, or decline the form with the charter. Do not change the ALTER parse path without that measurement.

**Retires:** the card when `ALTER TABLE IDENTIFIER(...)` parses and matches Spark 4.1.2 with and without a view, pinned, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (lead L3; clause C-024, the kept divergence).
