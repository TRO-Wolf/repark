# Card LINE-COMMENT-CARRIAGE-RETURN-1: a `--` comment ends at a carriage return on Spark 4.1.2 and not on RePark

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the Opus 5.5 verifier's round-3 lead L7.

**Status:** open. Grade C, P3. Not built; not dispatched; awaiting the owner's charter.

**Why:** sqlparser ends a `--` line comment only at a line feed (`\n`). Spark 4.1.2 also ends it at a carriage return (`\r`). So `-- c\rALTER TABLE t DROP COLUMN v`, with no view in scope, changes the table on Spark, while on RePark the text after the carriage return stays inside the comment and the statement does nothing. The verifier measured this in round 3. No temporary view is involved, so it is outside the claim of [SPARK-DOOR-TEMP-VIEW-DDL-1](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md).

**Fix shape (not built):** measure the carriage-return forms on Spark 4.1.2 and on RePark first: a comment ended by `\r` followed by a statement, with and without a view, and the `\r\n` form. Then either end the comment at `\r` in the statement reader as Spark does, or decline the form with the charter. Do not change the parser path without that measurement.

**Retires:** the card when a `--` comment ends at `\r` on RePark as it does on Spark 4.1.2, pinned, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (lead L7, the round-3 verifier's finding; not in that unit's claim).
