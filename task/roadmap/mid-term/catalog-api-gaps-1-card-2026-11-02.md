# Card CATALOG-API-GAPS-1: catalog-API answers that differ from Spark 4.1.2

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 round-4 records round (`claude-haiku-5-5`), for the orchestrator, from the Opus 5.5 round-4 verdict's lead L11.

**Status:** open. Grade C, P3. Not built; not dispatched; awaiting the owner's charter.

**Why:** The round-4 verifier ran the catalog APIs on Spark 4.1.2 and on RePark at the verdict head `d35a792b`. No API changed a table. These answers differ (verdict 4, Catalog APIs, L11; not re-measured on the round-4 fix head):
- `spark.catalog.tableExists("1t")`, also `9x_1` and names created by SQL or by the DataFrame API, raises ``invalid table identifier: unexpected character '1'``; Spark returns `True`.
- `spark.table` and `getTable`, on a view whose name holds an embedded doubled backtick, raise ``expected '.' between identifier segments``; Spark answers the view.
- `DESCRIBE` of a view named by an empty identifier, and of a view named `a/b`, raises ``cannot resolve the temporary view name …``; Spark describes the view.
- `getTable` on a table named `a.b`, with the name written quoted, returns `b` with a stray backtick; Spark returns `a.b`.
- `getTable("T1")` returns `t1`; Spark returns `T1`.
- `createOrReplaceTempView("a.b")` refuses with RePark's ``temp view name 'a.b' is qualified …``; Spark refuses with `TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS`.
- `dropTempView` answers equal on every name measured.

**Fix shape (not built):** measure each row on Spark 4.1.2 and on RePark, on the catalog the API uses, then pin the measured answers and make RePark match them, or decide each refusal text with the charter. Two rows overlap the name checks carded in [TEMP-VIEW-NAME-VALIDATION-1](temp-view-name-validation-1-card-2026-10-30.md) and [TEMP-VIEW-SURFACE-LEADS-1](temp-view-surface-leads-1-card-2026-10-29.md) (`createOrReplaceTempView` and `tableExists`); measure the overlap once.

**Retires:** the card when each measured row answers as Spark 4.1.2 does, pinned, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (lead L11, carded in the round-4 records; not that unit's claim).
