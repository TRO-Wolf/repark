# Card SPARK-DOOR-TEMP-VIEW-DDL-1: ALTER TABLE through a temporary view's name changes the same-named Iceberg table on the Spark door

**Date:** 2026-11-01. **Filed by:** the third-audit records round (`claude-haiku-5-5`), from the targeted re-audit of the format-v2 label, finding AUD-015 ([the report](../../../docs/audits/iceberg-v2-label-targeted-reaudit-2026-11-01.md), section 5).

**Status:** dispatched 2026-11-01 on an Opus 5.5 lane (branch `fix/spark-door-temp-view-ddl-1`), not merged. Grade A, P1, Critical. A blocker of the format-v2 label (ledger clause C-079).

**Why:** With a temporary view `t` in scope, the twelve `ALTER TABLE t ...` kinds the audit counts change the same-named Iceberg table, where Spark 4.1.2 refuses with `EXPECT_TABLE_NOT_VIEW`. `DROP COLUMN` and `REPLACE COLUMNS` remove a column's values from every current read, which is silent data loss by an ordinary statement, the class that blocked the label under AUD-006. The view keeps answering `SELECT`. The fault predates the third audit: it is identical at `321c6095`.

**Site:** the Spark door's ALTER route resolves its target without consulting the temporary view. The write-target refusal of [SPARK-DOOR-TEMP-VIEW-TARGET-1](spark-door-temp-view-target-1-card-2026-10-29.md) covers the six write heads only (`WRITE_HEADS` in `temp_write.rs`, line 12), so ALTER is not reached. The charter names the function from the code.

**Fix:** with a temporary view of the name in scope, every ALTER kind refuses with Spark's condition (`EXPECT_TABLE_NOT_VIEW`, and Spark's `USE_ALTER_VIEW` text for the table-property kinds) and leaves the table unchanged. Each kind gets one pin, with its expected outcome taken from Spark 4.1.2. `ADD PARTITION FIELD` and `WRITE ORDERED BY` change the table on both engines, and `SET IDENTIFIER FIELDS` refuses on both; those three stay as they are. The audit measured the Spark door only, so the native door's ALTER route is measured in the unit before it is pinned.

**Measured (the third audit, 2026-11-01; copied, and reproduced by the orchestrator):**

```text
CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 99 AS id;
ALTER TABLE t DROP COLUMN v;
```

RePark applies it: the table then reads `[[1],[2],[3]]` with the schema `id` alone. Spark refuses with `EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE` and the table reads `[[1,'a'],[2,'b'],[3,'c']]`. `ALTER TABLE t REPLACE COLUMNS (id INT)` is applied by RePark, and the table then reads NULL in every row; Spark refuses. Per kind, on RePark and Spark: `ADD COLUMN`, `ADD COLUMNS`, `RENAME COLUMN`, `ALTER COLUMN ... TYPE BIGINT` and `ALTER COLUMN ... COMMENT` are applied and refused. `SET TBLPROPERTIES` and `UNSET TBLPROPERTIES` are applied and refused with `EXPECT_TABLE_NOT_VIEW.USE_ALTER_VIEW`. `RENAME TO` renames the table on the memory catalog, so the catalog's `t` disappears; Spark renames the view and leaves the table. On the hadoop catalog RePark refuses with that catalog's own `Cannot rename Hadoop tables`. The same results hold on the base `321c6095`.

**Retires:** the card when each ALTER kind in the probe refuses with Spark's condition and leaves the table unchanged on both doors, each kind pinned. The label's AUD-015 row (ledger clause C-079) then closes by citation, in the targeted re-audit's follow-up.

**Relation:** [DDL-OVER-TEMP-VIEW-1](ddl-over-temp-view-1-card-2026-10-30.md), which covers `CREATE OR REPLACE TABLE` and `DROP TABLE` (measured Spark-equal on 2026-11-01); the label order [ICE-V2-LABEL-1](../../wo/ice-v2-label-1/order.md) and its [ledger](../../ledgers/staging/ice-v2-label-1-ledger.md).
