# Card FACADE-OVER-REFUSALS-1: over-refusals and refusal texts where Spark answers or names a condition

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the format-v2 label re-audit ([report](../../../docs/audits/iceberg-v2-label-corpus-audit-2026-11-05.md), finding AUD-028).

**Status:** filed, awaiting the owner's decision; severity S3; does not block the format-v2 label. Each item is loud and changes no state. Ledger [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) C-104.

**Why:** The audit measured these over-refusals and refusal texts on the facade:
- `DROP VIEW IF EXISTS t` over a same-named table refuses `VIEW_NOT_FOUND`, where Spark does nothing.
- `WITH t AS (...) INSERT INTO ...` refuses, where Spark inserts the CTE rows.
- A DELETE with an IN-subquery refuses. This is a deliberate guard, and it stays unless the owner rules otherwise.
- An INSERT of `array(map('k', ...), map())` refuses on array coercion.
- `CREATE TABLE ... TBLPROPERTIES ('format-version'='3')` and the matching `SET TBLPROPERTIES` refuse unless `repark.sql.allowCreateFormatVersion3` is set.
- Several failure refusals carry DataFusion text where Spark names `CAST_INVALID_INPUT`, `NOT_NULL_ASSERT_VIOLATION`, `CAST_OVERFLOW_IN_TABLE_INSERT` or `INSERT_COLUMN_ARITY_MISMATCH`.

**Appended 2026-11-12 (records clerk, `claude-opus-5-5`, owner ruling Q8 of 2026-11-12): the `WITH … INSERT` item,
re-measured.** SIGNED-ZERO-NORMALIZE-1 part B evidence (`task/wo/signed-zero-normalize-1/evidence/part-b/`, filed with
that unit), item 1's addendum on main `276dc503`, cells SW-cte-except-all and SW-cte-intersect-all. This item is the
part B finding `CTE-INSERT-REFUSE-1` (S3, over-refusal), not filed separately.

- `WITH y AS (SELECT a FROM l EXCEPT ALL SELECT a FROM r) INSERT INTO x SELECT a FROM y` and its `INTERSECT ALL` twin
  refuse on the Spark door and on native with `This feature is not implemented: Query INSERT INTO x SELECT a FROM y
  not implemented yet` (table names shortened). Spark 4.1.2 accepts both and inserts the CTE rows.
- The refusal is not the facade's alone: the native door refuses the same way.
- The `INSERT … WITH` order is accepted on both doors; its stored multisets are wrong, which is
  `SQL-SETOP-ALL-MULTISET-1`, filed by its own unit.

**Proposed fix:** one pin per item. The IN-subquery guard is recorded as deliberate. The refusal-text items follow the refusal-text classes that the label allows as cards.

**Close:** each item pinned to Spark's answer or condition, or recorded with the owner's decision.
