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

**Proposed fix:** one pin per item. The IN-subquery guard is recorded as deliberate. The refusal-text items follow the refusal-text classes that the label allows as cards.

**Close:** each item pinned to Spark's answer or condition, or recorded with the owner's decision.
