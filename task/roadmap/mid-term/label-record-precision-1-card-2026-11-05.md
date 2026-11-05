# Card LABEL-RECORD-PRECISION-1: six precision corrections to the format-v2 label text

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the format-v2 label re-audit ([report](../../../docs/audits/iceberg-v2-label-corpus-audit-2026-11-05.md), finding AUD-029).

**Status:** filed; severity S3 (record precision); does not block the format-v2 label. Ledger [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) C-105.

**Why:** The coverage check of the re-audit found the label's counts correct, with these precision notes:
- (a) The family-1 composition reads as if the 42 native cells were additional. They sit inside the 84 catalog-door cells.
- (b) "280 of 280" is 270 EQUAL, 6 EQUAL_REFUSE and 4 REFUSE_TEXT. The four are native list `drop` and `rename` column cells with a different refusal text.
- (c) The writer-axis non-equal cells include 32 `eqw_boolean` cells filed under oracle group O1, where RePark keeps the row deleted and Spark resurrects it. They are not O2.
- (d) The columnresolution-views claim holds on the facade only. The native door refuses statements 1 to 17.
- (e) C-012 is parity with Java's integer parsing, not a leniency.
- (f) Facade DML answers one count row and DDL answers one empty row, where Spark answers no rows. This is the names D14 class, and it reaches DELETE, UPDATE, MERGE and DDL, not only INSERT.

**Proposed fix:** correct the label text at its next text edit, after the blocking findings are settled.

**Close:** the label text carries the six corrections, and the cells counts it cites are unchanged.
