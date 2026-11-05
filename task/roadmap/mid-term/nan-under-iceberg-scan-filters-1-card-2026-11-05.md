# Card NAN-UNDER-ICEBERG-SCAN-FILTERS-1: Spark's DELETE and UPDATE on Iceberg keep a NaN row that SQL semantics and RePark delete or change

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the format-v2 label re-audit ([report](../../../docs/audits/iceberg-v2-label-corpus-audit-2026-11-05.md), finding AUD-024).

**Status:** filed; the owner's ruling is pending. AUD-024 is S1 and blocks the format-v2 label (ledger [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) C-100), unless the owner rules it an oracle-class group (O3). Grade not set.

**Why:** On Spark 4.1.2 with Iceberg 1.11.0, the pushed scan filter for `d > 0` excludes a NaN row (the plan shows `IcebergScan filters=d > 0.0`). Spark's own SQL semantics do not: `CAST('NaN' AS DOUBLE) > 0` is true on a VALUES relation and on a projection of the same table, so NaN is larger than every number there. Two readings are therefore on record. Reading one, Spark's SELECT on the table answers without the NaN row, and Spark's DELETE and UPDATE with `WHERE d > 0` keep it, under merge-on-read and copy-on-write. Reading two, SQL semantics as RePark implements them, the NaN row is selected: RePark's SELECT includes it, and RePark's DELETE and UPDATE remove or change it. The survivors therefore differ from Spark's. `DELETE WHERE d >= CAST('NaN' AS DOUBLE)` refuses on Spark with `Cannot create expression literal from NaN`, and RePark deletes the row.

**Options for the owner:** (a) keep the divergence, state it in the label and pin the stated behaviour per door; (b) match Iceberg's evaluator for DML, so NaN comparisons are false in DELETE and UPDATE, while the SELECT answers stay as they are, and re-measure.

**Close:** the owner's ruling on the oracle class, then either the stated divergence with its pins, or the DML match with its pins and a targeted re-audit of AUD-024.
