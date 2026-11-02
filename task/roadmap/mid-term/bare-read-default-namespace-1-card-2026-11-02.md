# Card BARE-READ-DEFAULT-NAMESPACE-1: a bare read the facade cannot qualify resolves against the DataFusion default namespace, not the current one

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 round-4 records round (`claude-haiku-5-5`), for the orchestrator, from the Opus 5.5 round-4 verdict's lead L9.

**Status:** open. Grade C, P3. Not built; not dispatched; awaiting the owner's charter.

**Why:** With only an Iceberg table `1t` in the current namespace and no temporary view of that name, a bare read the facade cannot qualify resolves against `datafusion.public`. The round-4 verifier measured `SELECT * FROM 1t`, `DESCRIBE 1t`, `SELECT * FROM select`, `values` and `lateral` each answering ``table 'datafusion.public.1t' not found``, where Spark 4.1.2 reads the table (verdict 4, Leads, L9; measured at `d35a792b`). The same answer follows `DROP TABLE 1t`, or a `RENAME TO` that removes the view. These are read errors only: no table changes, and the gap predates SPARK-DOOR-TEMP-VIEW-DDL-1. The cause is not traced here; the answer is measured, not explained.

**Fix shape (not built):** measure the bare-read shapes on Spark 4.1.2 and on RePark, on both catalogs, with the table present and absent and after a view is dropped or renamed. Then pin the measured outcome and make RePark match it, or decide the refusal text with the charter. Do not change the read route without that measurement.

**Retires:** the card when a bare read of a digit-leading table name answers as Spark 4.1.2 does, pinned for the measured shapes, or the charter declines it.

**Relation:** [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (lead L9, carded in the round-4 records; not that unit's claim).
