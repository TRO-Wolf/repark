# Card MERGE-SCHEMA-EVOLUTION-SPELLING-1: the facade does not expand `MERGE WITH SCHEMA EVOLUTION INTO ns.t`, so a two-part target refuses where Spark executes

**Date:** 2026-10-29. **Filed by:** the SPARK-DOOR-TEMP-VIEW-TARGET-1 clerk round 2 (`claude-haiku-5-5`), from the fix round's step-0 measurement (the one remaining difference of the 328-case probe, not a view target).

**Status:** open. Grade C, P2. Not dispatched; awaiting the owner's charter.

**Why:** RePark refuses a statement that Spark answers. No wrong value is produced: RePark refuses with the three-part-name text and leaves the table unchanged, while Spark executes the statement. The spelling is a pre-existing gap in the facade and is outside the temporary-view unit; the fix round's step 0 found it while measuring the MERGE spelling of a view name.

**Site:** `python/repark/src/repark/spark/session/session_core.py`, `_expand_merge_into_sql` (the facade's MERGE INTO target expansion, which runs before the native door). The expansion does not recognise the `WITH SCHEMA EVOLUTION` spelling between `MERGE` and `INTO`, so a two-part name reaches the native MERGE, which requires a three-part target. The Spark door's view check accepts the spelling (SPARK-DOOR-TEMP-VIEW-TARGET-1, fix round 1); the facade does not.

**Fix shape (not built):** the facade's MERGE target expansion accepts the `WITH SCHEMA EVOLUTION` spelling and qualifies its target the way it qualifies the plain spelling, so the statement reaches the native door in the same shape as `MERGE INTO`. The change pins the two-part and the three-part spellings with the schema-evolution clause, and the view refusal of the same spelling (fixed by SPARK-DOOR-TEMP-VIEW-TARGET-1) must still hold after it.

**Measured (2026-10-29, Spark 4.1.2 with Iceberg 1.11.0, copied from the fix round's step 0; RePark at `3243444a`):** `MERGE WITH SCHEMA EVOLUTION INTO ns.t …` executes on Spark and the table reads `1,3`. On RePark the same statement refuses with ``MERGE INTO target must be a three-part `catalog.namespace.table` name, got `ns.t` `` and the table is unchanged. The other 327 of the 328 probe outcomes match Spark.

**Retires:** the card when `MERGE WITH SCHEMA EVOLUTION INTO ns.t` executes on the facade and matches Spark's table contents, pinned, and the view refusal of the same spelling still holds.

**Relation:** [SPARK-DOOR-TEMP-VIEW-TARGET-1 card](spark-door-temp-view-target-1-card-2026-10-29.md) (its fix round's F-4 leftovers; the two-part lead is ledger clause C-017); [SPARK-DOOR-TEMP-VIEW-TARGET-1 ledger](../../ledgers/completed/spark-door-temp-view-target-1-ledger.md).
