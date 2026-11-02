# Card TEMP-VIEW-WRITE-REFUSAL-SQLSTATE-SPACING-1: the AUD-006 write refusals print one space before SQLSTATE where Spark 4.1.2 prints two

**Date:** 2026-11-02. **Filed by:** the SPARK-DOOR-TEMP-VIEW-DDL-1 records round (`claude-haiku-5-5`), for the orchestrator, from the builder's leads in that unit's hand-back.

**Status:** open. Grade C, P3. Not built; not dispatched; awaiting the owner's charter.

**Why:** Spark 4.1.2's `EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE` text carries two spaces before `SQLSTATE`, because its alternative sentence is empty. The ALTER refusal texts of SPARK-DOOR-TEMP-VIEW-DDL-1 copy that spacing. The AUD-006 write refusals (`expects_table` in `crates/repark-spark/src/view_ddl/temp_write.rs`) print one space. Their facade pins match a substring, so they do not see the difference. The difference is in the message text only: no table changes and no value is produced.

**Fix shape (not built):** confirm the spacing on Spark 4.1.2 for each NO_ALTERNATIVE write route (INSERT, INSERT OVERWRITE, TRUNCATE TABLE) before any change; then make the one-character change in `expects_table` and tighten the pins to the two-space text. A text-only change.

**Retires:** the card when the AUD-006 write refusals print Spark's text byte for byte, with a pin that asserts the two spaces.

**Relation:** [SPARK-DOOR-TEMP-VIEW-TARGET-1 card](spark-door-temp-view-target-1-card-2026-10-29.md) and its [unit ledger](../../ledgers/completed/spark-door-temp-view-target-1-ledger.md) (the write refusals this text lives in); [SPARK-DOOR-TEMP-VIEW-DDL-1 ledger](../../ledgers/staging/spark-door-temp-view-ddl-1-ledger.md) (lead L2; this unit's ALTER texts already copy Spark's spacing).
