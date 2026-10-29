# Card WRITER-PATH-TEMP-VIEW-TARGET-1: the DataFrame writer paths do not consult temporary views, and bare names on the Rust door do not follow USE

**Date:** 2026-10-29. **Filed by:** the SPARK-DOOR-TEMP-VIEW-TARGET-1 clerk round (`claude-haiku-5-5`), from the builder's two leads.

**Status:** open. Grade C. Not dispatched; awaiting the owner's charter.

**Why:** The parent unit refuses a SQL write whose one-part target is a temporary view name (INSERT, INSERT OVERWRITE, TRUNCATE TABLE, UPDATE, MERGE INTO and DELETE). The DataFrame writer paths `insertInto`, `writeTo` and `saveAsTable` still resolve their target with `prefer_temp_view=False`, so on those paths a view with the same name as a table may still be shadowed by the table. The builder expects Spark's `insertInto` onto a temporary view to refuse, as its SQL form does; that expectation is not measured.

**Second lead:** on the Rust door alone, a bare name does not follow `USE`. DataFusion's default catalog does not move with `USE`, so a bare name resolves to the temporary-view home. This is pre-existing and the facade compensates for it. The builder saw it while writing the Rust pins of the parent unit: a bare-name control fails there with DataFusion's own text.

**Measured:** none for the three writer paths. The Rust-door `USE` behaviour was observed by the builder while writing the parent unit's Rust pins (2026-10-29), not as a Spark comparison.

**Fix (not built):** measure Spark 4.1.2's answers for `insertInto`, `writeTo(...).append()` and `saveAsTable` onto a temporary view with the table in scope. If Spark refuses a form, route that writer path through the same check with `prefer_temp_view=True`, with a pin per form. The Rust-door `USE` gap is in this card for the record: fix it only if the measurement shows a SQL or writer route reaching the wrong object.

**Retires:** the card when each measured writer form matches Spark with a temporary view in scope, each form pinned, and the Rust-door `USE` behaviour is either fixed or recorded as matching Spark.

**Relation:** [SPARK-DOOR-TEMP-VIEW-TARGET-1 card](spark-door-temp-view-target-1-card-2026-10-29.md); [SPARK-DOOR-TEMP-VIEW-TARGET-1 ledger](../../ledgers/staging/spark-door-temp-view-target-1-ledger.md), clause C-010.
