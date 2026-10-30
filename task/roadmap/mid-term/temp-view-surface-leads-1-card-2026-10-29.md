# Card TEMP-VIEW-SURFACE-LEADS-1: three pre-existing temporary-view class and listing leads from the SPARK-DOOR-TEMP-VIEW-TARGET-1 probe

**Date:** 2026-10-29. **Filed by:** the SPARK-DOOR-TEMP-VIEW-TARGET-1 clerk round 3 (`claude-haiku-5-5`), from the fix round 2 step-0 probe on Spark 4.1.2 (484 cases) and the builder's clerk notes.

**Status:** open. Grade C. Not built; not dispatched; awaiting the owner's charter. Each lead is loud or harmless: none changes a table.

**Why:** the temporary-view unit's probe reached three places where RePark's surface differs from Spark 4.1.2 and that the unit does not own. Each is pre-existing and outside the unit's write-target fix. They are carded as one card because they share one origin, the same probe and the same grade, and each is small.

**Lead 1, `SHOW VIEWS` lists internal scratch views.** RePark's `SHOW VIEWS` lists the internal `__repark_cdf_*` names that its createDataFrame path registers as temporary views. Spark never shows them. The probe's comparison removes those names from the RePark listing, so the 484-case count is not affected by them. Pre-existing.

**Lead 2, `createOrReplaceTempView('a.b')` refuses with a different class.** Spark 4.1.2 refuses with `TEMP_VIEW_NAME_TOO_MANY_NAME_PARTS`. RePark refuses with its own qualified-name text under a different error class. Class only; no table changes.

**Lead 3, an unquoted non-ASCII name in `tableExists` raises RePark's text.** Spark 4.1.2 raises `INVALID_IDENTIFIER` (the unquoted identifier must be back quoted). RePark raises its own "invalid table identifier" text. Class and text only; no table changes.

**Fix shape (not built):** one pin per lead on the probe's outcome, then the change that makes the listing or the error class match Spark 4.1.2. The charter decides whether each item is built, and an item the charter declines is closed as declined in this card.

**Retires:** the card when each of the three matches Spark 4.1.2's listing or error class on the probe, each item pinned, or the charter declines an item.

**Relation:** [SPARK-DOOR-TEMP-VIEW-TARGET-1 card](spark-door-temp-view-target-1-card-2026-10-29.md) and its [unit ledger](../../ledgers/staging/spark-door-temp-view-target-1-ledger.md), clauses C-022 to C-024. The probe is the fix round 2 step 0 of that ledger.
