# Card TEMP-VIEW-NAME-UNICODE-FOLD-1: a view name with a letter newer than the JVM's Unicode version folds on RePark and not on Spark

**Date:** 2026-11-01. **Filed by:** the third-audit records round (`claude-haiku-5-5`), from the targeted re-audit of the format-v2 label, finding AUD-019 ([the report](../../../docs/audits/iceberg-v2-label-targeted-reaudit-2026-11-01.md), section 5).

**Status:** open. Grade C (clerk). P3. Not dispatched; no build is owed until the owner charters it. Not a label blocker (ledger clause C-083).

**Why:** RePark folds a temporary view's name with Rust's `str::to_lowercase`. A name holding a letter added in Unicode 14 (U+A7C0) folds to the same key as its table, so DML that names the table is refused as if it named a view (`EXPECT_TABLE_NOT_VIEW`). Spark on JDK 17, whose Unicode is version 13, does not fold that letter and writes the table. The answer depends on the JVM: on JDK 21 Spark would fold it, which the audit did not measure. The name rule in ledger clause C-075 does not bound the Unicode version. Names with `Ä`, a final sigma and a dotted capital I are equal on both engines.

**Site:** `temp_view_key` (Rust `str::to_lowercase`).

**Fix:** the fold of a temporary view's name matches Spark on the oracle's JVM for letters newer than that JVM's Unicode version, pinned on the audit's DELETE, INSERT, UPDATE and TRUNCATE statements with Spark's expected values. The charter first decides whether the JDK 21 answer must match as well; the unit measures it before a pin claims it.

**Measured (the third audit; copied, from its `p3_view_matrix.py` cases `X.ci.newuni.*`):** a view named with U+A7C0 over a table named with the same letter in lower case. `DELETE` through the table's name: RePark refuses with `EXPECT_TABLE_NOT_VIEW` and the table stays `[1,2,3]`; Spark deletes id 2 and the table reads `[1,3]`. `INSERT`, `UPDATE` and `TRUNCATE` show the same pattern.

**Retires:** the card when the fold matches Spark on the oracle's JVM with its pins green, or when the owner rules the JVM-dependent divergence kept, with a date. The label's AUD-019 row (ledger clause C-083) then closes by citation.

**Relation:** [TEMP-VIEW-NAME-VALIDATION-1](temp-view-name-validation-1-card-2026-10-30.md), the name grammar; [SPARK-DOOR-TEMP-VIEW-TARGET-1](spark-door-temp-view-target-1-card-2026-10-29.md), the target refusal these names reach.
