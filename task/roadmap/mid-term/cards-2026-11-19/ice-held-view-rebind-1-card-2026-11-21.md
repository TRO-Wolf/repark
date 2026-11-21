# Card ICE-HELD-VIEW-REBIND-1: a frame built over a held DataFrame view answers silently after a drop and recreate, where Spark raises

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** the owner's ruling of 2026-11-21 (decision 1, and the D1 block), recorded in [rulings-2026-11-21.md](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-21.md).

**Rating: S3.** From the Opus re-check of #1390; recorded as a card by the owner's ruling of 2026-11-21. It is not label-blocking on its own.

**Status:** filed 2026-11-21; not dispatched.

- **The shape:** a held Iceberg frame is captured in a DataFrame temp view (`held.createOrReplaceTempView("v")`), and a frame `spark.sql("SELECT * FROM v")` is built over it before a `DROP TABLE` and recreate. Then `frame.count()`.
- **The answers:** Spark 4.1.2 raises `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH`. Main before #1390 answers 2, the old table. #1390's head answers 1, the recreated table. Both are silent.
- **The cause:** the view's scans carry a provider clone bound in view mode, and the frame's action re-binds in view mode, which skips the table-id check.
- **The fix** is open as #1391: a frame built over a view takes its table at its build, in query mode. Its records carry the sibling table. Its two verifiers run after the usage reset.

Up: [map.md](map.md).
