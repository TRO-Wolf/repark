# Card NOT-BETWEEN-DML-CSE-1: DELETE and UPDATE through NOT BETWEEN refuse with a missing common-expression column

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from round 1 finding B1 of the Opus 5.5 verifier on PR #1295.

**Status:** filed 2026-11-11, not dispatched. Severity **S3**. Pre-existing: base and head refuse the same way, so it is not a regression.

**Measured:**

- `DELETE` and `UPDATE ... WHERE s NOT BETWEEN r AND r`, and the `NOT (... BETWEEN ...)` form, refuse on all three table formats with `Schema error: No field named __common_expr_3`. The snapshot is unchanged.
- The `INT` twin, `id NOT BETWEEN v - 1 AND v - 1`, refuses the same way.
- Spark answers both. The cause is common-subexpression elimination on the DML path.

**Close:** the `DELETE` and `UPDATE` forms through `NOT BETWEEN` answer as Spark does on the three formats, with a pin.
