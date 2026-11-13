# Card SPARK-DISTRIBUTE-BY-1: `DISTRIBUTE BY` refuses on both doors

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`), under owner ruling Q8 of 2026-11-12 ("Go with
the defaults"). **Source:** SIGNED-ZERO-NORMALIZE-1 part B evidence (`task/wo/signed-zero-normalize-1/evidence/part-b/`,
filed with that unit): step 0 on main `ae247767`, re-measured by item 1 on main `276dc503`, item 1 finding F1.

**Status:** filed 2026-11-12, not dispatched. Severity **S3, over-refusal**: loud, no table changes.

**Measured on main `276dc503` against Spark 4.1.2:**

- GB-10, Spark door: `SELECT d, count(*) AS c FROM (SELECT d FROM t DISTRIBUTE BY d) AS x GROUP BY d` refuses with
  `This feature is not implemented: Physical plan does not support DistributeBy partitioning`, for DOUBLE, FLOAT and
  the decimal twin. Spark answers.
- X-repartition (item 1's plan-shape cells): the native door refuses `DISTRIBUTE BY` with the same text.
- The typed `repartition(4, 'd')` answers, and leaves no `Repartition` node in the optimized plan
  (X-repartition-typed).

**Fix direction:** DataFusion's physical planner has no arm for `Partitioning::DistributeBy`. Lower `DISTRIBUTE BY`
to a hash repartition on its expressions before physical planning; it changes no result in Spark, only the
partitioning. Spark normalises float keys in that repartition, so once the node is reachable, part B part 1's
`Repartition` seat needs a measured cell; today its pin is a hand-built plan (F1). Pin GB-10 on both doors.

**Close:** fixed with that pin, or declined by a recorded ruling.
