# Card AGG-ORDER-BY-ACCEPTED-1: aggregate `ORDER BY` is accepted on every RePark door where Spark refuses the syntax

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** FEXPR-WINDOW-NULL-ORDER-1
(PR #1316) verifiers 1 and 2, 2026-11-13 (finding F1 in each); the unit's own
[card](fexpr-window-null-order-1-card-2026-11-12.md), finding F1; FILTER-STRING-NULL-ORDER-1 builder, 2026-11-13
(finding F1).

**Status:** filed 2026-11-13, not dispatched. Severity **S3** under the owner's ruling of 2026-11-13 (verbatim
"proceed with defaults"): refuse the syntax on every door, after the label.

## Measured

Spark 4.1.2 raises `PARSE_SYNTAX_ERROR` on every door for `array_agg(id ORDER BY i)`, `array_agg(id ORDER BY kd DESC)`,
`first_value(id ORDER BY i DESC)`, `string_agg(s, ',' ORDER BY s DESC)` and
`element_at(array_agg(… ORDER BY kc DESC), 1)`. RePark accepts every one of them.

On #1316's head `35c5de77`:

- `F.expr` through `agg` and `groupBy().agg` orders NULLs Spark's way (`nulls_min`):
  `array_agg(id ORDER BY i)` answers `[2,6,1,4,3,5]`, and `first_value(id ORDER BY i DESC)` answers 5.
- `selectExpr`, `spark.sql`, the native door and `rollup()`/`cube()` (which re-plan the text through the Spark door)
  keep `nulls_max`: `[6,1,4,3,5,2]` and 2. So the `F.expr` door disagrees with itself between `groupBy` and `rollup`.
- `string_agg(… ORDER BY s DESC)` shows no movement, because the NULL value is dropped.

On the base, main `a3f8c7d9`, every door placed NULLs largest, so the doors agreed.

**It reaches a write, only for text Spark rejects.** Over `(1,10,1),(2,NULL,1),(3,30,2),(4,20,2),(5,NULL,2)`,
`groupBy('g').agg(F.expr('first_value(id ORDER BY i DESC)')).writeTo(…).create()` stores `{1:1, 2:3}` on the head and
`{1:2, 2:5}` on the base. A `spark.sql` CTAS of the same text stores `{1:2, 2:5}` on both. `array_agg` written the same
way moves too. `array_agg(… ORDER BY …) OVER (…)` refuses on both builds. No write that Spark accepts can carry this
syntax, so no Spark-accepted write changes.

**The string filter.** On the FILTER-STRING-NULL-ORDER-1 branch (`fix/filter-string-null-order-1`, not on main at
filing), an aggregate `ORDER BY` inside a subquery of a string predicate (`array_agg(a ORDER BY a)`) orders NULLs like
`F.expr`, while `spark.sql` and `selectExpr` keep `nulls_max`.

## Fix direction

The owner's ruling: refuse aggregate `ORDER BY` on every door, after the label. The verifiers' alternative, giving
the Spark door's AST pass function-argument `ORDER BY`, is not the ruled direction.

## Close

The refusal lands on every door (`F.expr` in `select`, `agg`, `groupBy().agg` and `rollup`/`cube`, `selectExpr`,
`spark.sql`, the string filter and the native door), with one pinned cell per door and the `WITHIN GROUP` forms Spark
accepts kept as controls.
