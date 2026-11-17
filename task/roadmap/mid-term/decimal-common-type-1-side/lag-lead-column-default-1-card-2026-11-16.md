# Card LAG-LEAD-COLUMN-DEFAULT-1: `lag` and `lead` with a column default refuse, where Spark answers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Merge those and defaults." (2026-11-16, about 23:40 UTC), ruling B7 on DECIMAL-COMMON-TYPE-1 step 0 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md)). **Source:** [domain table](../../../wo/decimal-common-type-1/domain-table.md), the "Other causes" table and the position grid's `lag default` and `lead default` rows; [sketch §9 and §10 question 7](../../../wo/decimal-common-type-1/sketch.md). Parent: [DECIMAL-DECIMAL-UNION-WIDEN-1](../decimal-decimal-union-widen-1-card-2026-11-16.md).

**Status:** filed 2026-11-16, not dispatched. **Not label-blocking** (ruled B7).

**Severity: S3** (ruled B7). A refusal where Spark answers, on a window with a column as the default.

## Finding

With a column as the default argument of `lag` or `lead`, main refuses at planning with `only support Literal types`. Spark answers. The same refusal holds for a same-type control, so the defect is not specific to decimals.

## Measured

| cell | Spark 4.1.2 | main |
|---|---|---|
| `lag default` and `lead default` rows, pairs `ab`, `ac`, `gh`, `i64h`, `i32w30` | answers | refuses (`R`) |
| the same rows, control pair `c0c` | answers | refuses (`R`) |
| side cell `lag_same_type_default` | answers | refuses |

The grid note says ANSI on is identical cell for cell.

## Seat (not diagnosed)

Not diagnosed. The refusal names the literal-only check on the default argument.

Up: [map.md](map.md).
