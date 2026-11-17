# Card DECIMAL-MULTIPLY-OVERFLOW-1: `d38_10 * 3` raises an Arrow overflow where Spark answers `decimal(38,8)`

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's afternoon ruling of 2026-11-17 (about 14:05 UTC), ruling 8 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** the AVG-DECIMAL-ROUND-1 step 0, merged as #1352 (`9c96a963`): [sketch §6, Q8](../../../wo/avg-decimal-round-1/sketch.md) and the [domain table](../../../wo/avg-decimal-round-1/domain-table.md) ("Overflow at 38 digits" and "Other causes").

**Status:** filed 2026-11-17, not dispatched. **Not label-blocking.** It sits outside AVG-DECIMAL-ROUND-1 and DECIMAL-DIVIDE-LITERAL-1 (ruling 8).

**Severity: S2** (ruled 2026-11-17, ruling 8).

## Finding

On a `DECIMAL(38,10)` column, `d38_10 * 3` and `d38_10 * -3` raise `Arrow error: Arithmetic overflow` on RePark, in both ANSI modes. Spark answers, with the result type `decimal(38,8)`.

## Cells

- `divctl/mul/d38_10/*` and `shape/avg_mul/d38_*` (sketch §6, Q8).
- Control `d * 3` and `d * -3` at `d38_10`: `R` in both modes (domain table, "Division and controls"). The DataFrame control `col(d) * 3` at `d38_10`: `R`. At `d38_0` and `d38_2` the same SQL cells are `B` with ANSI on and `R` with ANSI off.

## Seat

Not diagnosed by the step 0. The nearest inventory row is `SparkDecimalExprPlanner::plan_binary_op` (`crates/repark-functions/src/decimal_spark.rs:60-89`), the SQL-text `*` past Arrow's limit (DEC-8). The inventory marks it "no" for the two units, and does not say whether it is this cell's seat. The DataFrame door never plans SQL text (inventory), so the DataFrame cell cannot pass through that row.

Up: [map.md](map.md).
