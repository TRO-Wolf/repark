# Card AVG-DISTINCT-MIXED-1: `avg(DISTINCT x)` beside another aggregate raises where Spark answers

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's afternoon ruling of 2026-11-17 (about 14:05 UTC), ruling 4 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** the AVG-DECIMAL-ROUND-1 step 0, merged as #1352 (`9c96a963`): [sketch §6, Q4](../../../wo/avg-decimal-round-1/sketch.md), the [domain table](../../../wo/avg-decimal-round-1/domain-table.md) ("Other causes" and "After the inventory critic") and the [inventory](../../../wo/avg-decimal-round-1/inventory.md).

**Status:** filed 2026-11-17, not dispatched. **Not label-blocking** (ruling 4).

**Severity: S3** (ruled 2026-11-17, ruling 4).

## Finding

`avg(DISTINCT x)` beside a non-distinct aggregate raises `This feature is not implemented: DistinctAvgAccumulator` on RePark. Spark answers: `avg(DISTINCT c10_2)` beside `count(c10_2)` answers `0.005000` and `0.026667` on the measured groups (domain table, "Other causes"). The window form, `avg(DISTINCT c10_2) OVER (…)`, is refused on both engines, with different error classes.

## Cells

- `mixed/avg_distinct_count/*`: 13 `R` per mode (sketch §6, Q4).
- The revision's row for `avg` and `mean` `(DISTINCT x)` beside `count(x)`: 26 cells per mode, `R` 13 for `avg` and `t` 13 for `mean` (domain table, "After the inventory critic").

## Seat

`accumulator` refuses `is_distinct` at `crates/repark-functions/src/aggregate.rs:148-150`. DataFusion's `single_distinct_aggregation_to_group_by` rewrites only an all-distinct aggregate list, so a mixed list reaches the accumulator (inventory, "`avg` — result type, rounding, overflow").

## Relation to AVG-DECIMAL-ROUND-1

`mean(DISTINCT x), count(x)` answers a truncated value today. That is S1 inside AVG-DECIMAL-ROUND-1 (ruling 4, `mixed/mean_distinct_count/*`, 13 `t` per mode), and the alias fixes it there. After the alias it refuses like `avg` until this card lands.

Up: [map.md](map.md).
