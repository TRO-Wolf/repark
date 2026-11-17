# Card DECIMAL-PRECISION-LOSS-CONF-1: `spark.sql.decimalOperations.allowPrecisionLoss=false` is accepted and ignored

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's afternoon ruling of 2026-11-17 (about 14:05 UTC), ruling 5 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** the AVG-DECIMAL-ROUND-1 step 0, merged as #1352 (`9c96a963`): [sketch §6, Q5](../../../wo/avg-decimal-round-1/sketch.md), the [domain table](../../../wo/avg-decimal-round-1/domain-table.md) ("The confs") and the [inventory](../../../wo/avg-decimal-round-1/inventory.md) ("The confs").

**Status:** filed 2026-11-17, not dispatched. **Not label-blocking** (ruling 5).

**Severity: S2** (ruled 2026-11-17, ruling 5).

## Finding

RePark accepts `spark.sql.decimalOperations.allowPrecisionLoss=false`, reads it back, and ignores it. Spark's lossless formula gives the wide decimal results a different type. Spark raises on some division cells where the narrower result type cannot hold row 10, and RePark answers.

## Cells

- `apl_false` runs, division SQL, ANSI on: 53 `S` and 96 `V` (sketch §6, Q5). Under the default, the same cells are 101 `V` and no `S` (domain table, "The confs").
- Multiplication and addition at 38 digits differ under the conf too (domain table, "The confs").

## Seat

Spark: `Divide.resultDecimalType` with precision loss disabled (sketch §2). RePark: `spark_result_type` and `spark_div_result_type` (`crates/repark-functions/src/decimal_precision.rs:246-252`) are the `allowPrecisionLoss=true` formulas only, and no rule reads the conf (inventory, "The confs").

## Scope

Outside both units. The conf changes every operator's formula, so the sketch keeps it out (sketch §4 A2, owner question 5).

Up: [map.md](map.md).
