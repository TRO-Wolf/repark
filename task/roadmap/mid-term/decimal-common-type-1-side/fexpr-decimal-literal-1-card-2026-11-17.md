# Card FEXPR-DECIMAL-LITERAL-1: `F.expr` types an unsuffixed decimal literal as DOUBLE, where Spark types it DECIMAL

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Defaults." (2026-11-17, about 10:40 UTC), ruling 4 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** the DECIMAL-COMMON-TYPE-1 build's [bar](../../../wo/decimal-common-type-1/evidence/built/bar.md) and [bar table](../../../wo/decimal-common-type-1/evidence/built/raw/bar-table.md); Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 3, Q5.

**Status:** folded into DOUBLE-DECIMAL-DATAFRAME-1.

**Severity: S1, label-blocking** (ruled 2026-11-17). The fold into DOUBLE-DECIMAL-DATAFRAME-1 is ruled. Only `F.expr` is affected.

The bar names this finding FEXPR-DECIMAL-LITERAL, without the `-1` suffix. This card is that finding.

## Cell

`dfconst/fexpr_a_eq_lit/t`: a wrong answer in both ANSI modes. `F.expr("a = 1.25")` types `1.25` as `double`, where Spark types it `decimal(3,2)`. The pair is decimal beside double, not two decimals (bar table).

## Breadth

Fable's table, measured on main before #1341 (Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 3, Q5, ANSI on):

| cell | Spark | main |
|---|---|---|
| `F.expr("1.25")`, also through `withColumn` | `decimal(3,2)` | `double` |
| `F.expr("d * 1.1")` | `decimal(13,3)`: 3.300 | `double`: 3.3000000000000003 |
| `F.expr("0.1 + 0.2")` | `decimal(2,1)`: 0.3 | `double`: 0.30000000000000004 |
| `df.where(F.expr("d = 0.1 + 0.2"))` | one row | no rows |
| `F.expr("1.0 / 3")` | `decimal(7,6)` | `double` |
| `selectExpr("1.25 AS x")`, `selectExpr("d * 1.1")` | decimal | the same |
| `df.filter("d = 1.25")`, `df.filter("d = 0.1 + 0.2")` | rows | the same |
| `F.expr("1e2")`, `F.expr("CAST(1.25 AS DECIMAL(3,2))")` | `double`, `decimal(3,2)` | the same |

Fable's readings, as readings: it is `F.expr` alone; `selectExpr` and string filters type the literal as Spark does; it gives wrong rows, not only a wrong type. "The package is old; the unit's step 0 should re-measure on current main."

## Seat

The consult's lead, not diagnosed: the `F.expr` context list runs `crates/repark-python/src/column/expr_build.rs:303-305` and `crates/repark-spark/src/spark_typed.rs:60-83`, the same seats that DOUBLE-DECIMAL-DATAFRAME-1 measures.

## Fold

Folded into [DOUBLE-DECIMAL-DATAFRAME-1](../double-decimal-dataframe-1-card-2026-11-13.md), ruling 2 and ruling 4. Its step 0 measures the breadth on current main.

Up: [map.md](map.md).
