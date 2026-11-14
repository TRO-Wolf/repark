# Card DOUBLE-DECIMAL-DATAFRAME-1: two DataFrame doors still round a DOUBLE into DECIMAL by Arrow's product

**Date:** 2026-11-13. **Filed by:** the DOUBLE-DECIMAL-STORE-ROUND-1 builder (`claude-opus-5-5`) under the owner's
ruling of 2026-11-13, about 20:50 UTC, verbatim "defaults" (these two doors are not in that unit; they get a
label-blocking card). **Sources:** that unit's inventory critic (M6, M7) and
[its evidence](../../wo/double-decimal-store-round-1/map.md).

**Status:** filed, not dispatched. Severity **S1** (a wrong write through `writeTo`). **Label-blocking.**

## Measured (main, RePark against Spark 4.1.2, ANSI on; the critic's probes)

| door | RePark | Spark | why |
|---|---|---|---|
| `F.expr("CAST(1.255D AS DECIMAL(10,2))")` | 1.25 | 1.26 | the F.expr context list runs `fold_spark_numeric_casts` (`crates/repark-spark/src/spark_typed.rs:60-83`) before the decimal wrapper (`crates/repark-python/src/column/expr_build.rs:303` vs `:305`), so the float literal folds through `ScalarValue::cast_to` and Arrow before the session's `bool_decimal_cast` sees it |
| `df.na.fill(1.255)` / `fillna({'v': 1.255})` on a DECIMAL(10,2) column | `1.255000000000000`, typed `decimal(30,15)` | 1.26, `decimal(10,2)` | `crates/repark-core/src/na_fill.rs:15-24` leaves DECIMAL out of `is_fill_numeric_type`, so the fill stays DOUBLE and `coalesce` coerces to `(30,15)` (`:58-66`); Spark casts the fill to the column type |

Also measured by the critic, not ruled: pandas `createDataFrame(pd.DataFrame({'v': [1.255]}), 'v decimal(10,2)')`
stores 1.25 through pyarrow's own cast (`python/repark/src/repark/spark/session/create_dataframe_arrow.py:140`); the
Spark side needs an environment with pandas.

## Close

`F.expr` float literals into DECIMAL and `na.fill`/`fillna` on a DECIMAL column answer Spark's value and type, pinned in
both ANSI modes and through `writeTo`.

Up: [map.md](map.md).
