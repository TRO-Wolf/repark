# Card TRY-ARITH-DECIMAL-INT-1: the `try_*` functions cast a DECIMAL to `INT` beside an integer

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's afternoon ruling of 2026-11-17 (about 14:05 UTC), ruling 6 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** the AVG-DECIMAL-ROUND-1 step 0, merged as #1352 (`9c96a963`): [sketch §4 seat A3 and §6, Q6](../../../wo/avg-decimal-round-1/sketch.md) and the [domain table](../../../wo/avg-decimal-round-1/domain-table.md), its "`try_*` with a decimal and an integer" section (measured on main `a792ce47`).

**Status:** filed 2026-11-17, not dispatched.

**Severity: S1, label-blocking** (ruled 2026-11-17, ruling 6). Its place in the order is not ruled.

## Finding

`try_add`, `try_subtract`, `try_multiply`, `try_divide` and `try_mod` of a DECIMAL and an integer cast the decimal to `INT` on RePark. Spark keeps the decimal type and its digits. A value past `INT` raises `Cast error` on RePark.

## Measured

Spark door, `d10_2`, rows 1 to 9. Row 10, the column maximum, makes every `try_*` raise on RePark; those are the `R` cells below.

| call | Spark 4.1.2 | RePark |
|---|---|---|
| `try_add(2.51, 1)` (row 3 of `d10_2`) | `decimal(11,2)`, `3.51` | `int`, `3` |
| `try_subtract(2.51, 1)` | `decimal(11,2)`, `1.51` | `int`, `1` |
| `try_multiply(2.51, 3)` | `decimal(12,2)`, `7.53` | `int`, `6` |
| `try_divide(2.51, 3)` | `decimal(14,6)`, `0.836667` | `double`, `0.6666666666666666` |
| `try_mod(2.51, 3)` | `decimal(3,2)`, `2.51` | `int`, `2` |

Over the 100 `try_*` cells per mode: `T` 8 (scale 0, values equal, type only), `V` 47 and `R` 45, the same with ANSI off. The sketch cites them as `try/try_*_rows1to9/*` (sketch §6, Q6).

## Seat (A3)

`coerce_pair` at `crates/repark-functions/src/try_invert/arith.rs:263-315` (inventory). A decimal beside an integer matches neither the both-decimal arm (`:266`) nor the both-non-decimal arm (`:269-276`), so it falls to `integer_width` (`:306-314`), which casts the decimal. The result types are at `:187-210` (`result_type`).

The fix the step 0 sketches (sketch §4, A3): a decimal-with-integer arm ahead of `integer_width`, with the integer taking its decimal form (a literal by its digits), for all five functions, each taking Spark's result type for its operator (`try_mod` the `Remainder` rule).

## Scope

All five functions, as ruled. `try_avg` is not in this card: it reaches the averager, which is AVG-DECIMAL-ROUND-1's seat.

## Not this card

DECIMAL-DIVIDE-LITERAL-1's `/` literal width is a different seat (`SparkDecimalPrecision`, `decimal_precision.rs:65-70`). The sketch keeps the two apart (sketch §5, unit 3).

Up: [map.md](map.md).
