# Card MAKE-DT-INTERVAL-OVERFLOW-1: `make_dt_interval` answers NULL on overflow and types its result as a string

**Date:** 2026-11-25. **Filed by:** the orchestrator (`claude-opus-5-5`), from the separate shapes on the
TRY-CAST-NONNULL-FIELD-1 card, re-measured here. **Severity:** S1 (owner, 2026-11-25): a silent wrong read. Where
Spark raises, RePark answers NULL, and where Spark answers a large interval, RePark answers NULL too.
**Status:** filed, not dispatched.

## Measured

Measured on live Spark 4.1.2 and on a build of main `14d2404a6` (#1429's tested head), in the UTC session zone. The
same answers hold in both ANSI modes. `i` is an `int` column holding `1` and `2147483647`; the sources are an
in-memory frame and a Parquet file.

| Statement | Spark 4.1.2 | Main |
|---|---|---|
| `SELECT id, make_dt_interval(i, 0, 0, 0.0) AS r FROM t` | `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` | **`1 day`, then `NULL`**; `r` is `string` |
| `SELECT id, make_dt_interval(0, i, 0, 0.0) AS r FROM t` | `1:00:00`, then `89478485 days, 7:00:00`; `r` is `interval day to second` | **`1:00:00`, then `NULL`**; `r` is `string` |
| `SELECT make_dt_interval(2147483647, 0, 0, 0.0) AS r` | `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` | **`NULL`**; `r` is `string` |

The TRY-CAST-NONNULL-FIELD-1 card records two more cells of the same function: over an Iceberg NOT NULL column the
overflow crashes (`Column 'r' is declared as non-nullable`), and a NaN seconds argument does the same.

## Seat

`crates/repark-functions/src/expr_fn.rs:677` builds the call from `datafusion_spark`'s `make_dt_interval`, which
returns NULL when the day-time interval overflows and whose result the facade types as a string. Spark's
`IntervalUtils.makeDayTimeInterval` raises the overflow and returns `DayTimeIntervalType(DAY, SECOND)`.

## First step

Measure the domain first: each argument at its overflow edge, negative values, fractional seconds, NULL and NaN
arguments, every door, Parquet, Iceberg and in-memory sources, `dtypes` and the stored type of a CTAS. Then decide
whether a RePark UDF replaces the call. Patching the `datafusion-spark` crate is out of scope.

Up: [map.md](map.md).
