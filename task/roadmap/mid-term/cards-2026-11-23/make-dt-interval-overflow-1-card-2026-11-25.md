# Card MAKE-DT-INTERVAL-OVERFLOW-1: `make_dt_interval` answers NULL on overflow and types its result as a string

**Date:** 2026-11-25. **Filed by:** the orchestrator (`claude-opus-5-5`), from the separate shapes on the
TRY-CAST-NONNULL-FIELD-1 card, re-measured here. **Severity:** S1 (owner, 2026-11-25): a silent wrong read. Where
Spark raises, RePark answers NULL, and where Spark answers a large interval, RePark answers NULL too.
**Status:** fixed on `fix/make-dt-interval-overflow-1` (head `c17c744af`), not merged.

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

## Fix (2026-11-25)

RePark's own `make_dt_interval` (`crates/repark-functions/src/temporal_ctor/day_time.rs`) replaces the upstream kernel
by name: Spark's exact adds raise `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` in both modes, every in-range value
answers, seconds follow Spark's implicit `decimal(18, 6)` cast, and the field is nullable as Spark declares it.
`Duration(µs)` is named and typed `interval day to second` (`crates/repark-spark/src/type_table.rs`,
`python/repark/src/repark/spark/column_fields.py`). Measured over 1434 cells in both ANSI modes: 1010 fixed, 70 equal
on both, 0 moved off Spark. Pins: `python/repark/tests/sql/test_make_dt_interval_overflow_1.py` (1064 tests: 1004
red on main, 60 controls) and 14 Rust tests in `day_time_tests.rs`. Evidence:
[task/wo/make-dt-interval-overflow-1/evidence/](../../../wo/make-dt-interval-overflow-1/evidence/summary.md).

## Still unlike Spark (measured on head)

The full list with cell counts is in the evidence summary. The shapes that touch this function:

- **Parquet, a narrowing projection under a filter.** `SELECT id, make_dt_interval(d, h, m, s) FROM pq WHERE id = 1`
  raises the overflow of a filtered-out row (20 cells), where Spark answers the kept row. DataFusion's
  `FilterExec::try_swapping_with_projection` moves the projection below the filter. Main does the same for every
  raising expression (`CAST(d + h + m AS SMALLINT)` raises `CAST_OVERFLOW`), so this needs its own card.
  In 14 of the 20 cells (7 per ANSI mode, `pq#1`, `#4`, `#7` to `#10` and `#13`) main answered Spark's rows under the
  `string` type, so these cells keep their values on main and raise on head. No cell equal to Spark on main moves.
- **`F.expr` decimal literals.** `F.expr("make_dt_interval(0, 0, 0, 0.1)")` types `0.1` as `double`, so `r` is nullable
  (Spark: not nullable), and `-999999999999.999999` rounds to `-1E12` and answers NULL or raises (44 cells).
- **SQL `999999999999.9999D`** arrives as `1E12` and answers NULL or raises (2 cells).
- **`CAST(x AS STRING)`** renders `1 days 2 hours 3 mins 4.500000 secs`, not `INTERVAL '1 02:03:04.5' DAY TO SECOND`.
- **`extract`** answers totals (`26` hours) rather than fields (`2`).
- **Refused consumers:** `-x`, `x * 2`, `x / 2`, `DATE + x`, `typeof(x)`, `avg(x)`, `try_add(x, y)`, and strings or
  decimals as days, hours or minutes.
- **Writes:** CTAS into Iceberg refuses on both engines with different text; `write.parquet` stores `duration[us]`
  where Spark stores `int64`.

Up: [map.md](map.md).
