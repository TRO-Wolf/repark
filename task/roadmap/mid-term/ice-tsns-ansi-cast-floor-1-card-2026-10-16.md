# Card ICE-TSNS-ANSI-CAST-FLOOR-1: on the ANSI doors a nanosecond cast to TIMESTAMP truncates toward zero

**Date:** 2026-10-16. **Filed by:** Grok 4.7, clerk, under owner ruling 120 (a).

**Status:** open. Not scheduled. Does not block U-1 (ICE-TSNS-TYPE-PLANNER-1, merged 2026-10-16 as `c7121437`).

**Retires:** when `CAST(ns AS TIMESTAMP)` and `CAST(ns AS TIMESTAMPTZ)` floor a pre-1970 sub-microsecond value on the ANSI doors (`repark.sql()` and `repark_sql::AnsiDialect`), with a pin per door.

**Registry row:** [ICE-TSNS-SQL-1-R-025](../../../docs/spark-sql-iceberg-parity.md) (dated row 2026-10-16, owner ruling 120 (a)).

## Risk

A nanosecond value before 1970 with sub-microsecond digits narrows to the wrong microsecond on the ANSI doors: 1969-12-31 23:59:59.999999999 becomes 1970-01-01 00:00:00 instead of 1969-12-31 23:59:59.999999.

## Sequence

- `repark.sql("SELECT CAST(CAST('1969-12-31 23:59:59.999999999' AS timestamp_ns) AS TIMESTAMP) AS v")` answers `timestamp[us]` 0 (1970-01-01 00:00:00).
- A `repark_sql::AnsiDialect` session over a batch with `ns = -1` answers 0 for `CAST(ns AS TIMESTAMP)` and `CAST(ns AS TIMESTAMPTZ)`.
- The same door's literal `TIMESTAMP '1969-12-31 23:59:59.999999999'` answers -1 µs, which floors. The Spark door floors both `CAST(ns AS TIMESTAMP_NTZ)` and `CAST(ns AS TIMESTAMP)`.
- Expected under Q-21c-8: -1 µs (1969-12-31 23:59:59.999999), since every narrowing floors on every door, and ruling 94 makes ANSI `TIMESTAMP` microseconds.

## Against main

Main typed ANSI `TIMESTAMP` as nanoseconds, so the expression answered -1 ns exactly, which is the wrong type under Q6. Main's ANSI store into a `TIMESTAMP(6)` column already truncated -1 ns to 0, and U-1 stores the same 0. Stored values are no worse than main. No test pins a pre-1970 sub-microsecond narrowing on an ANSI door.

## Note

Measured by the U-1 verifier on 2026-10-16 (finding F-1) with an uncommitted probe. Not re-run for this card.
