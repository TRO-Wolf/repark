# Card UNIX-TIMESTAMP-TRUNCATE-1: `unix_timestamp` floors where Spark truncates, and `unix_seconds` truncates where Spark floors

**Date:** 2026-11-22. **Filed and built by:** the builder (`claude-opus-5-5`), branch
`fix/unix-timestamp-truncate-1` off main `c5370be6`. **Source:** the Spark audit's family `NEW-dates-4`
(32 cells, `WRONG-VALUE`, S1), its neighbour `UNIX-TIMESTAMP-STRING-TRIM-1` (absorbed here), and the
earlier card [UNIX-SECONDS-PRE-EPOCH-1](sweep-2026-11-16/unix-seconds-pre-epoch-1-card-2026-11-16.md).
**Severity:** S1, a silent wrong value in released 1.5.6.

**Status:** **FIXED on the branch** (not merged).

## The defect

Spark divides epoch microseconds by a million two ways. `UnixTimestamp` / `ToUnixTimestamp` use Java's
`/`, which truncates toward zero, for `TIMESTAMP`, `TIMESTAMP_NTZ`, `DATE` and parsed text alike.
`UnixSeconds` and `UnixMillis` use `Math.floorDiv`. RePark had them the other way round, so every
instant before 1970 with a fraction of a second was off by one: `unix_timestamp(TIMESTAMP'1969-12-31
23:59:59.5')` answered `-1` (Spark `0`) and `unix_seconds` of it answered `0` (Spark `-1`). Whole
seconds and every instant after 1970 agreed. A job that stores whole minutes between two timestamps as
`(unix_timestamp(end) - unix_timestamp(start)) / 60` stored a value one minute off when an endpoint is a
fractional instant before 1970. `F.unix_seconds` and `F.unix_millis` also computed through a `DOUBLE`,
so after about 2242 a fraction rounded up to the next second or millisecond.

The one-argument `unix_timestamp(x)` is Spark's `unix_timestamp(x, 'yyyy-MM-dd HH:mm:ss')`. RePark ran
it through a separate parser that skipped leading whitespace, accepted two spaces between date and time,
refused a wall clock inside a daylight-saving gap, and refused a `DATE` at planning; the two-argument
form already answered as Spark on every one of those inputs.

## Measured (Spark 4.1.2 and a release package of `c5370be6`)

7,854 cells per engine: `unix_timestamp` and `to_unix_timestamp` over `TIMESTAMP`, `TIMESTAMP_NTZ`, `DATE` and
`STRING` (one argument, the default pattern written out, and `yyyy-MM-dd HH:mm:ss.SSSSSS`), `unix_seconds`,
`unix_millis`, `unix_micros`, `unix_date`, `CAST(ts AS BIGINT | INT | SMALLINT | TINYINT | DOUBLE)` and the
minute-difference shape, each as a SQL literal, over a temporary view, through `F.expr` and through the
typed `F.*` function, in `UTC`, `America/Los_Angeles` and `Asia/Kolkata`, ANSI on and off. 3,252 cells
differ on `c5370be6`.

| Expression (every door, both ANSI modes) | Spark 4.1.2 | `c5370be6` | head |
|---|---|---|---|
| `unix_timestamp(TIMESTAMP'1969-12-31 23:59:59.5')`, UTC | `0` | `-1` | `0` |
| `unix_timestamp(TIMESTAMP'1960-06-01 00:00:00.25')`, UTC | `-302486399` | `-302486400` | `-302486399` |
| `unix_timestamp(TIMESTAMP'1969-12-31 15:59:59.5')`, Los Angeles | `0` | `-1` | `0` |
| `unix_timestamp` of the same `TIMESTAMP_NTZ` column, any zone | the wall `/ 10^6` | floored | truncated |
| `unix_timestamp('1969-12-31 23:59:59.999999', 'yyyy-MM-dd HH:mm:ss.SSSSSS')`, UTC | `0` | `-1` | `0` |
| `unix_seconds(TIMESTAMP'1969-12-31 23:59:59.5')`, UTC | `-1` | `0` | `-1` |
| `unix_millis(TIMESTAMP'1969-12-31 23:59:59.000001')`, Kolkata | `-19801000` | `-19800999` | `-19801000` |
| `F.unix_seconds` of `9999-12-31 23:59:59.999999` | `253402300799` | `253402300800` | `253402300799` |
| `unix_seconds(ntz)`, `unix_millis(ntz)` over a column | `UNEXPECTED_INPUT_TYPE` | a value (SQL, `F.expr`); `CAST_WITHOUT_SUGGESTION` (`F.*`) | `UNEXPECTED_INPUT_TYPE` |
| `unix_timestamp(DATE'1960-06-01')`, SQL and `F.expr` | `-302486400` (UTC) | planning error | `-302486400` |
| `unix_timestamp(' 2025-01-15 12:00:00')`, tab, two inner spaces | `NULL` / `CANNOT_PARSE_TIMESTAMP` | a value | `NULL` / `CANNOT_PARSE_TIMESTAMP` |
| `unix_timestamp('2025-03-09 02:30:00')`, Los Angeles (a gap) | `1741516200` | `NULL` / `CANNOT_PARSE_TIMESTAMP` | `1741516200` |
| `(unix_timestamp(e) - unix_timestamp(s)) / 60` with a fractional pre-1970 endpoint | Spark's minutes | one minute off | equal |
| whole seconds, every instant after 1970, `CAST(ts AS BIGINT)`, `unix_micros(ts)`, `unix_date` | | equal | equal |

The full table, the probe and the logs stay with the builder's scratch hand-back (not committed).

## The fix

- `timestamp_cast.rs`: `unix_timestamp` of a timestamp divides with `/` (`seconds_truncated_from_ticks`),
  and the one-argument form runs the two-argument seat with Spark's default pattern, so `DATE`, text,
  `NULL` and timestamps all take one path. The separate default-pattern parser is gone.
- `java_datetime.rs`: `unix_timestamp(text, pattern)` divides the parsed instant with `/`.
- `timestamp_cast/unix_floor.rs`: registered `unix_seconds` and `unix_millis` that floor every Arrow
  timestamp unit, replacing `datafusion-spark`'s truncating cast chains; a `TIMESTAMP_NTZ` argument
  refuses with Spark's `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`.
- `functions_datetime.py`, `function_dispatch.rs` and `functions_byname.py`: `F.unix_seconds` and
  `F.unix_millis` build the engine function instead of a `DOUBLE` composite, and leave the facade-only
  by-name list.

`CAST(ts AS BIGINT)` already floored, as Spark does, and is unchanged.

## Pins

- `python/repark/tests/timestamps/test_unix_timestamp_truncate_1.py`: 4,092 cells read from
  `unix_timestamp_truncate_1_spark_oracle.json` (recorded from PySpark 4.1.2 by
  `_record_unix_timestamp_truncate_1.py`, `local[1]`, 2026-11-22), grouped by function, input type, zone
  and ANSI mode, over the four doors and three zones; the whole-second, post-1970, `CAST AS BIGINT`,
  `unix_micros` and `unix_date` cells are the controls. The minute difference over seven spans is written
  to Parquet and read back in every zone and mode. `unix_seconds` / `unix_millis` of a `TIMESTAMP_NTZ`
  column check Spark's refusal text on three doors. Plan item P04-07's boundary vectors (one microsecond
  before the epoch, `23:59:59.4996`, one microsecond either side of a whole second, the epoch, one second,
  `NULL`) are written to Parquet and read back cold, then answered through SQL, `F.*`, `F.expr` and the
  `SSSSSS` pattern in three zones and both modes: at one microsecond before the epoch `unix_seconds` and
  `unix_millis` are `-1` and `unix_timestamp` / `to_unix_timestamp` `0`, all `bigint`, and
  `unix_millis(23:59:59.4996)` is `-501` (`-500` on `c5370be6`).
- `python/repark/tests/timestamps/zone_horizon_render_1_residue.json`: the 15 `wall_gap` `unix_timestamp`
  cells (New York, Sydney, Lord Howe; 2099 to 9999) now equal Spark and leave the residue, the gap cells of
  [TRANSITION-DAY-ARITHMETIC-1](transition-day-arithmetic-1-card-2026-10-08.md).
- A mutant that restores the floor in `unix_timestamp` alone (both seats) reds the `unix_timestamp`,
  `to_unix_timestamp` and pattern groups over `TIMESTAMP`, `TIMESTAMP_NTZ` and text, the stored minute
  difference and the UTC and Kolkata boundary vectors (46 tests); `unix_seconds` / `unix_millis`, the
  one-argument `DATE` and text groups, the NTZ refusal and the controls stay green.
- `python/repark/tests/functions/test_functions_d.py`: `unix_seconds` of `1969-12-31 23:59:58.5` is now
  `-2`, Spark's answer (the old pin held the truncation).
- Rust: `timestamp_cast::tests::unix_timestamp_truncates_toward_zero_where_the_cast_floors` and
  `timestamp_cast::unix_floor::tests`.

## Still unlike Spark (out of this unit's seat)

- A `TIMESTAMP_NTZ` literal within about 35 minutes of 1970-01-01 fails to plan
  (`'__repark_timestamp_ntz__' expects an Int64 wall`), whatever function reads it; a year-1
  `TIMESTAMP_NTZ` literal or cast refuses (`INVALID_TYPED_LITERAL`, `CAST_INVALID_INPUT` or `NULL`). The
  pins leave those cells out; they belong to the literal and the cast, not to these functions.
- `unix_micros` of a `TIMESTAMP_NTZ` answers where Spark refuses, and `F.unix_micros` of a `STRING` or
  `DATE` answers where Spark refuses; `unix_seconds` / `unix_millis` of a `STRING` or `DATE` refuse with
  DataFusion's planning text rather than Spark's `UNEXPECTED_INPUT_TYPE`.
- `unix_timestamp(text, 'yyyy-MM-dd HH:mm:ss.SSSSSS')` refuses a fraction shorter than six digits; Spark
  accepts one to six.
- `CAST(ts AS INT | SMALLINT | TINYINT)` that overflows raises Arrow's cast error, where Spark answers
  `NULL` with ANSI off and `CAST_OVERFLOW` with ANSI on.
- `F.to_unix_timestamp(col, F.lit(pattern))` raises `NOT_ITERABLE`; PySpark accepts a `Column` pattern.
- `F.to_unix_timestamp(col, "pattern")` with a plain string answers as if the pattern were a literal; PySpark
  reads a string `format` as a column name and refuses (`UNRESOLVED_COLUMN`). Same on `c5370be6`.
- [UNIX-TIMESTAMP-NTZ-ZONE-1](cards-2026-11-19/unix-timestamp-ntz-zone-1-card-2026-11-19.md): a
  `TIMESTAMP_NTZ` column built by a cast answered its stored wall in every zone on `c5370be6`, equal to
  Spark apart from the rounding fixed here; that card's Iceberg-column step is not measured here.

Up: [map.md](map.md).
