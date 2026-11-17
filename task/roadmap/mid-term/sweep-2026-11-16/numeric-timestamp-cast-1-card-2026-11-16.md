# Card NUMERIC-TIMESTAMP-CAST-1: a FLOAT or DECIMAL cast to TIMESTAMP reads the number as microseconds, not seconds

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's type-pair sweep of 2026-11-16 (family 2). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. Re-run on main `e50ed17f` (RePark side): unchanged; only family 10's 32 `VALUES` cells moved to equal (#1341).

**Status:** ruled 2026-11-16, not dispatched; label-blocking. Built inside the string-cast unit, not as a separate build.

**Severity:** S1, label-blocking, ruled 2026-11-16. Silent, in both ANSI modes, on an ordinary cast of a measured number.

Ruled 2026-11-16 ("Merge those and defaults."): S1, label-blocking; absorbed into the string-cast build (STRING-CAST-SILENT-1 with STRING-CAST-LENIENT-1) as one Spark cast unit.

## Finding

`CAST` of a FLOAT or DECIMAL value to TIMESTAMP reads the value as a count of microseconds. Spark reads it as seconds with a fraction, as it does for an integer. Main answers a timestamp within the first second of the epoch where Spark answers a timestamp some seconds or years later, with no error. The reverse direction, TIMESTAMP to DECIMAL, is carded in TRY-CAST-TIMESTAMP-NUMERIC-1; this card is the other way round.

## Measured

Both ANSI modes give the same values (class V: same type, different values).

| cell | Spark 4.1.2 | main |
|---|---|---|
| FLOAT `0.1` to TIMESTAMP | `1970-01-01 00:00:00.100000` | `1970-01-01 00:00:00` |
| FLOAT `1.5` to TIMESTAMP | `1970-01-01 00:00:01.500000` | `1970-01-01 00:00:00.000001` |
| FLOAT `1e10` to TIMESTAMP | `2286-11-20 17:46:40` | `1970-01-01 02:46:40` |
| DECIMAL(10,2) `0.50` to TIMESTAMP | `1970-01-01 00:00:00.500000` | `1970-01-01 00:00:00` |
| DECIMAL(10,2) `12345678.99` to TIMESTAMP | `1970-05-23 21:21:18.990000` | `1970-01-01 00:00:12.345678` |

The DOUBLE rows are not counted. On the DOUBLE row that holds 2^53, the Spark side failed in the harness's Python conversion (`year 294247 is out of range`), not inside Spark, so that row is not a Spark refusal.

Reproduction, with `<scratchpad>` the Fable sweep scratch folder (not committed):
`python3 <scratchpad>/sweep/v.py f:cast:FLOAT:TIMESTAMP f:cast:DEC10_2:TIMESTAMP`

## Seat (not diagnosed)

The analyzer lowers `CAST(ts AS DECIMAL)` through `__repark_epoch_seconds_real__` (`crates/repark-functions/src/analyzer.rs:193`, as TRY-CAST-TIMESTAMP-NUMERIC-1 records). The number-to-TIMESTAMP direction has no such lowering in the measured path. Step 0 measures which function the cast reaches.

Up: [map.md](map.md).
