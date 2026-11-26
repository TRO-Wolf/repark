# Card FEXPR-TIMESTAMP-DISPLAY-NAME-1: unaliased timestamp literals and NTZ casts carry different column names than Spark

**Date:** 2026-11-24. **Filed by:** the builder of FEXPR-TIMESTAMP-NTZ-1 (`gemini-4-neon-xhigh`), split from
[FEXPR-TIMESTAMP-ZONE-1](../fexpr-timestamp-zone-1-card-2026-10-01.md) because the display-name path lives in a
separate seat from `build_expr_context` / `SparkLtzTimestampCast`. **Severity:** S3 proposed. **Status:** filed, not
dispatched.

## Measured

Measured on main (`14d2404a6` / `pkg-d1-57c92b41`) and Spark 4.1.2, in both ANSI modes, in `America/New_York` (also
checked in `Asia/Kolkata` and `UTC`). Values and dtypes agree after FEXPR-TIMESTAMP-NTZ-1; only the unaliased output
column name differs.

| Expression | Conf | Door | Spark 4.1.2 column name | RePark column name |
|---|---|---|---|---|
| `TIMESTAMP '2024-01-01 02:00:00Z'` | default | `F.expr` (`select`) | `TIMESTAMP '2023-12-31 21:00:00'` | `TIMESTAMP '2024-01-01 02:00:00Z'` |
| `TIMESTAMP '2024-01-01 02:00:00Z'` | default | `spark.sql`, `selectExpr` | `TIMESTAMP '2023-12-31 21:00:00'` | `1704074400000000` |
| `TIMESTAMP '2024-01-01 02:00:00+05:30'` | default | `F.expr` (`select`) | `TIMESTAMP '2023-12-31 15:30:00'` | `TIMESTAMP '2024-01-01 02:00:00+05:30'` |
| `TIMESTAMP '2024-01-01 02:00:00+05:30'` | default | `spark.sql`, `selectExpr` | `TIMESTAMP '2023-12-31 15:30:00'` | `1704054600000000` |
| `TIMESTAMP '2024-01-01 02:00:00'` | default | `spark.sql`, `selectExpr` | `TIMESTAMP '2024-01-01 02:00:00'` | `1704092400000000` |
| `TIMESTAMP '2024-01-01 02:00:00'` | `timestampType=TIMESTAMP_NTZ` | `F.expr` (`select`) | `TIMESTAMP_NTZ '2024-01-01 02:00:00'` | `TIMESTAMP '2024-01-01 02:00:00'` |
| `TIMESTAMP '2024-01-01 02:00:00'` | `timestampType=TIMESTAMP_NTZ` | `spark.sql`, `selectExpr` | `TIMESTAMP_NTZ '2024-01-01 02:00:00'` | `2024-01-01 02:00:00` |
| `CAST(TIMESTAMP '2024-01-01 02:00:00' AS TIMESTAMP_NTZ)` | default | `spark.sql`, `selectExpr` | `CAST(TIMESTAMP '2024-01-01 02:00:00' AS TIMESTAMP_NTZ)` | `__repark_cast_timestamp_ntz__(Utf8("2024-01-01 02:00:00"))` |

## Cause (seat inventory)

Two display-name seats, neither in `build_expr_context` or `SparkLtzTimestampCast`:
- `python/repark/src/repark/spark/functions.py:312-339` (`expr(sql)`) and `column.py:1136-1152`: sets
  `projection_name = sql.strip()` statically before any session or time zone exists, and wraps the expression in
  `.alias(self._projection_name)`.
- `crates/repark-spark/src/spark_typed.rs:88-151, 419-438` (`SparkProjectionDisplay` / `literal_spark`): renders folded
  `TimestampMicrosecond` literals as raw integer ticks, `Cast(Utf8, Timestamp(us, None))` as bare string text, and
  `__repark_cast_timestamp_ntz__` over non-literal/non-column arguments without Spark's `CAST(... AS TIMESTAMP_NTZ)`
  form.

Up: [map.md](map.md).
