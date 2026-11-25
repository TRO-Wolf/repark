# Card CAST-STRING-GRAMMAR-1: string casts to DATE, TIMESTAMP_NTZ, DOUBLE and DECIMAL refuse Spark-valid text

**Date:** 2026-11-25. **Filed by:** the orchestrator (`claude-opus-5-5`), from findings F-A4 and F-A5 of the first
verifier of INSERT-OVERWRITE-CONST-CAST-1 (PR #1431), re-measured here on the read path. **Severity:** S2 proposed;
the owner rates it. Every cell refuses loudly, and none answers wrongly. **Status:** filed, not dispatched.

## Measured

Measured on live Spark 4.1.2 and on a build of main `14d2404a6` (#1429's tested head), UTC session zone, as
`SELECT CAST(s AS <type>) AS v FROM (SELECT <text> AS s UNION ALL SELECT <text> AS s) t`, so the cast runs on rows
rather than folding.

| Text | Type | Spark 4.1.2, both modes | Main, ANSI on | Main, ANSI off |
|---|---|---|---|---|
| `' 2024-01-01 '` | DATE | `2024-01-01` | Arrow cast error | Arrow cast error |
| `'2024'` | DATE | `2024-01-01` | Arrow cast error | Arrow cast error |
| `'2024-01'` | DATE | `2024-01-01` | Arrow cast error | Arrow cast error |
| `'0x1.0p0'` | DOUBLE | `1.0` | `CAST_INVALID_INPUT` | `Column 'v' is declared as non-nullable` |
| `'1e4'` | DECIMAL(5,0) | `10000` | Arrow cast error | Arrow cast error |
| `'2024-1-1'` | TIMESTAMP_NTZ | `2024-01-01 00:00:00` | `CAST_INVALID_INPUT` | `Column 'v' is declared as non-nullable` |
| `' 2024-01-01 00:00:00 '` | TIMESTAMP_NTZ | `2024-01-01 00:00:00` | `CAST_INVALID_INPUT` | the same crash |
| `'2024-01'` | TIMESTAMP_NTZ | `2024-01-01 00:00:00` | `CAST_INVALID_INPUT` | the same crash |
| `'2024'` | TIMESTAMP_NTZ | `2024-01-01 00:00:00` | `CAST_INVALID_INPUT` | the same crash |

The same texts cast to `TIMESTAMP` already answer as Spark does. #1431 keeps its write-door guard from judging these
casts, so the guard does not turn them into new refusals.

## Seat

- DATE and DECIMAL: Arrow's string cast kernels, which do not accept Spark's trimmed, partial-date and exponent forms.
- DOUBLE: `crates/repark-functions/src/java_double/parse_float.rs`, which has no Java hex-float form.
- TIMESTAMP_NTZ: `crates/repark-functions/src/timestamp_ntz_cast.rs`, which does not share the `TIMESTAMP` string
  grammar. FEXPR-TIMESTAMP-NTZ-1 (#1432) changes the nullability of this cast, so re-measure the ANSI-off cells once
  it merges.

## First step

Measure Spark's string grammar for each type (whitespace, partial dates, signs, exponents, hex floats, `T` and zone
suffixes) on every door and in both modes, then give each cast one Spark grammar.

Up: [map.md](map.md).
