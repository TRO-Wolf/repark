# Card DATE-PART-SESSION-ZONE-1: date_part and extract read a timestamp's calendar fields in UTC, not the session zone

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's simplifier audit of 2026-11-16 (difference 9). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. The sweep re-run on main `e50ed17f` does not cover this audit cell; it was not re-measured on that main.

**Status:** fixed 2026-11-23 on branch `fix/date-part-session-zone-1` (merge pending; S1, label-blocking, ruled 2026-11-16). `SparkDatePart` + `SparkExtractDatePart` (seated before DataFusion's first `type_coercion` on both `SparkExtension` and `F.expr`, with `DeferredSessionTimeZoneConfig` deferring zoneless `TIMESTAMP`/`CAST`/`TRY_CAST` literal folds in `build_expr_context` to runtime) route `date_part`, `datepart`, and `extract` over `TIMESTAMP`/`DATE`/`STRING`/`INTERVAL`/`NULL` through `session_wall::extract_calendar_part` (and stock `date_part` for typed `F.date_part` / SQL `EXTRACT` over `INTERVAL` and Arrow `YearMonth`/`DayTime`) with no UTC year preimage, exact 38 Spark temporal fields + `[INVALID_EXTRACT_FIELD]`, pre-optimizer `[NON_FOLDABLE_ARGUMENT]`, and 1-based `extract(DOW)`/`extract(DAYOFWEEK)` over `DATE` and `TIMESTAMP_NTZ`. Remaining recorded boundaries: SQL `date_part` refuses `Interval(MonthDayNano)` (restoring main's refusal where DataFusion drops the interval qualifier); a foldable non-literal invalid field over `LIMIT 0` skips validation (D1-3); `date_part(NULL, ts)` raises as on main where Spark answers `double: NULL`; other `EXPR_CONTEXT`-baked settings (`spark.sql.timestampType=TIMESTAMP_NTZ`) remain on `FEXPR-TIMESTAMP-ZONE-1`.

**Severity:** S1, label-blocking, ruled 2026-11-16. A silent wrong calendar field on an ordinary `extract` in a session that is not UTC.

Ruled 2026-11-16 ("Merge those and defaults."): S1, label-blocking; in the order after the string-cast build, ahead of TINYINT-SMALLINT-ARITH-ANSI-1 and RAND-SHARED-VALUE-1.

## Finding

In a session whose zone is `America/Los_Angeles` (the audit's probe set it on purpose), `date_part('year', ts)`, `extract(YEAR FROM ts)` and `date_part('hour', ts)` read the timestamp's fields in UTC on main. `year(ts)` and `hour(ts)` read the session zone and are right. The defect is in the two spellings of the same functions, in both ANSI modes. `date_trunc` agrees with Spark as text.

## Measured

Both ANSI modes give the same main output. The seed rows are 2024-01-01 02:00 UTC and 2025-01-01 07:00 UTC.

| row | Spark 4.1.2: `year(ts)`, `date_part` year, `extract` year, `hour(ts)`, `date_part` hour | main, same order |
|---|---|---|
| 0 (2023-12-31 18:00 in the session zone) | 2023, 2023, 2023, 18, 18 | 2023, 2024, 2024, 18, 2 |
| 1 (2024-12-31 23:00 in the session zone) | 2024, 2024, 2024, 23, 23 | 2024, 2025, 2025, 23, 7 |

Reproduction, with `<scratchpad>` the Fable audit scratch folder (not committed):
`python3 <scratchpad>/audit/check/verify.py repark false`, and the same with `spark`; the case `zone_parts`.

## Not yet known

The seat. Related and not the same: TRANSITION-DAY-ARITHMETIC-1 names this defect in its residue R-2 (`extract` and `date_part` read the UTC label) and does not card it. FN-DATE and FN-PREIMAGE-YEAR are the audit inventory's rules for `date_part`; the year preimage is a separate seat. FEXPR-TIMESTAMP-ZONE-1 is a different defect (F.expr timestamp literals).

Up: [map.md](map.md).
