# Card FEXPR-STRING-LITERAL-ZONE-1: `F.expr` over a string literal reads the text as UTC outside UTC, in a projection and in a filter

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's rulings of 2026-11-19: ruling 1 (about 12:17 UTC, the sketch's §7 Q3 default), and rulings 4 and 5 (about 15:47 UTC and 17:40 UTC), which accept the lost EQUAL cells named below. **Source:** [domain table](../../../wo/string-hour-minute-second-1/domain-table.md) §4 and §6, row N8 (the step 0 of STRING-HOUR-MINUTE-SECOND-1, merged as #1369, `81ae984e`); the seat A verifier's verdict on PR #1370, held outside the repository, finding F1.

**Status:** filed 2026-11-19; not dispatched.

**Severity: S1 by class, label-blocking** (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default, "the silent ones (N1, N3, N4, N8, N11) S1 by class and label-blocking").

## Finding

- **Silent.** `F.expr` over a string literal outside UTC reads the text as UTC. `hour(CAST('2024-01-01 03:04:05' AS TIMESTAMP))` answers `19` under Los Angeles, where Spark answers `3`. SQL, `selectExpr` and `F.hour(F.lit(…).cast('timestamp'))` answer `3`. Step 0's table counts 10 LTZ and 5 NTZ cells (N8, domain table §4).
- **The three cells accepted under ruling 4 (about 15:47 UTC).** `hour('2024-01-01')` on `F.expr` under `a0-la`, `a1-la` and `a1-la-ntz`: PR #1370 loses their EQUAL against main.
- **The family accepted under ruling 5 (about 17:40 UTC).** The seat A verifier measured it (held outside the repository, finding F1, FAIL, class (a), S1). Main answered Spark's `0` only by reading a midnight, so the family reaches every non-UTC zone:
  - `hour` of the date-only literal `2023-07-15` in Los Angeles, Kolkata, Kathmandu and Tokyo answers 17, 5, 5 and 9, where Spark and main answer 0;
  - `minute` of the same literal in Kolkata and Kathmandu, zones with a 30-minute and a 45-minute offset, answers 30 and 45;
  - the projection door, and `filter(F.expr("hour('2023-07-15') = 0"))`, where the row count flips from 1 (Spark, main) to 0, in all four ANSI and timestamp-type configurations;
  - `a0-la-ntz`, `hour('2024-01-01')` on `F.expr` (ANSI off, `TIMESTAMP_NTZ`), was not in step 0's configurations and is outside the three accepted cells.
  - In all, 49 cells beyond the three accepted under ruling 4: 48 from the verifier's 600-cell breadth probe, and 1 from its own cell list.
- **Bar.** Rulings 4 and 5 accept the lost EQUAL cells, and this card's bar must include them: the three cells, the 48 breadth cells and the one configuration cell above.

## First step

None is given in the source.

Up: [map.md](map.md).
