# Card STRING-DATE-FIELD-OFFSET-1: `year` to `weekday` and `CAST(c AS DATE)` over an offset text answer the UTC date, where Spark reads the text's date

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling of 2026-11-19 (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default for the neighbours' severities). **Source:** [domain table](../../../wo/string-hour-minute-second-1/domain-table.md) §6, row N1 (the step 0 of STRING-HOUR-MINUTE-SECOND-1, merged as #1369, `81ae984e`).

**Status:** filed 2026-11-19; not dispatched.

**Severity: S1 by class, label-blocking** (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default, "the silent ones (N1, N3, N4, N8, N11) S1 by class and label-blocking").

## Finding

- **Silent.** `year` to `weekday`, and `CAST(c AS DATE)`, over `off` (`2024-01-01T03:04:05+05:30`) answer the UTC date, `2023-12-31`, where Spark reads the text's date, `2024`. 64 cells per function (N1; measured in step 0 on main `2fdab5c7` against Spark 4.1.2).

## First step

None is given in the source.

Up: [map.md](map.md).
