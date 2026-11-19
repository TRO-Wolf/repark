# Card UNIX-TIMESTAMP-STRING-TRIM-1: `unix_timestamp` of a text with leading spaces answers where Spark raises or answers NULL, and `gap` raises in Los Angeles

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling of 2026-11-19 (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default for the neighbours' severities). **Source:** [domain table](../../../wo/string-hour-minute-second-1/domain-table.md) §6, row N9 (the step 0 of STRING-HOUR-MINUTE-SECOND-1, merged as #1369, `81ae984e`).

**Status:** filed 2026-11-19; not dispatched.

**Severity: S2, not label-blocking** (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default, "the loud ones (N2, N5, N6, N9, N10) S2").

## Finding

- **Silent and loud.** `unix_timestamp(lead)`: SQL answers where Spark raises or answers NULL, because Spark's format parse does not trim the text. `gap` raises `CANNOT_PARSE_TIMESTAMP` in Los Angeles. 60 cells (N9).

## First step

None is given in the source.

Up: [map.md](map.md).
