# Card STRING-DATE-TRUNC-FORMAT-1: `date_trunc` and `date_format` answer NULL where Spark answers, and read the offset and `Z` texts as a UTC wall in Los Angeles

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling of 2026-11-19 (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default for the neighbours' severities). **Source:** [domain table](../../../wo/string-hour-minute-second-1/domain-table.md) §6, row N3 (the step 0 of STRING-HOUR-MINUTE-SECOND-1, merged as #1369, `81ae984e`).

**Status:** filed 2026-11-19; not dispatched.

**Severity: S1 by class, label-blocking** (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default, "the silent ones (N1, N3, N4, N8, N11) S1 by class and label-blocking").

## Finding

- **Silent.** `date_trunc` and `date_format` answer NULL where Spark answers (`lead`, `trail`, `nosec`, `tonly`, `yonly`) or raises (`empty`, `bad`, `badtime` with ANSI on). The `off` and `zulu` texts read as a UTC wall in Los Angeles. 32 to 64 cells per text (N3).

## First step

None is given in the source.

Up: [map.md](map.md).
