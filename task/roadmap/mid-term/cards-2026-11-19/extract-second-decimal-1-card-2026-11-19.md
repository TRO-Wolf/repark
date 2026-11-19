# Card EXTRACT-SECOND-DECIMAL-1: `extract(SECOND …)` and `date_part('SECOND' / 'S', …)` answer an integer where Spark answers `decimal(8,6)`

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling of 2026-11-19 (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default for the neighbours' severities). **Source:** [domain table](../../../wo/string-hour-minute-second-1/domain-table.md) §6, row N4 (the step 0 of STRING-HOUR-MINUTE-SECOND-1, merged as #1369, `81ae984e`).

**Status:** filed 2026-11-19; not dispatched.

**Severity: S1 by class, label-blocking** (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default, "the silent ones (N1, N3, N4, N8, N11) S1 by class and label-blocking").

## Finding

- **Silent.** `extract(SECOND …)` and `date_part('SECOND' / 'S', …)` answer an integer where Spark answers `decimal(8,6)`: `5` against `5.000000`, and `5.123456` for a text with a fraction. Every parsed text. 672 cells or more (N4).

## First step

None is given in the source.

Up: [map.md](map.md).
