# Card NTZ-CAST-OFFSET-TEXT-1: under `TIMESTAMP_NTZ`, `CAST` of an offset text keeps the instant, where Spark drops the offset

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling of 2026-11-19 (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default for the neighbours' severities). **Source:** [domain table](../../../wo/string-hour-minute-second-1/domain-table.md) §3 and §6, row N11 (the step 0 of STRING-HOUR-MINUTE-SECOND-1, merged as #1369, `81ae984e`).

**Status:** filed 2026-11-19; not dispatched.

**Severity: S1 by class, label-blocking** (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default, "the silent ones (N1, N3, N4, N8, N11) S1 by class and label-blocking").

## Finding

- **Silent.** `CAST(<offset text> AS TIMESTAMP)` under `TIMESTAMP_NTZ` keeps the instant, where Spark drops the offset. 52 of 256 `cast_hour` NTZ cells (N11). Step 0's §3 records the same drop: the NTZ cast removes the offset of `off` in both zones, and of `zulu` in Los Angeles.

## First step

None is given in the source.

Up: [map.md](map.md).
