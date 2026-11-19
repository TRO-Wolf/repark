# Card STRING-DATE-FIELD-RAISE-1: `year` to `weekday` and `CAST(c AS DATE)` raise on texts that Spark answers

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling of 2026-11-19 (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default for the neighbours' severities). **Source:** [domain table](../../../wo/string-hour-minute-second-1/domain-table.md) §6, row N2 (the step 0 of STRING-HOUR-MINUTE-SECOND-1, merged as #1369, `81ae984e`).

**Status:** filed 2026-11-19; not dispatched.

**Severity: S2, not label-blocking** (ruling 1, about 12:17 UTC: the sketch's §7 Q3 default, "the loud ones (N2, N5, N6, N9, N10) S2").

## Finding

- **Loud.** The same functions raise on `lead`, `trail`, `nosec`, `tonly`, `badtime` and `yonly`, where Spark answers. They also raise on `empty` and `bad` with ANSI off, where Spark answers NULL. Main's arrow `Utf8 → Date32` cast raises (`Cast error: Cannot cast string …`). 512 cells per function (N2).

## First step

None is given in the source.

Up: [map.md](map.md).
