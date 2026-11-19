# Card CSV-DIRECTORY-EXTENSION-1: a CSV directory read skips files whose names do not end in `.csv`, silently

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling of 2026-11-19 (ruling 2, item 9, about 14:00 UTC). **Source:** the Fable reply of 2026-11-19 in the agents' lessons folder (file `Fable reply — reader-parse units, cost noise and the next queue 2026-11-19.md`), §7, first bullet.

**Status:** filed 2026-11-19; not dispatched.

**Severity: S2, not label-blocking** (ruling 2, item 9, about 14:00 UTC: "the mixed CSV directory, S2").

## Finding

- **Silent.** A directory holding `a.csv` and `data.txt` answers 2 rows, where Spark answers 4 (Fable §7, [M]). Fable's note: a directory Spark wrote always carries the extension. The same on main, delta 1 and delta 2.

**Seen in passing, not rated** (Fable §7, the three loud bullets; the same on main, delta 1 and delta 2):

- a single file, or a directory, with no `.csv` extension raises, where Spark reads it (loud);
- an empty directory raises, where Spark answers no rows (loud);
- `input_file_name()` is not resolved (loud).

## First step

None is given in the source.

Up: [map.md](map.md).
