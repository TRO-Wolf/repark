# task/wo/ice-tsns-type-planner-1/ — the type-planner sketch for R-017

The Fable 5.1 sketch for ICE-TSNS-TYPE-PLANNER-1 (2026-10-13): the SQL type name
decides the unit, so a nanosecond cast means nanoseconds. Filed from the sketch
source; no product code. Closes unmerged #1027 (ICE-TSNS-NARROW-REFUSE-1) and
#1042 (ICE-TSNS-COERCION-1).

- [sketch.md](sketch.md) — the sketch, filed byte for byte (2026-10-13).
- [amendment-1.md](amendment-1.md) — amendment 1, filed byte for byte (2026-10-13): step 0 halts H1 to H4; H1 to H3 resolved, H4 not met with producer P15 added; §4 and §5 amended for step 1.
- [step0-fresh-2026-10-15.md](step0-fresh-2026-10-15.md) — the fresh step 0 from main `fa1e4274` (2026-10-15, no product code). Live PySpark 4.1.2's `convertToArrowArraySafely` default (`'true'`). The three mechanisms with their seams. The 13-commit carry-over table. The red set of the carry-over on today's main: the 218 by mechanism, plus two new shapes outside them (UPDATE … WHERE into `TIMESTAMP`, encoded sources). U-3's fixtures (246 green; 341 of 356). The Q6 cell, and the overlap with U-6.

Up: [../map.md](../map.md).
