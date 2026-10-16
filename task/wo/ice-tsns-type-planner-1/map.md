# task/wo/ice-tsns-type-planner-1/ — the type-planner sketch for R-017

The Fable 5.1 sketch for ICE-TSNS-TYPE-PLANNER-1 (2026-10-13): the SQL type name
decides the unit, so a nanosecond cast means nanoseconds. Filed from the sketch
source; no product code. Closes unmerged #1027 (ICE-TSNS-NARROW-REFUSE-1) and
#1042 (ICE-TSNS-COERCION-1).

- [sketch.md](sketch.md) — the sketch, filed byte for byte (2026-10-13). A 2026-10-15 addendum quotes ruling 112 (the analyzer-list re-pin) and ruling 113 (the ANSI planner variant lives once in `repark-functions`, and `AnsiDialect` and `NativeDoorTypes` both call it).
- [amendment-1.md](amendment-1.md) — amendment 1, filed byte for byte (2026-10-13): step 0 halts H1 to H4; H1 to H3 resolved, H4 not met with producer P15 added; §4 and §5 amended for step 1.
- [step0-fresh-2026-10-15.md](step0-fresh-2026-10-15.md) — the fresh step 0 from main `fa1e4274` (2026-10-15, no product code). Live PySpark 4.1.2's `convertToArrowArraySafely` default (`'true'`). The three mechanisms with their seams. The 13-commit carry-over table. The red set of the carry-over on today's main: the 218 by mechanism, plus two new shapes outside them (UPDATE … WHERE into `TIMESTAMP`, encoded sources). U-3's fixtures (246 green; 341 of 356). The Q6 cell, and the overlap with U-6.
- [carryfix-trace-2026-10-15.md](carryfix-trace-2026-10-15.md) — the carry-over trace for rulings 100 and 101 (2026-10-15, no product code): each of the 78 breaks (51 `repark-spark` Rust tests, 27 facade tests) and the 8 ruling-101 cells attributed to the first carried commit at which it is red, measured state by state; the mechanism each carried commit serves (none is dropped); the group, the oracle or Q6 answer, main and head for every row.
- The fresh build's ledger (2026-10-15): [../../ledgers/staging/ice-tsns-type-planner-1-ledger.md](../../ledgers/staging/ice-tsns-type-planner-1-ledger.md). It corrects step 0 on one point: the UNION DISTINCT gap is in the nanosecond kernel's return field, not the widen rule's union projection (ledger §2).

Up: [../map.md](../map.md).
