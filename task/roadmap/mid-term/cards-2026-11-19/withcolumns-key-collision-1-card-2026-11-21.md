# Card WITHCOLUMNS-KEY-COLLISION-1: withColumns with two keys that match one existing column under caseSensitive=false keeps the last key, where Spark keeps the first

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** item 4 of the wiring unit's card lines, in [card-lines.md](../../../wo/csv-string-field-text-1/built/card-lines.md), and the owner's ruling of 2026-11-21 (decision 4, the `withColumns` key collision, 2 cells), recorded in [rulings-2026-11-21.md](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-21.md).

**Rating: S2, not label-blocking (the owner, 2026-11-21).**

**Status:** filed 2026-11-21; not dispatched.

- When two `withColumns` keys match one existing column under `caseSensitive=false`, Spark takes the first key and keeps its name; RePark takes the last. This is silent, and it is on main too: `withColumns({"I": 5, "ı": 6})` over a column `I` answers `I` = 5 on Spark and `ı` = 6 on main.
- Delta 2's Java final sigma lets `withColumns({"ΑΣ Β": 5, "ασ β": 6})` reach the same path (RAISE to WRONG, 2 cells).
- The fix lives in the facade's `withColumns`.

Up: [map.md](map.md).
