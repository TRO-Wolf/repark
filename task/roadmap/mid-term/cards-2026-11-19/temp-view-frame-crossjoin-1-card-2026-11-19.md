# Card TEMP-VIEW-FRAME-CROSSJOIN-1: `crossJoin` and the exact-key join door analyse both held sides under the live settings

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's rulings of the 2026-11-18 window (item 6; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md)). **Source:** the [TEMP-VIEW-SESSION-SETTINGS-1 card](../temp-view-session-settings-1-card-2026-11-02.md) and the part 2 bar, [bar.md](../../../wo/temp-view-session-settings-1/evidence/built-part2/bar.md), "Task 2: `crossJoin` and the exact-key join door".

**Status:** filed 2026-11-19, not dispatched.

**Severity: S1, label-blocking** (owner, 2026-11-18, about 14:58 UTC, verbatim "Defaults on all three, extend the window 4 hours"). Its prerequisite is [LEFT-ANTI-PROJECTION-PUSHDOWN-1](left-anti-projection-pushdown-1-card-2026-11-19.md) (S3, on the label path, ordered ahead of this card).

## Finding

`crossJoin` and the exact-key join door analyse both held frames under the live settings, and the answer is silent. The sweep covered 219 cells (`raw/delta1/joins-*.jsonl`): inner, left, right, full, semi, anti and cross joins, keyed by USING and by a condition, a held frame on each side and on both, zone and ANSI both ways, plus matching settings.

- The fix, built and measured then backed out (the back-out rule): each side's plan through the frame seat before `join_exact_sides` analyses the join. It moved 74 cells WRONG → EQUAL and 2 EQUAL cells away from Spark: `ansi/on_off/left_anti/cond_both` and `ansi/on_off/left_anti/cond_left` went EQUAL → RAISE `ARITHMETIC_OVERFLOW`. Spark pushes the anti join below the projection and answers empty.
- Against part 1, the head's join cells moved 13 WRONG → EQUAL (SQL views and CTAS over held joins, through the seat), with 0 EQUAL → WRONG.

**Main:** the part 2 bar does not record main's answer cell by cell for these join cells. The 74 cells the backed-out fix moved were WRONG on the head before the fix.

Up: [map.md](map.md).
