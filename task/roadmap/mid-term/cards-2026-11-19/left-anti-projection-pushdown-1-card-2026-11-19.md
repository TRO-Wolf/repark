# Card LEFT-ANTI-PROJECTION-PUSHDOWN-1: a left-anti or left-semi join evaluates the left projection on every row, where Spark pushes the join below it

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's rulings of the 2026-11-18 window (items 6 and 8; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md)). **Source:** the [TEMP-VIEW-SESSION-SETTINGS-1 card](../temp-view-session-settings-1-card-2026-11-02.md) and the part 2 bar, [bar.md](../../../wo/temp-view-session-settings-1/evidence/built-part2/bar.md), "Task 2".

**Status:** filed 2026-11-19, not dispatched. On the label path, ordered ahead of [TEMP-VIEW-FRAME-CROSSJOIN-1](temp-view-frame-crossjoin-1-card-2026-11-19.md) and [HELD-FRAME-USING-JOIN-1](held-frame-using-join-1-card-2026-11-19.md).

**Severity: S3, not label-blocking** (owner, 2026-11-18, about 14:58 UTC, verbatim "Defaults on all three, extend the window 4 hours"; ordered ahead by the owner's 2026-11-18 about 20:05 UTC ruling, verbatim "Go with Fable's recommendations on all seven, re-run 1362").

## Finding

A held ANSI-on frame on the left of a left-anti join raises `ARITHMETIC_OVERFLOW`, where Spark pushes the anti join below the projection (`PushDownLeftSemiAntiJoin`) and answers empty. Loud, not silent.

- With the backed-out CROSSJOIN fix, `ansi/on_off/left_anti/cond_both` and `ansi/on_off/left_anti/cond_left` went EQUAL → RAISE `ARITHMETIC_OVERFLOW`. That fix is why this card is the prerequisite of TEMP-VIEW-FRAME-CROSSJOIN-1.
- It is a direct gap that RePark shows under matching settings too (the bar's Task 2).

**Main:** RePark raises the same way under matching settings (`ansi/match/cond_left_anti`, `ansi/match/left_anti`), on every build.

Up: [map.md](map.md).
