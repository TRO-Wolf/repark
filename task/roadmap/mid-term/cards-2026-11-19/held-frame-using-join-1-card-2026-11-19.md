# Card HELD-FRAME-USING-JOIN-1: a USING join keeps only the left frame's state, so a held right side answers under the live settings

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's rulings of the 2026-11-18 window (item 6; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md)). **Source:** the [TEMP-VIEW-SESSION-SETTINGS-1 card](../temp-view-session-settings-1-card-2026-11-02.md) and the part 2 bar, [bar.md](../../../wo/temp-view-session-settings-1/evidence/built-part2/bar.md), "The other callers that analyse a held plan under the live state".

**Status:** filed 2026-11-19, not dispatched.

**Severity: S1, label-blocking** (owner, 2026-11-18, about 14:58 UTC, verbatim "Defaults on all three, extend the window 4 hours").

## Finding

`join_on_names` (`repark-python/src/dataframe/mod.rs`, a USING join) keeps only the left frame's state, so a held right side answers under the live settings. Silent. The bar's table of the other callers records 16 cells.

**Main:** WRONG, unchanged on every build (the bar's table, column "result on every build (pre, part1, head)"; `pre` is main before part 1).

Up: [map.md](map.md).
