# Card HELD-FRAME-DERIVED-ANSI-1: `withColumn` and `select(F.expr(...))` over a held ANSI frame answer unlike Spark on every build

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's rulings of the 2026-11-18 window (item 6; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md)). **Source:** the [TEMP-VIEW-SESSION-SETTINGS-1 card](../temp-view-session-settings-1-card-2026-11-02.md) and the part 2 bar, [bar.md](../../../wo/temp-view-session-settings-1/evidence/built-part2/bar.md), "The other callers that analyse a held plan under the live state".

**Status:** filed 2026-11-19, not dispatched.

**Severity: S2, not label-blocking** (owner, 2026-11-18, about 14:58 UTC, verbatim "Defaults on all three, extend the window 4 hours").

## Finding

The column door analyses eagerly at `repark-python/src/column/expr_build.rs:187`. Over a held frame, `withColumn` and `select(F.expr(...))` answer unlike Spark with ANSI on, in both directions. The bar's table records 8 cells: zone EQUAL; ANSI WRONG in both directions.

**Main:** ANSI WRONG in both directions, unchanged on every build; the zone cells are EQUAL on every build.

Up: [map.md](map.md).
