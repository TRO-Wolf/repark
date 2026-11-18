# Card HELD-FRAME-CURRENT-TIMEZONE-1: `current_timezone()` on a held frame first acted on after a zone `SET` answers the creation zone

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), from the TEMP-VIEW-SESSION-SETTINGS-1 step 0 (PR #1356, head `a2e34b9e`), under the owner's ruling "defaults on both" (about 23:05, later the same evening; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), later section).

**Status:** filed 2026-11-17, not dispatched.

**Severity: S3** (owner, 2026-11-17, "defaults on both", question 5: carded at S3, not in the view unit).

## Finding

A DataFrame held by the session keeps the creation zone for `current_timezone()`. Spark folds `current_timezone()` at the frame's first action. So a frame first acted on after a zone `SET` answers the reader's zone on Spark, and the creation zone on RePark.

## Cells

Step 0 domain table §6: a held frame first acted on after a zone `SET`, `current_timezone()` answers the creation zone on RePark and the reader's on Spark, 6 cells. Held frames collected before the `SET` match Spark.

## Relation

TEMP-VIEW-SESSION-SETTINGS-1. This is Spark's first-action rule for an optimizer-time function on a held frame, which the view unit does not build (owner question 5).

## Source

TEMP-VIEW-SESSION-SETTINGS-1 step 0 domain table §6 and sketch §7, question 5; step 0 revision hand-back, "New counts" and "Held door"; the round 2 re-critic's verdict (GAPS, `a2e34b9e`), item 6.

Up: [map.md](map.md).
