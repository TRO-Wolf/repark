# Card C-3-NULL-STRIDE-1: the first stride's NULL test is a sequential scan of the whole table

**Date:** 2026-10-08. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief. **Source:** the C-3 lane's hand-back (PR #998, not on main yet), question Q3, and the orchestrator's C-3 round 1 rulings (2026-10-08) on it.

**Status:** filed, not scheduled. The ruling is a follow-up card, not in C-3. The ruling files this card and does not rule on the lean.

## Why

**The arm.** Spark's first stride is `col < cut OR col IS NULL`. Postgres plans it as a sequential scan of the whole table.

**The measured cost.** About 1.1 s of a 7.3 s four-stream read on the 10M-row table. The hand-back also gives four psql COPY streams to /dev/null: 6.1-6.6 s as index ranges, and 7.2-7.5 s with Spark's first-stride NULL test.

**Why the NOT NULL shortcut was not taken.** Dropping the arm for a NOT NULL column was not done. A constraint dropped between planning and the scan would lose NULL rows silently.

**The lean (the hand-back's).** Run the NULL test as its own stride (`col IS NULL`). It is exact under schema drift, and it is an index scan when the column is indexed. Measure it on an unindexed column before adopting.

## The ask

Take the lean, measured first: the NULL test as its own stride, measured on an unindexed column before it is adopted.

## Gates

- the measurement the lean names: the NULL stride on an unindexed column, measured before adoption;
- any other gate: Not measured.

## Pointers

- The C-3 lane's hand-back (PR #998, not on main yet), question Q3.
- The orchestrator's C-3 round 1 rulings (2026-10-08), Q3.
