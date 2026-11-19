# Card AVG-DISTINCT-MIXED-COUNT-1: `avg(DISTINCT c)` with `count(DISTINCT id)` in one query refuses, where Spark answers

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** the step 0 critic verdict of AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 (Opus 5.5 medium, 2026-11-19, PR #1365), its "Step 3" item 5(c) and its "Step 5" shapes table, the `avg(DISTINCT c), count(DISTINCT id)` row; kept outside the repository.

**Status:** filed 2026-11-19, not dispatched. Outside the `avg` unit: the verdict says "That is a refusal outside the card; file it."

**Rating:** not rated; rating owed; measured once by the AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 step 0 critic.

## Finding

`avg(DISTINCT c), count(DISTINCT id)` refuses (`DistinctAvgAccumulator`, `aggregate.rs:194-197` in the verdict's cite) where Spark `local[1]` / `local[4]` answers `[1, 8]`.

**Main:** RePark main (`pkg-main-4c365814`, the verdict's Step 5) refuses the query.

Up: [map.md](map.md).
