# Card CUBE-GROUPING-EXPR-PLAN-1: `CUBE` over an expression fails to plan, where `CUBE` over plain columns plans

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** the step 0 critic verdict of AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 (Opus 5.5 medium, 2026-11-19, PR #1365), its "Step 5" shapes table, the `CUBE(g, h)` row and its note; kept outside the repository.

**Status:** filed 2026-11-19, not dispatched. Outside the `avg` unit: the verdict says "outside avg; file it".

**Rating:** not rated; rating owed; measured once by the AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 step 0 critic.

## Finding

`CUBE(g, id % 2)` with an expression fails to plan (`spark_integral_literal … No field named`). `CUBE(g, h)` over plain columns plans. On `t`, Spark `local[1]` / `local[4]` answers AO (the verdict's own abbreviation, not expanded there).

**Main:** on `t`, RePark main answers VARIES for `CUBE(g, h)` (the verdict's term for answers that change across runs; its Step 5 measurements ran on `pkg-main-4c365814`). The expression form fails to plan.

Up: [map.md](map.md).
