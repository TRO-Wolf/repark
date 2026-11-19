# Card DECIMAL256-SUM-WRAP-1: a Parquet `decimal256(76,0)` file: `avg` and `sum` answer wrong values where Spark refuses the read

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** the step 0 critic verdict of AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 (Opus 5.5 medium, 2026-11-19, PR #1365), its "Step 3" item 2 and its "Step 5" shapes table, the `Decimal256 near 2^256 (m_d256)` row; kept outside the repository.

**Status:** filed 2026-11-19, not dispatched. The `avg` half is the seat A row the verdict calls wrong; the `sum` half is outside the `avg` unit (the verdict: "Decimal256 `sum` is not #1360's carrier").

**Rating:** not rated; rating owed; measured once by the AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 step 0 critic.

## Finding

A Parquet `decimal256(76,0)` file (`m_d256`, 12 rows, exact total `2^256 + 5`): `avg` answers `0.4167` and `sum` answers `5`, silently, where Spark refuses the read (`DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION`). `avg_decimal.rs:129-134` adds with `wrapping_add`, with no count, and its `state` still ships the marker.

**Main:** RePark main answers `avg` `0.4167` in the window (steady) and in grouped and ungrouped cells (VARIES, with NULL in 5 of 7 configurations; the verdict's Step 5 and Step 3, measured on `pkg-main-4c365814`). `sum` answers `5` on the same file.

Up: [map.md](map.md).
