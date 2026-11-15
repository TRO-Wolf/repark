# task/wo/signed-zero-const-compare-1/ — SIGNED-ZERO-CONST-COMPARE-1: step 0

SIGNED-ZERO-CONST-COMPARE-1 (step 2b of the DECIMAL-DOUBLE split plan, owner ruling 2026-11-14). Two FLOAT/DOUBLE
constants `0.0` and `-0.0` compare by bits once DataFusion folds them; Spark 4.1.2 compares them equal. Step 0 only:
the domain table on main `d843d37e` and the inventory. No product code.

- [inventory.md](inventory.md) — how the wrong answer is made, every seat that compares or folds two float values (DataFusion 54.1.0, Arrow, RePark, the fork) with "seen: yes / no, because", how each was searched, and the proposed seat.
- [evidence/](evidence/map.md) — the probe, the comparer, the generated table and the raw cells.

Up: [../map.md](../map.md).
