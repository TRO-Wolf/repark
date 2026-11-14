# task/wo/double-decimal-store-round-1/evidence/step0/ — step 0, measurement and inventory

Step 0 of DOUBLE-DECIMAL-STORE-ROUND-1 (2026-11-13), before any code and before the xhigh inventory
critic (owner, 2026-11-13, "Defaults"). Measured on main `2707924e` against Spark 4.1.2 in both ANSI modes.

- [domain-table.md](domain-table.md) — 21 values × 5 DECIMAL targets × DOUBLE and FLOAT × 16 doors × both
  ANSI modes: the verdicts by door, the value matrices, the card, container, subscript and `TRY_CAST`
  cells, the provenance and the headline.
- [spark-rule.md](spark-rule.md) — Spark's rule from the `v4.1.2` source with file and line: the double's
  JDK 17 `Double.toString` text, half up, NULL for a non-finite value, a store raising on overflow.
- [inventory.md](inventory.md) — every seat where a FLOAT or DOUBLE becomes a DECIMAL (Arrow, DataFusion,
  RePark plan and batch seats, the iceberg-rust fork), "seen: yes / no, because", how it was searched.
- [probes/](probes/map.md) — the comment-free runners and the renderer.
- [raw/](raw/map.md) — one JSON object per cell and engine.

Up: [../map.md](../map.md).
