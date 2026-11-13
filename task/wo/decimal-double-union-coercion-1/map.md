# task/wo/decimal-double-union-coercion-1/ — DECIMAL-DOUBLE-UNION-COERCION-1, the design sketch

The owner's "Defaults" of 2026-11-13 (about 12:12 UTC): an xhigh design sketch before any build, #888's
table folded in, and one xhigh critic over the inventory before any code. Measured on main `04d84ab9`
against Spark 4.1.2; documents and probes only, no product code.

- [sketch.md](sketch.md) — the fault, the table's headline, and (revised after the inventory critic)
  the two-seat design: seat V marks multi-row `VALUES` cells at the Spark-door parse and types them at
  the head of the analyzer list, seat E widens every other context at idx 6 with the INTERSECT / EXCEPT
  discriminator and the RecursiveQuery exclusion; what a patched `decimal_coercion` would cover; the
  build waits for SIGNED-ZERO-NORMALIZE-1 part B; cost and pruning; the split and eight questions.
- [domain-table.md](domain-table.md) — 4,540 compared cells in both ANSI modes on the Spark door and the
  DataFrame API (66 wrong writes, 702 wrong reads), the Iceberg writes read back by Spark, the revision's
  write doors, signed-zero and pruning cells, the native-door matrix, and #888's 612 cells by root.
- [inventory.md](inventory.md) — the insertion inventory: every seat where a DECIMAL meets a FLOAT or
  DOUBLE and a common type is chosen, "seen: yes / no, because", with file:line in DataFusion 54.1.0 and
  RePark, how it was searched, and section G: the doors the critic found.
- [evidence/](evidence/map.md) — the probes, the raw cells and the provenance.

Up: [../map.md](../map.md).
