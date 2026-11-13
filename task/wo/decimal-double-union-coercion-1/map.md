# task/wo/decimal-double-union-coercion-1/ — DECIMAL-DOUBLE-UNION-COERCION-1, the design sketch

The owner's "Defaults" of 2026-11-13 (about 12:12 UTC): an xhigh design sketch before any build, #888's
table folded in, and one xhigh critic over the inventory before any code. Measured on main `04d84ab9`
against Spark 4.1.2; documents and probes only, no product code.

- [sketch.md](sketch.md) — the fault, the table's headline, the two seat proposals (a pre-coercion
  Spark common-type rule, recommended; a patched `decimal_coercion`), the unit split (S1 set operations,
  `VALUES` and writes; S2 expression unification and comparisons; four cards) and the six owner
  questions with defaults.
- [domain-table.md](domain-table.md) — 4,436 compared cells in both ANSI modes on the Spark door and the
  DataFrame API, 60 Iceberg write cells read back by Spark, the native-door matrix, and #888's 612 cells
  replayed and split by root.
- [inventory.md](inventory.md) — the insertion inventory: every seat where a DECIMAL meets a FLOAT or
  DOUBLE and a common type is chosen, "seen: yes / no, because", with file:line in DataFusion 54.1.0 and
  RePark, and how it was searched.
- [evidence/](evidence/map.md) — the probes, the raw cells and the provenance.

Up: [../map.md](../map.md).
