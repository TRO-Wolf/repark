# task/wo/decimal-common-type-1/ — DECIMAL-COMMON-TYPE-1, step 0

The owner's "defaults" of 2026-11-16 (about 14:40 UTC): one S1, label-blocking unit for Spark's decimal
least-common-type family, matching Spark's lossy comparison and union; step 0 "go" about 20:55 UTC. Measured on main
`e50ed17f` against Spark 4.1.2; documents and probes only, no product code. Card:
[../../roadmap/mid-term/decimal-decimal-union-widen-1-card-2026-11-16.md](../../roadmap/mid-term/decimal-decimal-union-widen-1-card-2026-11-16.md).

- [sketch.md](sketch.md) — the fault, Spark's rule as measured (integer forms, the integer-literal exception,
  per-node union rounding, one rounding per function and `VALUES`), the counts, seat A (one shared function in
  `SparkDecimalFloatWiden`'s walk and seat V, recommended) and seat B (a retarget rule after `type_coercion`), the
  legacy flag, the cost cases, how sweep families 3–6 and 10 join, the split and seven owner questions.
- [domain-table.md](domain-table.md) — 1,248 Spark-door cells per ANSI mode and 907 native cells: 14 type pairs by
  46 positions, constants both ways, three types, aggregates, `VALUES`, literals, windows and grouping, the doors,
  every Iceberg wrong write, the legacy flag and the other causes measured beside the family.
- [inventory.md](inventory.md) — every seat that picks a decimal common type, "seen: yes / no, because", in
  DataFusion 54.1.0 and RePark main, with the doors.
- [evidence/](evidence/map.md) — the probes, the raw cells, the verdicts and the provenance.

Up: [../map.md](../map.md).
