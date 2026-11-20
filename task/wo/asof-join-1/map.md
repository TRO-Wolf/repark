# task/wo/asof-join-1/ — ASOF-JOIN-1 step 0 (measured sketch, no product code)

PySpark's private as-of join `DataFrame._joinAsOf`, and `pyspark.pandas.merge_asof` on top of it, against RePark,
which has no door for either. Asked by the owner on 2026-11-20; rating proposed (S3), not ruled. Measured on main
`9a48932f` (product of `a4d8857f`) against Spark 4.1.2; documents and probes only.

- [sketch.md](sketch.md) — Spark's rule from the jar and live, RePark today, the domain in short, two seats with
  files, caps, cells, memory, `EXPLAIN` and cost, the unit split, the questions, what the brief got wrong.
- [domain-table.md](domain-table.md) — every verdict: the direct call and the two workarounds against Spark, by
  group, type, direction and mode; the errors, mixed types, bounds, NaN, ties, call forms, SQL and `merge_asof`.
- [inventory.md](inventory.md) — the insertion inventory: every site an as-of join would enter, with file:line.
- [card.md](card.md) — the card's text for the orchestrator's clerk to file.
- [evidence/](evidence/map.md) — probes, raw cells (gzipped), plans, cost samples, `javap` excerpts, provenance.

Up: [../map.md](../map.md).
