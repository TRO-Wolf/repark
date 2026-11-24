# task/wo/csv-string-field-text-1/ — CSV-STRING-FIELD-TEXT-1, step 0 (a measured sketch)

**The defect (the card, S1 by class, label-blocking).** A CSV read that does not infer types (a user schema, or no
schema with `inferSchema` false or unset) answers DataFusion's inferred value cast to the declared type, where Spark
4.1.2 parses each field's text by the declared type: a `STRING` field over `007` answers `7`, and the default read
rewrites `1.50`, `1e3` and `TRUE`. N1 folds in (the owner's "go with recommendations", 2026-11-19).

**Step 0 (2026-11-20, Claude Opus 5.5 xhigh; no product code).** 7,704 cells per build, identical on main `6d3739ab`
and unit 1's head `acd27d6e`: 1,908 WRONG, all silent, and 2,670 RAISE where Spark answers. Seat A: one all-text scan
in Rust, a Rust Spark-CSV field kernel and positional pairing; split into two units. Revised the same day after the
critic's GAPS (268 cells re-measured, 0 disagreements): 5,156 more records, the oracle, eight questions.

- [sketch.md](sketch.md) — Spark's rule, the reach, the two seats, cost estimates, the split and seven questions.
- [domain-table.md](domain-table.md) — the cells by read, door, type, shape, ANSI mode and the label path, and the
  seat emulations.
- [inventory.md](inventory.md) — every site that decides or casts a CSV column's type, with file:line.
- [oracle.md](oracle.md) — Spark's answer per declared type (16 types × 74 texts) and in three non-UTC zones: the
  kernel unit's test oracle.
- [evidence/](evidence/) — the probes and the raw cells.
- [kernel/](kernel/) — CSV-FIELD-KERNEL-1 (2026-11-20): the kernel unit's bar, cost and probes; the oracle
  gained 62 measured texts.
- [built/](built/) — CSV-STRING-FIELD-TEXT-1's wiring unit (2026-11-20): the text scan, the kernel per field by
  position, the bar, the cost and the probes.

Up: [../map.md](../map.md).
