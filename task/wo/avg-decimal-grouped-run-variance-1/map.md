# task/wo/avg-decimal-grouped-run-variance-1/ — AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 step 0

Owner, 2026-11-18 (about 20:05 UTC, "Go with Fable's recommendations on all seven, re-run 1362"): S2, not
label-blocking, built next on lane B after `sum`, as its own unit. Measured on main `6903fb48` (package
`pkg-main-4c365814`) against Spark 4.1.2; documents and probes only, no product code. Card:
[../../roadmap/mid-term/cards-2026-11-19/avg-decimal-grouped-run-variance-1-card-2026-11-19.md](../../roadmap/mid-term/cards-2026-11-19/avg-decimal-grouped-run-variance-1-card-2026-11-19.md).

- [sketch.md](sketch.md) — Spark's `Average` rule read from the 4.1.2 jar (a NULL running sum in the hash
  aggregate's buffer), the measured answers, the two seats (A: exact in every phase by input precision, leading; B:
  one column, a marker only past 128 bits), the cost cases, the kept divergence, the unit split and the owner
  questions.
- [domain-table.md](domain-table.md) — every `avg` cell on main in 11 configurations per ANSI mode, 20 runs each,
  against Spark `local[1]` and `local[4]`: EQUAL, WRONG, RAISE, VARIES; the `sum` side on #1360's head; the
  over-precision files.
- [inventory.md](inventory.md) — every path that adds, merges, ships, spills, converts or evaluates `avg`'s sum and
  count, with file:line, the ones that trust the declared precision or the row layout, and the reuse of #1360's
  carrier.
- [evidence/](evidence/) — the probes, the verdict tables, the Spark answers and the `javap` excerpts.
- [evidence/raw-r1/](evidence/raw-r1/) — **critic round 1 (r1, 2026-11-19):** the critic's UNSOUND verdict
  and the rendered tables that close its eight items. The sketch's §7 holds the revision and amends §1 (Spark's rule
  by operator), §4 (seat A's window, narrow and `Decimal256` paths, the state schema, the kept divergence), §5 (the
  pins) and §6 (Q1 to Q7) in place; the domain table and the inventory gain r1 sections (the codec, the analyzer
  routing, the file sizes).

Up: [../map.md](../map.md).
