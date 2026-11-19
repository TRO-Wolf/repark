# task/wo/string-hour-minute-second-1/ — STRING-HOUR-MINUTE-SECOND-1 step 0 (measured sketch, no product code)

Fable's guidance under the owner's delegation of 2026-11-19 (about 01:28 UTC): S1 by class, label-blocking, its own
unit. Measured on main `2fdab5c7` (package `pkg-main-4c365814`) against Spark 4.1.2; documents and probes only. Card:
[../../roadmap/mid-term/cards-2026-11-19/string-hour-minute-second-1-card-2026-11-19.md](../../roadmap/mid-term/cards-2026-11-19/string-hour-minute-second-1-card-2026-11-19.md).

- [sketch.md](sketch.md) — Spark's rule from the jar (a `STRING` takes the implicit cast to `TIMESTAMP_LTZ`, the
  written cast's twin, in both ANSI modes, never `spark.sql.timestampType`), what main does (`coerce_date_arg` sends
  the text to `Date32`), the two seats (A: coerce to the instant, leading; B: parse in the kernel), the cost cases,
  the unit split and six questions.
- [domain-table.md](domain-table.md) — 75,032 cells per engine: 16 texts × 4 inputs × 4 projection doors × 58
  functions × 4 configurations, six multi-row doors, two `TIMESTAMP_NTZ` configurations; EQUAL / WRONG / RAISE; the
  card's three functions, Spark's checks, seat A's forecast, the grid by function and text, neighbours N1–N11.
- [inventory.md](inventory.md) — the insertion inventory: name resolution (R1–R9), coercion (C1–C10), evaluation
  (E1–E6) with file:line, why `year` is right and `hour` is not, what each seat sees.
- [evidence/](evidence/map.md) — probes, verdicts, `EXPLAIN` and cost records, `javap` excerpts.

Up: [../map.md](../map.md).
