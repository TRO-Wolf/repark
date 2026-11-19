# task/wo/temp-view-frame-reader-parse-1/ — TEMP-VIEW-FRAME-READER-PARSE-1 step 0 (measured sketch, no product code)

Owner, 2026-11-18: "We better go with B to be safe" (the card blocks ICE-V2-FLAT-AWS-LABEL-1, S1 by class) and
"Defaults on all three" (`smartCsv` keeps the creation zone). Measured on main `4c365814` and on `6286ba19` (part 1
only) against Spark 4.1.2; documents and probes only. Card file: filed by the records clerk under
`task/roadmap/mid-term/cards-2026-11-19/` (branch `docs/rulings-2026-11-19`). Parent unit:
[../temp-view-session-settings-1/](../temp-view-session-settings-1/map.md).

- [sketch.md](sketch.md) — the findings, Spark's rule from the jars (the parse reads the live session at scan time;
  everything above binds at the frame's analysis), RePark's mechanism, seats (a), (b) and (c), the cost cases, the
  unit split and seven owner questions.
- [domain-table.md](domain-table.md) — 5,732 cells per build: 382 zone, 30 ANSI and 128 write view cells, no
  timestamp-type view gap, 0 moves against part 1 in the lineage, the emulation readers, every wrong write, eight
  direct gaps (D1, the CSV schema timestamp parsed as UTC, silent).
- [inventory.md](inventory.md) — the insertion inventory: 17 parse sites and 11 entries, "seen at the seat" for each.
- [evidence/](evidence/map.md) — probes, raw cells, verdicts, tables, `javap` excerpts, provenance.

Up: [../map.md](../map.md).
