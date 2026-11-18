# task/wo/temp-view-session-settings-1/ — TEMP-VIEW-SESSION-SETTINGS-1 step 0 (absorbs VIEW-ANSI-CAPTURE-1)

Owner, 2026-11-17, evening: "dispatch Opus xhigh effort levels for these fixes …"; the orchestrator reads it as this
unit and SUM-DEC-I128WRAP-1 (a reading). Owner, 2026-11-17, "Defaults, merge 1355": S1 by class; the time-zone half
blocks ICE-V2-FLAT-AWS-LABEL-1 (scoped to ANSI on); only the matching path is held to 1.05x. Measured on main
`c22d61a1` (and #1355's package for `avg`) and, in the revision after the critic's UNSOUND verdict, on main
`6f80793b`, against Spark 4.1.2; documents and probes only, no product code. Cards:
[../../roadmap/mid-term/temp-view-session-settings-1-card-2026-11-02.md](../../roadmap/mid-term/temp-view-session-settings-1-card-2026-11-02.md),
[../../roadmap/mid-term/view-ansi-capture-1-card-2026-10-01.md](../../roadmap/mid-term/view-ansi-capture-1-card-2026-10-01.md).

- [sketch.md](sketch.md) — the findings, Spark's capture rule read from the 4.1.2 jars (`CapturesConfig`,
  `ViewHelper$`, `View$.effectiveSQLConf`, `Analyzer$.retainResolutionConfigsForAnalysis`, `ComputeCurrentTime`) and
  what the cells add, the key list, the DataFusion 54.1.0 mechanism and the four traps (filters pushed with the
  reader's semantics), seats 1A/1B, 2A/2B and 3A/3B, the cost cases, the two-part split with every wrong write, eleven
  owner questions (three ruled), what went wrong, and the critic's verdict item by item.
- [domain-table.md](domain-table.md) — 2,725 comparisons over eight keys, the temp-view doors, four positions, writes,
  nested views, reader-side expressions, `now`, the unset case and the revision's cells (filters, caches, held
  derivations, renames, current-time functions, shapes, held writes): 574 view gaps of 1,527 cross-setting cells, the
  follows grid, every wrong write, and the direct gaps seen in passing.
- [inventory.md](inventory.md) — every answer-changing session key with its readers by phase and Spark's capture,
  the ANSI reader counts, and the 23-row insertion inventory ("seen at a seat: yes / no, because").
- [evidence/](evidence/map.md) — probes, raw cells, verdicts, headline and summary, plans, `javap` excerpts,
  provenance.

Up: [../map.md](../map.md).
