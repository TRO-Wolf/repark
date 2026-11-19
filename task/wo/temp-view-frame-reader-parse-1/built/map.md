# task/wo/temp-view-frame-reader-parse-1/built/ — TEMP-VIEW-FRAME-READER-PARSE-1 unit 1, built (seat (a) amended)

**The defect.** A DataFrame temp view, a SQL view over one, and a held frame whose lineage reaches a JSON or CSV
reader answered as main does when the session's settings changed after the frame was built. Part 2 (#1362) exempted
that lineage, because the facade builds the typed parse as a projection over a string scan. Spark parses JSON and CSV
at scan time under the live conf of the session that made the relation (`FileSourceScanExec` → `buildReader` →
`getSqlConf`), and `ResolveTimeZone` stamps the expressions above the relation when the frame is analysed.

**The fix (step 0's seat (a), amended, with Fable's G1 to G4).** The facade wraps each JSON and CSV reader frame in a
`ReaderParse` node (`crates/repark-core/src/extension.rs`): its parse plan as its input and as a private unanalysed
copy, the session's one `LiveOptions` handle and the frame's analyzer. Analysis leaves the node in place. The
optimizer's first rule, `repark_reader_parse` (`crates/repark-spark/src/view_ddl/frame_settings.rs`), compares the
live options with those the plan is optimized under: the same, the node becomes its analysed input; different, it
becomes a closed `FrameView` over the copy, analysed under the live settings. Part 2's reader-lineage exemption and
the SQL door's fallback are removed; `write_plan` hashes the node as its input.

**This unit changes answers with no view involved.** A held JSON or CSV frame built before a `SET` and acted on after
it (`collect()`, `cache()` then an action, a path write, an exact-key join) now parses under the live settings and
evaluates the expressions above the parse under the frame's, as Spark does. Step 0's sentence "the parse's own cells
must not move" (sketch §8) held only for the doors step 0 measured.

## The rulings this unit carries

- **Owner, 2026-11-18, about 14:30 UTC, verbatim:** "We better go with B to be safe". The card is S1 and
  label-blocking.
- **Owner, 2026-11-19, about 01:28 UTC, verbatim:** "Go with your recommendations and tonight you don’t even need to
  wait on me even if fable says that it’s my call. Merge what you need, follow the goal and good luck".
- **Fable's guidance under that delegation, not an owner ruling** (`claude-fable-5-2`, medium, 2026-11-19; its reply
  on step 0, recorded in [../sketch.md](../sketch.md) §10): amended seat (a) is sound, with no second critic round;
  the four specification fixes G1 to G4 go into the build; the held-frame family of its §1.4 goes into the bar; Q9
  carries a stop rule (if any of the 12 E5 cells is not EQUAL in `ph` or `b`, add no file and leave it to
  TEMP-VIEW-FRAME-CROSSJOIN-1); Q11 (`EXPLAIN VERBOSE`) is accepted and disclosed; the cap is nine files.
- **Owner, 2026-11-19, about 14:00 UTC:** "go with recommendations" (adopting Fable's reply on the reader-parse units, §2.2, §3, §4 and §5, including the short-row cost rule).

## Contents

- [bar.md](bar.md) — the bar of sketch §6 unit 1 plus Fable's §1.4, checked per column against main `8bc26cba` (the base package),
  the head package built from `ba1957bf`, and Spark 4.1.2: every set, every moved cell, the E5 stop rule, `udf` and `PREPARE`, `EXPLAIN` before and
  after, G1 to G4 with their cells and the root-shape rule list.
- [cost.md](cost.md) — K0 to K4, like-for-like medians against main `8bc26cba`, head package built from `ba1957bf`.
- [evidence/](evidence/map.md) — the raw cells and the verdicts.

Up: [../map.md](../map.md).
