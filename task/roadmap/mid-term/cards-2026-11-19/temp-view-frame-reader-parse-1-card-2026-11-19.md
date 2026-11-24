# Card TEMP-VIEW-FRAME-READER-PARSE-1: a JSON- or CSV-reader frame view, and a SQL view over one, keeps main's answer when the zone differs

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's rulings of the 2026-11-18 window (item 5; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md)). **Source:** the [TEMP-VIEW-SESSION-SETTINGS-1 card](../temp-view-session-settings-1-card-2026-11-02.md), its TEMP-VIEW-FRAME-READER-PARSE-1 text, and the part 2 bar, [bar.md](../../../wo/temp-view-session-settings-1/evidence/built-part2/bar.md) (the reader inventory, and "A judgement call: `smartCsv`").

**Status:** filed 2026-11-19, not dispatched.

**Severity: S1, label-blocking** (owner, 2026-11-18, about 14:30 UTC, verbatim "We better go with B to be safe"; B: the card blocks ICE-V2-FLAT-AWS-LABEL-1, and its own unit must land before the label can be declared; S1 by class, the 2026-11-17 "S1 by class" ruling).

## Finding

Spark parses a JSON or CSV source under the reader's settings and decides everything above the parse under the frame's settings. The facade's JSON and CSV readers build their typed parse as an ordinary projection over a scan of strings (`reader.py`, `_apply_reader_schema_semantics`), so the zone lives in a frame expression, and closing the frame parses under the creation zone. A view of either door whose definition reaches a JSON or CSV scan, through any number of views, answers as part 1's package does (the delta 1 invariant under the owner's delegation).

- Cells: the reader sweep's non-EQUAL JSON and CSV lineage cells, 330 of 524 (`raw/delta1/readers-*.jsonl`, in the bar's evidence); Fable's `json/derived_hour/*`; the verifier's `csv/sqlview_hour_utc/*`.
- The shape the unit must reach: `74c22dfe`'s 20 `csv_s` EQUAL cells.
- Example in the ruling: `hour(ts)` answers 3 where Spark answers 19.

**Main:** keeps its own answer when the zone differs, and the view part 2 leaves that answer unchanged. The owner's ruling 5 gives the example: `hour(ts)` answers 3 where Spark answers 19.

**2026-11-19, step 0 (Fable's guidance).** Step 0 is PR #1364. The critic (Opus 5.5 medium) returned GAPS. The revision amended seat (a): the first optimizer rule decides, under the live session at every depth, with a cap of nine files. Fable's guidance accepted it with no second critic round. The split: unit 0 is CSV-SCHEMA-TIMESTAMP-ZONE-1, then unit 1 is seat (a), stacked on it. This is Fable's guidance under the owner's delegation of 2026-11-19, not an owner ruling; see [sketch §10](../../../wo/temp-view-frame-reader-parse-1/sketch.md).

- **2026-11-19 — built.** Unit 1 rebuilt on main `8bc26cba` (after #1367 and #1370): every bar count holds, 0 EQUAL cells lost, C-012 1.002x; record [built/](../../../wo/temp-view-frame-reader-parse-1/built/).

Up: [map.md](map.md).
