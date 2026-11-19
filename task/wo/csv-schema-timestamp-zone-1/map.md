# task/wo/csv-schema-timestamp-zone-1/ — CSV-SCHEMA-TIMESTAMP-ZONE-1 (unit 0 of TEMP-VIEW-FRAME-READER-PARSE-1), built

**The defect (D1 of the parent's step 0, PR #1364).** A CSV read with a user schema whose field is `TIMESTAMP`
parsed the text as UTC whatever the session zone: DataFusion's scan infers a zone-less timestamp and the facade's
typed parse cast it without the zone. Silent, with or without a view, and written wrong by every write door. Spark
parses the text under the session zone at scan time.

**The fix (step 0's seat (c)).** `_load_csv` (`python/repark/src/repark/spark/session/reader.py`) re-reads the
scan's zone-less timestamp columns as text through the existing `utf8_columns` option when the user schema holds a
`TIMESTAMP` field; the typed parse then casts the text under the session zone, as the JSON reader does. On a local
path the scan's pre-scan makes every column text once `utf8_columns` is set (`csv_utf8_schema_from_path`), as the
`nullValue` and `multiLine` schema reads already do. One product file; no Rust.

**Delta 1 (2026-11-19), the verifier's F1 and F2.** That all-text read cast a `DECIMAL` field from exponent text (a
raise) and a `TIMESTAMP_NTZ` field from DST-gap text (an hour forward). `read_csv_path`
(`crates/repark-core/src/read_options.rs`, the second product file) now re-reads only the named columns, except under
`inferSchema`, whose re-read was measured to rely on the all-text read; `_load_csv` leaves a `TIMESTAMP_NTZ` field's
column on the scan's type. See [bar.md](bar.md) and [cost.md](cost.md), "Delta 1".

**Delta 2 (2026-11-19), cost only.** Delta 1's cost came from its build, not its logic: the same plan over the same
scan ran 10 to 15 percent slower in delta 1's native library than in main's. `read_csv_path` is main's again, so the
native library's code is main's; `_load_csv` re-reads an existing local path through its `file://` URL, which main's
code already re-reads named-only. Every bar cell equals delta 1's. K2, `k0_hour` and C-012 are back at the first
head's cost; `k0_count` misses 1.05x (1.08x, the second inference), awaiting a ruling. See [bar.md](bar.md) and
[cost.md](cost.md), "Delta 2".

## The rulings this unit carries

- **Owner, 2026-11-19, about 01:28 UTC, verbatim:** "Go with your recommendations and tonight you don’t even need to
  wait on me even if fable says that it’s my call. Merge what you need, follow the goal and good luck".
- **Fable's guidance under that delegation, not an owner ruling** (`claude-fable-5-2`, medium, 2026-11-19; its reply
  on step 0, §2 Q2 and §4): D1 gets its own card, CSV-SCHEMA-TIMESTAMP-ZONE-1, S1 and label-blocking; it is unit 0,
  built first as its own PR; it moves matching cells, so it cannot share unit 1's bar; the cost cases are K0 (CSV),
  K2 and C-012.
- **The orchestrator's call for delta 1, under that delegation:** take Fable's option (b), which the consult had
  declined on cost alone: on every path `utf8_columns` re-reads only the named columns, with `read_options.rs` as the
  second product file; Fable's cost exception and the 14 runtime `timestampType` cells stand.
- **The orchestrator's call for delta 2, under that delegation, not an owner ruling:** a second delta, past the
  one-delta cap, for cost only; the owner sees it in the morning read and may stop it. Fable's bars stand unchanged.
- **The parent card** TEMP-VIEW-FRAME-READER-PARSE-1 is S1 and label-blocking (owner, 2026-11-18, about 14:30 UTC:
  "We better go with B to be safe").

## Contents

- [bar.md](bar.md) — the bar of step 0's sketch §6 for unit 0, measured: step 0's whole table re-run on main and on
  the head, the reader sweep, part 2's write sweep, the extra shapes, every moved cell, `EXPLAIN` before and after.
- [cost.md](cost.md) — K0 (CSV), K2 and C-012, like-for-like medians against main.
- [evidence/](evidence/map.md) — the probes, the raw cells and the verdicts.

Up: [../map.md](../map.md).
