# map — repark-spark/src/call/changelog

## Purpose

File-backed tests for the changelog row transforms (`../changelog.rs`) — the Spark-engine half of
`create_changelog_view`, ported from Java's `ChangelogIterator` family and pinned against the
recorded `QC-*` cells.

## Contents

- `tests.rs` — the recorded `create_changelog_view` fixture as synthetic rows (three appends, a
  copy-on-write UPDATE, a whole-file DELETE): carryover removal keeping 8 of 10 rows (`QC-DEFAULT`),
  net changes keeping 4 with the last touching ordinal (`QC-NET`), `compute_updates` pairing one
  ordinal's DELETE and INSERT while the unpaired DELETE stays a DELETE (`QC-UPDATES-IDENT`), the
  same answer from a non-unique identifier column (`QC-SQL-UPDATE-DUP`), Java's
  multiple-rows-per-identifier refusal, and the two window edges no cell covers (a delete and
  insert one snapshot apart; a row deleted and re-inserted inside the window). Round 2
  (2026-09-20) carries `_commit_snapshot_id` in the `rendered()` helper, so every value
  assertion pins the commit column too.
  pins: ice-changelog-1/C-011, C-012, C-013
