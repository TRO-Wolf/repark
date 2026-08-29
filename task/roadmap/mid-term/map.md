# map — task/roadmap/mid-term/

## Purpose
Evaluated intakes awaiting an owner charter. A document here names a unit list and the
measurements behind it; it leaves when the owner charters it (a brief under `briefs/`) or
declines it (a dated ruling in the intake, then the archive).

## Contents
- [iceberg-rust-handoff-2026-08-23.md](iceberg-rust-handoff-2026-08-23.md) — **the fork-side
  handoff (2026-08-23):** the document handed to the owned `iceberg-rust` fork's orchestrator —
  every fork-side item the 2026-08-23 intake surfaced (position-delete rewrite admission gate,
  expire-report split, dangling-delete removal in rewrite, `RewriteManifests` result counts,
  `ReplacePartitions` remainder, branch commit target, metadata-table projection, S3 Tables
  `register_table`, declared sort order → output ordering, and the format-v3 spine (F-7,
  plus F-13–F-15 added 2026-08-23 with the v1.0 north-star ruling): the deletion-vector write
  path, the Hadoop metadata-pointer math, the v3 type system), each with the engine-side
  evidence, the consumed surfaces a change must not break, and the engine pin that flips when
  it lands (F-15 excepted — it carries neither observation nor pin until V3-6 charters).
  **F-16 added 2026-08-24 from MW-7:** the delete-RATIO candidate clause in `RewriteDataFiles`
  (`tooHighDeleteRatio`, Java `DELETE_RATIO_THRESHOLD_DEFAULT = 0.3`), deferred in the fork, so
  a correctly sized 100 %-dead data file is never compacted and its dead rows are retained
  without bound. Registry `RDF-1`; the engine pin that flips is
  `test_mw7_scale_smoke.py::test_delete_laden_in_band_file_survives_the_runbook`.
  **F-17 added 2026-08-28 from RP-2:** DML that supersedes one blob in a shared Puffin must
  carry every still-live sibling blob. The measured engine fixture loses the untouched sibling
  delete; the fork reuses its maintenance sibling-closure primitive and proves Java read-back.
- [roadmap-intake-2026-08-21.md](roadmap-intake-2026-08-21.md) — **the roadmap intake
  (2026-08-21):** every campaign brief, queue, and grant that had existed only in planning space,
  reduced to eleven open workstreams, one closed ledger, and five items needing verification
  before anything asserts them. Read it to find out whether a piece of work is real, already
  landed, or merely proposed — it is an intake, not a plan of record, and STATUS.md stays the
  SSOT. It carries the **MW campaign** (Iceberg merge-on-read operability), chartered and
  green-lit by the owner on 2026-08-21 with all four of its decisions ruled, plus the
  intake-time measurements MW-0 starts from — including an undeclared `rewrite_data_files`
  result-schema divergence found while verifying the scope.
- [roadmap-intake-2026-08-23.md](roadmap-intake-2026-08-23.md) — **roadmap intake
  (2026-08-23), two tracks.** Track A: the six DuckDB window-operator optimizations evaluated
  against the pinned DataFusion 54.1.0 sources — two already in DataFusion, two upstream
  operator work this engine should not own, two real gaps (non-retractable aggregates over
  sliding frames; sort elision via Iceberg ordering provenance) — proposed as a measure-first
  W-0 battery plus W-1…W-3. Track B: Iceberg merge-on-read readiness at format v2 — the
  verdict (correctness production-grade, operability wired, evidence missing), the ranked gaps,
  and the post-#218 units — the Glue dispatch, MW-4b (S3 Tables leg, owner-gated), MW-5…MW-9,
  DML-A/B/C. Track C points at the fork handoff.

## Pointers
- Up: [../map.md](../map.md)
