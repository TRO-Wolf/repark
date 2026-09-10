# map — task/roadmap/mid-term/

## Purpose
Evaluated intakes awaiting an owner charter. A document here names a unit list and the
measurements behind it; it leaves when the owner charters it (a brief under `briefs/`) or
declines it (a dated ruling in the intake, then the archive).

## Contents
- [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) — **the 2026-09-08 owner slate,
  cut for mechanical-tier workers:** eight owner rulings (5+5 polars edges, lazy `repr` renders
  data, `repark.toml` pulled ahead of 1.2/1.3, the Ballista audit joins the Rust migration pilot,
  fixtures pin `spark` style, the object stays a RePark DataFrame) and seven work cards with
  pre-made decisions, one step per worker round, tier per step, red-first pin names, gates and
  hand-back conditions: SQL-DESCRIBE-1, DF-EXPLAIN-1, DISPLAY-POLARS-1, CFG-1, DF-EAGER-1,
  BALLISTA-AUDIT-0, PROFILES-1; plus the ADAPT-PART and DYNCFG-1 epic intakes and the sequence.
- [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md) — **slate 2 (owner-chartered
  2026-09-09):** roadmap 2.1 maintenance policy (`[<profile>.maintenance]` + `CALL run_maintenance()`,
  the ADAPT-PART AP-1 planner underneath), 1.2 torture-test dataset suite (eight generated families,
  the secrets flag), 1.3 Never-OOM spill-coverage matrix (27 cells, three outcomes, subprocess-guarded);
  four cards in slate 1's format with pre-made decisions, tiers per step and red-first pins.
- [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md) — **how a
  cheaper orchestrating session runs the slate unattended:** the four owner grants, the five
  reads, lane/brief/launch commands per launcher, the hand-back audit checklist, the gated
  push/PR/merge chain, bounded decision authority, one night's order, stop conditions, the
  morning report, and the launch command the owner runs.
- [overnight-report-2026-09-09.md](overnight-report-2026-09-09.md) — **what the first
  unattended run of that runbook actually did (night of 2026-09-08/09):** two units merged
  (BALLISTA-AUDIT-0 #426, DF-EXPLAIN-1 #427), SQL-DESCRIBE-1 parked on the D-3 type-spelling
  ruling (#428 draft), DISPLAY-POLARS-1 step 1 of 5 gated green; the six decisions taken under
  the G-2 grant, the two questions parked for the owner, the `JAVA_HOME` and CAP-1-mirror
  environment findings, and why GLM's dropped sockets moved the long rounds to Muse.
- [overnight-report-2026-09-09-night2.md](overnight-report-2026-09-09-night2.md) — **what the
  second unattended run did (morning of 2026-09-09, 05:26–09:00):** three units merged
  (LEDGER-READING-1 #432, SQL-DESCRIBE-1 #428, PREFLIGHT-PARITY-1 #433), DISPLAY-POLARS-1
  step 2 parked because D-5's probe reds the protected full-collect pin (#434 draft); the
  six decisions taken under G-2, the five questions parked for the owner
  (including that `make check-docs-links` does not exist), and five corrections the runbook's
  §3 needs — the `pgrep` guard that matches its own command line, Muse's separate run
  directory, and the CAP-1 map.md merge conflict every lane branch now hits.
- [overnight-report-2026-09-09-run4.md](overnight-report-2026-09-09-run4.md) — run 4 (2026-09-09,
  14:29–22:29 local): seven merged (CFG-1 steps 1b, 2, 3 and 4; DISPLAY-POLARS-1 steps 4 and 5;
  DF-EAGER-1 steps 2 and 3), one parked green (DISPLAY-BRIDGE-1 #454, a semantic merge conflict
  with DF-EAGER-1 in the styled renderer). **DISPLAY-POLARS-1 and DF-EAGER-1 complete**; CFG-1's
  work is done and its ledger waits on one clause. Five §6 decisions, four questions for the owner, three refused ceiling raises that all
  absorbed.
- [overnight-report-2026-09-09-run3.md](overnight-report-2026-09-09-run3.md) — run 3 (2026-09-09,
  08:56–15:00 local): five merged (DISPLAY-POLARS-1 steps 2 and 3, DOCS-LINKS-1, the
  PREFLIGHT-PARITY-1 close, CFG-1 seed + step 1), two open at the stop (PROFILES-1 step 0,
  DF-EAGER-1 step 1), none parked. Eight §6 decisions, three questions for the owner.
- [iceberg-rust-handoff-2026-08-23.md](iceberg-rust-handoff-2026-08-23.md) — **the fork-side
  handoff (2026-08-23; F-3 / V3-DANGLE-1 errata 2026-08-31, V3-5; RP-5 consumed F-6b/F-6c / F-8 / F-16r / F-0 follow-up):** the document handed to the owned `iceberg-rust` fork's orchestrator —
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
  without bound. Registry `RDF-1`. **Residue 2 re-homed 2026-09-02 (RDF-1):** the
  bounds-absent half was RePark's own truncated `file_path` statistics and is fixed here; the
  pin flipped to
  `test_mw7_scale_smoke.py::test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_dies`.
  What still waits on the fork is one delete file naming two or more data files.
  **F-17 added 2026-08-28 from RP-2:** DML that supersedes one blob in a shared Puffin must
  carry every still-live sibling blob. The measured engine fixture loses the untouched sibling
  delete; the fork reuses its maintenance sibling-closure primitive and proves Java read-back.
  Landed fork #237 the same day; RP-3 (2026-08-30) wired the call from the engine's own MOR path.
  **F-5 corrected 2026-08-29:** answered in fork #217 (inside the engine pin) — the static half
  of the ask was void; DML-B is not blocked.
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
  (2026-08-23), two tracks** (Track B's MW-4b row delivered as MW-10 — measured allow 2026-08-30). Track A: the six DuckDB window-operator optimizations evaluated
  against the pinned DataFusion 54.1.0 sources — two already in DataFusion, two upstream
  operator work this engine should not own, two real gaps (non-retractable aggregates over
  sliding frames; sort elision via Iceberg ordering provenance) — proposed as a measure-first
  W-0 battery plus W-1…W-3. Track B: Iceberg merge-on-read readiness at format v2 — the
  verdict (correctness production-grade, operability wired, evidence missing), the ranked gaps,
  and the post-#218 units — the Glue dispatch, MW-4b (S3 Tables leg, owner-gated), MW-5…MW-9,
  DML-A/B/C. Track C points at the fork handoff.

- [review-1-findings-2026-09-10.md](review-1-findings-2026-09-10.md) — **the REVIEW-1 critic
  sweep (2026-09-10):** twenty-four Grok critic rounds over the units merged since 2026-09-08, with
  51 numbered findings (39 CONFIRMED, nine of them re-run by the orchestrator), fifteen fix cards
  (REVIEW-FIX-1…15, none opened) and eight owner questions. Read it before opening any fix work on
  CFG-1, DISPLAY-POLARS-1, DF-EAGER-1, DF-EXPLAIN-1, SQL-DESCRIBE-1, PROFILES-1, DOCS-LINKS-1,
  LEDGER-READING-1 or BALLISTA-AUDIT-0. D-1 and D-2 coverage is complete; the card closes when this document reaches `main`.
- [overnight-report-2026-09-10-b.md](overnight-report-2026-09-10-b.md) — **run 5b (2026-09-10):**
  the second orchestrating session of the day, running the Grok lane (Ballista Milestone 1 A→D and
  the REVIEW-1 sweep) and FACADE-AUDIT-0 on Muse beside run 5. Three units merged, M1-C open,
  M1-D in flight at the close; the decisions taken under G-2 and the process notes the runbook
  should absorb.

## Pointers
- Up: [../map.md](../map.md)
