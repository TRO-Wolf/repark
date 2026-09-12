# map — task/roadmap/mid-term/

## Purpose
Evaluated intakes awaiting an owner charter. A document here names a unit list and the
measurements behind it; it leaves when the owner charters it (a brief under `briefs/`) or
declines it (a dated ruling in the intake, then the archive).

## Contents
- [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) — **the 2026-09-08 owner slate,
  cut for mechanical-tier workers:** the owner rulings (5+5 polars edges, lazy `repr` shows the
  schema — R-22 of 2026-09-10 superseding R-2's data render, card DISPLAY-LAZY-1 — `repark.toml` pulled ahead of 1.2/1.3, the Ballista audit joins the Rust migration pilot,
  fixtures pin `spark` style, the object stays a RePark DataFrame) and seven work cards with
  pre-made decisions, one step per worker round, tier per step, red-first pin names, gates and
  hand-back conditions: SQL-DESCRIBE-1, DF-EXPLAIN-1, DISPLAY-POLARS-1, CFG-1, DF-EAGER-1,
  BALLISTA-AUDIT-0, PROFILES-1; plus the ADAPT-PART and DYNCFG-1 epic intakes and the sequence.
- [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md) — **slate 2 (owner-chartered
  2026-09-09):** roadmap 2.1 maintenance policy (`[<profile>.maintenance]` + `CALL run_maintenance()`,
  the ADAPT-PART AP-1 planner underneath), 1.2 torture-test dataset suite (eight generated families,
  the secrets flag), 1.3 Never-OOM spill-coverage matrix (27 cells, three outcomes, subprocess-guarded);
  four cards in slate 1's format with pre-made decisions, tiers per step and red-first pins. **Run-8 rulings S2-19…S2-22 (2026-09-11):** fork card F-WRITE-COMPRESS-2 (compaction and the rewrite writers carry the codec; Q-1 deferred to its re-measure), STATUS sentences ride v1.4, perf reviewers return as Grok critic rounds on product branches, card EX-31 (the inventory drops six `Column.*` plumbing names and `F.PythonUDFColumn`), card PERF-DESCRIBE-1 (one aggregate pass for describe/summary, from the first S2-21 review). **RP-17 rulings S2-23…S2-24 (2026-09-12):** Q-1 ruled — the projection multiplies the uncompressed footer sum (card AP-3); the rewrite writes 1.5× its input's compressed bytes under one codec (fork card F-REWRITE-SIZE-1, then RP-18). **S2-25 (2026-09-12):** the cause measured — dead dictionary pages on overflowing rewrite chunks plus a zstd default of 1 versus Java's 3; step 2 ruled (per-column dictionary from the input footers, level 3 default). **S2-26 (2026-09-12):** two engine costs the perf reviews measured become cards — PERF-UNPIVOT-1 (a native unpivot primitive; describe back on a pure plan) and PERF-CAST-1 (where a CAST costs a millisecond).
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
- [overnight-report-2026-09-10-run6.md](overnight-report-2026-09-10-run6.md) — run 6 (2026-09-10
  15:06 → 2026-09-11, the review-fix run, one orchestrator across two processes): review-fix-slate
  §1 rows 1–11 plus M-0 and O-1, with every size-baseline question answered by a comment-funded
  ratchet down rather than a raise, and the evening grant change to Devin SWE-2 for M-tier rounds.
- [overnight-report-2026-09-11-run7.md](overnight-report-2026-09-11-run7.md) — run 7 (2026-09-11, Devin-first): REVIEW-FIX-15b, TORTURE-1 steps 3–5, AP-1 step 2, BALLISTA-M2-A steps 1–2 merged (#496–#501, #503); NEVEROOM-1 step 2 merged (#503, 24 of 27 cells stable); owner questions on uncompressed INSERT, typed scan accessors, the secrets flag scope.
- [overnight-report-2026-09-11-run8.md](overnight-report-2026-09-11-run8.md) — run 8 (2026-09-11, Grok-only workers): NEVEROOM-1 step 3 and the v1.3.0 release PR, fork F-MINIO-QUAY and F-WRITE-COMPRESS-1, BALLISTA-M2-B, RP-16 with PERF-CATALOG-CACHE-WEIGHT-1 closed, AP-2, and the AP-1 re-measure (#507–#514 plus fork #276/#277); findings: Docker Hub dropped the MinIO images, `rewrite_data_files` writes uncompressed files (card F-WRITE-COMPRESS-2), owner question Q-1 on the byte-ratio model.
- [overnight-report-2026-09-10-run5.md](overnight-report-2026-09-10-run5.md) — run 5 (2026-09-10,
  05:21–13:20 local, **orchestrator A**; the Grok/Ballista and facade lanes were orchestrator B's
  from 05:45 and are not in it): seven merged (DISPLAY-BRIDGE-1's rebase, AP-0 with its O-run,
  MAINT-POLICY-1 end to end, TORTURE-1 steps 1 and 2, PROFILES-1 step 1, AP-1 step 1) and one
  parked green as a draft (NEVEROOM-1 step 1, on two D-2 ruling questions).
  **DISPLAY-BRIDGE-1, AP-0 and MAINT-POLICY-1 complete.** Two measurements changed a decision:
  AP-0's O-run refutes P-3's byte model (76–88 % high, partition count exact), and a live-Spark
  run narrows TORTURE-1's `DATE-INTERVAL-NSBOUND-1` (Spark keeps `date` width; the fix is day
  width, not microsecond). Two audit findings fixed rather than shipped — a public name that had
  escaped the example-coverage AST walk, and a refactor that moved an error's precedence. Runbook
  corrections: run the **whole** parity suite before pushing, a native-needing test directory
  guards its own `conftest.py`, and one clone per live lane.
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
  51 numbered findings (39 CONFIRMED, eleven of them re-run by the orchestrator), fifteen fix cards
  (REVIEW-FIX-1…15, none opened) and eight owner questions. Read it before opening any fix work on
  CFG-1, DISPLAY-POLARS-1, DF-EAGER-1, DF-EXPLAIN-1, SQL-DESCRIBE-1, PROFILES-1, DOCS-LINKS-1,
  LEDGER-READING-1 or BALLISTA-AUDIT-0. D-1 and D-2 coverage is complete; the card closes when this document reaches `main`.
- [review-fix-slate-2026-09-10.md](review-fix-slate-2026-09-10.md) — **the REVIEW-1 disposition
  (2026-09-10):** ten rulings RF-1..RF-10 on the sweep's eight owner questions and Milestone 1's
  codec wall (`datafusion-proto` is taken), the fifteen fix cards ordered security-first behind
  DISPLAY-LAZY-1, the orchestrator steps (ledger departure, ci.yml dual-wire, the M2 seed) and
  card BALLISTA-M2-A.
- [overnight-report-2026-09-10-b.md](overnight-report-2026-09-10-b.md) — **run 5b (2026-09-10):**
  the second orchestrating session of the day, running the Grok lane (Ballista Milestone 1 A→D and
  the REVIEW-1 sweep) and FACADE-AUDIT-0 on Muse beside run 5. Six units merged — FACADE-AUDIT-0 and the whole of
  Ballista Milestone 1 (A, B, C, D) — the decisions taken under G-2 and the process notes the runbook
  should absorb.

## Pointers
- Up: [../map.md](../map.md)
