# map — task/roadmap/mid-term/

## Purpose
Evaluated intakes awaiting an owner charter. A document here names a unit list and the
measurements behind it; it leaves when the owner charters it (a brief under `briefs/`) or
declines it (a dated ruling in the intake, then the archive).

## Contents
- [overnight-report-2026-09-14-run13b.md](overnight-report-2026-09-14-run13b.md) — **run 13b report (2026-09-14,
  expressions, beside run 13):** REPLACE-LINEAR-1 step 1 finished by Devin (flat searched CASE, Spark semantics
  per Q-R1, 277 MB → ~5.4 MB at 16 entries) and parked as draft #577 at the 06:30 stop, with critic, reviewer and
  preflight still owed. ARRAY-NULL-1 and ANSI-DOOR-1 not started. Owner questions Q-13b-1..4 with recommendations.
- [overnight-report-2026-09-13-run12b.md](overnight-report-2026-09-13-run12b.md) — **run 12b report (2026-09-13,
  cache and expressions, beside run 12):** EAGER-BUDGET-1 merged (#569: `repark.cache.max_total_bytes` refuses and
  never evicts, distinct-buffer `repark.cache.retained_bytes`, incremental admission; the per-call slowdown did not
  reproduce under 2/4/8 GB caps). REPLACE-LINEAR-1 step 0 merged (#571), with step 1 parked on owner question Q-R1 (nine
  Spark divergences). ARRAY-NULL-1 and ANSI-DOOR-1 filed as cards. Owner questions Q-B1, Q-B2, Q-R1, Q-R2 with
  recommendations.
- [array-null-1-card-2026-09-13.md](array-null-1-card-2026-09-13.md) — **card ARRAY-NULL-1 (2026-09-13, run 12b
  under G-5; ABS-EXPR-1's C-004 audit residue):** `F.array_append` / `F.array_prepend` compose
  `when(isnull(arr), NULL).otherwise(flatten(array(arr, array(x))))`, embedding the array twice per level
  (19 MB at depth 12, 66 MB at 14), because DataFusion's kernels drop the input null buffer. D-1 a
  null-preserving native arm measured first between a wrapping `ScalarUDF` and a Rust-built single-reference CASE
  (HALT if neither is linear); D-2 Spark oracle cells (NULL array, NULL element, empty, nested, coercion); D-3 the
  SQL door's divergence row; step 0 measures and lands the red-first depth-40 memory pin. Not started.
- [ansi-door-1-card-2026-09-13.md](ansi-door-1-card-2026-09-13.md) — **card ANSI-DOOR-1 (2026-09-13, run 12b,
  card only):** the SQL door's datafusion-spark kernels read `execution.enable_ansi_mode`, which repark never sets,
  so `spark.sql("SELECT abs(x)")` wraps on signed minimums while the facade raises `ARITHMETIC_OVERFLOW` as Spark
  does with ANSI on. D-1 wire `spark.sql.ansi.enabled` (session build and runtime set) to that option; D-2 measure
  every ANSI-aware door kernel against the oracle first; D-3 the `abs` row leaves `EXPECTED_DIVERGENCES`. Not started.
- [eager-budget-1-card-2026-09-13.md](eager-budget-1-card-2026-09-13.md) — **card EAGER-BUDGET-1
  (2026-09-13, run 11 §7; worked by run 12b):** a session cache budget and retained-bytes
  accounting — `repark.cache.max_total_bytes` refuses (never evicts, Q-E2) when a
  materialization would push the sum of distinct Arrow buffers across live
  `__repark_cache_*` MemTables past the budget (D-1/D-2), admission checked incrementally
  per batch before the next pull (D-3), `repark.cache.max_bytes` unchanged in meaning (D-4),
  no spill (D-5), no registration or handle after a refusal or collection failure (D-6);
  step 0 measures the 30-iteration loop under 2/4/8 GiB cgroup caps, base vs main.
- [replace-linear-1-card-2026-09-13.md](replace-linear-1-card-2026-09-13.md) — **card
  REPLACE-LINEAR-1 (2026-09-13, run 12b under G-5; ABS-EXPR-1's C-004 audit residue):**
  `DataFrame.replace`'s dict loop nests the running expression once per mapping entry —
  `when(expression == lit(old), lit(new)).otherwise(expression)` embeds the previous
  expression twice, so N entries reference the column 2^N times (28 MB at 12, 297 MB at
  16). D-1 builds ONE searched `CASE WHEN col = a THEN b … ELSE col END` per target
  column (linear in entries); D-2 pins the live PySpark 4.1.2 oracle cells first (NULL
  key/value, subset shapes, a missing column, type coercion, list vs scalar `value`,
  the `{1: 2, 2: 3}` mapping-order case); D-3 rules that if the oracle answers 2 the
  CASE follows Spark; D-4 keeps `core.py`'s ceiling and puts any helper in a sibling
  module. Step 0 measures the oracle cells and lands the red-first 40-entry memory
  pin; step 1 rewrites.
- [eager-own-1-card-2026-09-13.md](eager-own-1-card-2026-09-13.md) — **card EAGER-OWN-1
  (2026-09-13, owner charter; run 11):** eager results own their materialization — a refcounted
  handle held by every frame that scans a cache view (D-2), eager-on-eager reuses the backing
  (D-4), no plan-equivalence caching (D-5), budget deferred to EAGER-BUDGET-1 (D-6); step 0
  measures the million-row TA loop first; owner questions Q-E1 (cache()/persist() lifetime) and
  Q-E2 (session budget policy).
- [eager-materialization-retention-review-2026-09-13.md](../../../docs/history/eager-own-1/eager-materialization-retention-review-2026-09-13.md)
  — **the owner's source review behind EAGER-OWN-1 (2026-09-13, verbatim):** bare `eager()`
  calls each register an unowned `__repark_cache_*` MemTable, the findings table, usage guidance,
  the three proposed corrections and the validation table the unit's pins follow. Archived to
  [docs/history/eager-own-1/](../../../docs/history/eager-own-1/map.md) on 2026-09-13 at the
  unit's close.
- [dyncfg-1-cards-2026-09-12.md](dyncfg-1-cards-2026-09-12.md) — **the DYNCFG-1 intake cut
  (2026-09-12, Devin reading round at `0233d96f`):** the sweep verdict — the entry criterion is
  met (three re-measure-confirmed read-profile wins, ten regression-only knobs, five no-effect,
  per-knob table traced to the committed CSVs) — rulings DC-1…9 the numbers decide (matrix
  membership, exclusions, the derivation rule, the measured per-cell budget), owner questions
  DQ-1…7 (write-back target, key names, writes, the sample, regression knobs, live-session
  semantics, overrun behaviour), and five cards: DYNCFG-1-BED (the reproducible, budgeted
  harness first), then TOML / RUNNER / WRITEBACK / DOCS behind the DQ rulings.
- [rest-catalogs-intake-2026-09-12.md](rest-catalogs-intake-2026-09-12.md) — **generic Iceberg
  REST catalog support (Lakekeeper, Apache Polaris), scoped 2026-09-12 by an Opus reading agent at
  fork pin `9e3522e3`:** the owned fork already ships a complete `iceberg-catalog-rest` (OAuth2 client
  credentials with proactive refresh, bearer tokens, `header.*`, `/v1/config` prefix routing,
  pagination, views, the commit-outcome taxonomy MERGE and compaction rely on, vended-credential
  overlay by longest prefix) that RePark consumes nowhere; the thirteen ranked gaps by owning layer
  (no `rest` catalog kind; one storage factory per catalog chosen from a scheme a REST warehouse
  never shows; vended-credential lifetime — the fork's R160; namespace property updates; views;
  `header.*` in `repark.toml`; SigV4), the zero-dependency-delta measurement, the CI-only docker
  strategy, the slate REST-0…4 plus four fork cards, and decisions REST-D-1…8 for the owner.
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
  four cards in slate 1's format with pre-made decisions, tiers per step and red-first pins. **Run-8 rulings S2-19…S2-22 (2026-09-11):** fork card F-WRITE-COMPRESS-2 (compaction and the rewrite writers carry the codec; Q-1 deferred to its re-measure), STATUS sentences ride v1.4, perf reviewers return as Grok critic rounds on product branches, card EX-31 (the inventory drops six `Column.*` plumbing names and `F.PythonUDFColumn`), card PERF-DESCRIBE-1 (one aggregate pass for describe/summary, from the first S2-21 review). **RP-17 rulings S2-23…S2-24 (2026-09-12):** Q-1 ruled — the projection multiplies the uncompressed footer sum (card AP-3); the rewrite writes 1.5× its input's compressed bytes under one codec (fork card F-REWRITE-SIZE-1, then RP-18). **S2-25 (2026-09-12):** the cause measured — dead dictionary pages on overflowing rewrite chunks plus a zstd default of 1 versus Java's 3; step 2 ruled (per-column dictionary from the input footers, level 3 default). **S2-26 (2026-09-12):** two engine costs the perf reviews measured become cards — PERF-UNPIVOT-1 (a native unpivot primitive; describe back on a pure plan) and PERF-CAST-1 (where a CAST costs a millisecond). **S2-27 (2026-09-12):** AP-1-R-001 closes as an estimator property — the projection is an upper bound (the rewrite now compresses better than its inputs); card AP-1-CLOSE-1 pins it and retires the S2-24 caveat. **S2-28 (2026-09-12):** the owner's S3 Tables orphan bug — a bare-bucket location (fork card F-S3ROOT-1) and a table bucket that refuses listing with 405 (card ORPHAN-S3TABLES-1: loud refusal naming S3 Tables' own unreferenced-file removal). **S2-29 (2026-09-12):** Windows, macOS and arm support slated for 1.6 — cards PLATFORM-1…4 (the wheel matrix, the suite on every platform with Linux-only tiers, the five-wheel tag pipeline, Python 3.13 and 3.14 proven); measured a CI and packaging job, no platform code in the product.
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
- [overnight-report-2026-09-13-run12.md](overnight-report-2026-09-13-run12.md) — run 12 (2026-09-13, Devin actor + Grok S2-21 Rust/Python reviewers + Grok critic-logic, beside run 12b): FACADE-3 step 3 (Row/dict lists into native, abi3 temporal route; rows −54 %, dicts −55 %, tuples −47 % at 1e5; three P1 subclass/cache bugs found and fixed before the PR), the FACADE-4/5 briefs not opened, the remaining switchover measured in lines, owner questions Q-R12-1..3.
- [overnight-report-2026-09-13-run11.md](overnight-report-2026-09-13-run11.md) — run 11 (2026-09-13, Devin actor + Grok critic-logic + Grok S2-21 Python reviewer): EAGER-OWN-1 (#565), a refcounted handle owns every `__repark_cache_*` view (10 → 0 registrations, peak RSS 3,041 → 1,257 MB on the million-row TA loop; the slowdown did not reproduce on 125 GiB), Q-E1/Q-E2 recommendations, and card EAGER-BUDGET-1 seeded with the numbers.
- [overnight-report-2026-09-13-run10.md](overnight-report-2026-09-13-run10.md) — run 10 (2026-09-13, Grok actor + Grok critic-logic): COMMENT-CORE-1 (#561) removes the 347 non-pragma comments from `dataframe/core.py` with no code change (AST-identical, 4468 → 4117 lines, facade 5985/369 before and after); 312 reasons moved to the dataframe map, 35 narration lines deleted; the critic’s 11 findings (2 lost rationale, 9 distorted paraphrases) fixed before the PR.
- [overnight-report-2026-09-12-run9b.md](overnight-report-2026-09-12-run9b.md) — run 9b (2026-09-12/13, orchestrator B beside run 9, Devin actors + Grok S2-21 reviewers): CFG-2 named sources in two steps (#551 Rust seam, #556 Python door; `[<profile>.database]` loads, lists, refuses on use naming roadmap 1.10) and the DYNCFG-1 intake (#547: five measure-first cards, DQ-1..DQ-7 parked for the owner).
- [overnight-report-2026-09-11-run8.md](overnight-report-2026-09-11-run8.md) — run 8 (2026-09-11, Grok-only workers): NEVEROOM-1 step 3 and the v1.3.0 release PR, fork F-MINIO-QUAY and F-WRITE-COMPRESS-1, BALLISTA-M2-B, RP-16 with PERF-CATALOG-CACHE-WEIGHT-1 closed, AP-2, and the AP-1 re-measure (#507–#514 plus fork #276/#277); findings: Docker Hub dropped the MinIO images, `rewrite_data_files` writes uncompressed files (card F-WRITE-COMPRESS-2), owner question Q-1 on the byte-ratio model.
- [overnight-report-2026-09-12-run9.md](overnight-report-2026-09-12-run9.md) — run 9 (2026-09-12, the v1.5 track; Grok before 20:40, Devin after): FACADE-2 complete (#544, #549, #557), Silver S-0 and S-1 (#543, #546), PERF-UNPIVOT-1 and PERF-CAST-1 complete (#553, #550 → apache/datafusion#25248), FACADE-3 steps 1–2 merged (#555, #559); findings: nesting `F.abs` is exponential in native memory (P1, a global OOM kill), SIL-4 needs armed conflict validation, SIL-1..10 recommendations for the owner.
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
