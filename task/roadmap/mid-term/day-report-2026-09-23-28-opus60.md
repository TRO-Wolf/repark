# report-xo-opus60 — lane C (orphans #807, U1 MEM-LAYOUT, U2 evidence) — run 28, 2026-09-23 — HAND-OFF at the stopping point (tick 96, 18:30 EDT)

## HAND-OFF (stopping point, owner via orchestrating session, claims 2026-09-23 18:27)

### Open PR — iceberg-rust#347 (Unit 2 PR-B slice 1: opt-in Hadoop metadata naming on the fork's MemoryCatalog)
- Head 5c9120195ab49f5d2933e1a4e1efe5ac9ff3d65b on branch `feat/memory-catalog-hadoop-naming`; base fork main 962f130cf (not behind; MERGEABLE).
- Critic: r1 (20260923T220024Z) VOID (sandbox mount). r2 was never launched (xreview.sh without XREPO=iceberg-rust). r2b (Sol, brief /tmp/xo-xo-opus60/wo/critic-prb-fork-r2.md, round /tmp/oc-worker/codex-worker/xr-xo60-hmeta/20260923T220740Z/, unit sol-xr-xo60-hmeta-220740) STILL RUNNING at 18:30 on head 5c9120195 — no verdict yet, no open findings on record.
- Local fork gate: all 0 at t91 (/tmp/oc-worker/xo60-hmeta-forkgate.done). Comment gate 0, skip-worktree empty, trailers `Authored-By: Claude (claude-opus-5-5)`, identity TRO-Wolf. catalog.rs 3145 lines (ceiling lowered 3384->3145), metadata_location.rs 987; no Cargo edits.
- CI at 18:30: 13/14 pass, `build (windows-latest)` pending, 0 failed.
- EXACT NEXT STEP: read the newest handback.json under /tmp/oc-worker/codex-worker/xr-xo60-hmeta/ with stamp after 20260923T220740Z; check its runs.tsv row TOOLS (8th col) >= 3 (else VOID -> refire with `XREPO=iceberg-rust xreview.sh xo60-hmeta <evidence-first brief>`; Grok fallback `XREPO=iceberg-rust XCRITIC=grok` only on a provider error).
  PASS + windows green -> append `fork#347 U1-hmeta HH:MM` to /tmp/oc-worker/run16/merge-queue.txt and run under systemd-run `/tmp/oc-worker/_lib/la-fork-merge.sh 347 "feat(memory): opt-in Hadoop metadata naming on MemoryCatalog (#347)" /tmp/xo60-hmeta` (result /tmp/oc-worker/fork-merge-347.done).
  NEEDS_REMEDIATION -> name the class, one opus WO (fresh round, lane xo60-hmeta) fixing exactly the findings + class sweep, regate with the fork gate, push via xpr.sh (XREPO=iceberg-rust, under systemd-run), fresh critic with XREPO=iceberg-rust.
- Known slice-1 residues (ledger §4 in the PR): staged create still uuid-named (slice 2); publish_replace_table writes no version hint; drop_table deletes only the current metadata file, so drop+recreate of the same name in hadoop mode collides at v2 (Java deletes the table dir) — measured t90: harness hc cells use fresh names, so not a PR-B blocker.

### Clones
- /tmp/xo60-hmeta — fork lane, branch feat/memory-catalog-hadoop-naming, head 5c9120195, clean; no executor round running.
- /tmp/xr-xo60-hmeta — critic clone at the same head; critic r2b (Sol) STILL RUNNING in it.
- Remove both after #347 merges (and after PR-B's replay is banked if the lane is reused).
- All other lane clones of this unit list are already removed (xo55-orph2, xr-xo55-orph2, xo60-layout, xr-xo60-layout).

### Unit-list items not started / not finished
- Unit 2 PR-B slice 2 (fork): staged create (StagedTableTransaction::begin_create; RePark CTAS ctas.rs:242, create_table.rs:190) in hadoop mode -> v1.metadata.json. begin_create has no catalog handle; publish_create_table defaults to register_table. Needs a design WO (opus). Java CTAS writing one v1 file is UNMEASURED — measure first.
- Unit 2 PR-B RePark side (after #347 merges): fork pin bump (drags fork 9712a2ec1 + 962f130cf; write a claims line before the bump) + `type=hadoop` passes `metadata-naming=hadoop` (crates catalog_kind.rs:22). Replay after: P-RTP-DEFAULT, P-RTP-*, P-ORPHAN-* (run on hc, cells_proc.py:295 — listings should show vN.metadata.json + version-hint.text), C-CODEC-*, D-NS-DROP-*-HADOOP, and xo-opus58's R-DF-LOAD-PATH / R-DF-LOAD-METADATA-JSON.
- R5 follow-up (not started): qualify scheme-less local paths as `file:<path>` in the orphan output — closes the 8 P-ORPHAN listing cells.
- R6 (not started): P-RTP-DEFAULT staging dir id shape (uuid vs 16hex-6hex) — harness normalise or RePark uuid; question for the owner if the harness route.
- Owner has NOT overturned Q-55-7 (claims through line 167).


## Unit 1 — #807 (lane xo55-orph2) — MERGED 2026-09-23 15:19Z (07a98452); clones removed
Rounds: orph2-r3fix (opus, 06:35-07:2x) CONCLUDED: V-001..V-006 closed, each pin mutation-proven; class sweep pinned every file_list_view branch; one code change (view gc refusals through iceberg_err, equal to the listing path). Rebased on f411192, pushed 35cebb34 07:31. Local gate + critic r2 (Sol) started 07:32. Replay of 10 P-ORPHAN-*: pending (after gate).
Critic r2 (Sol, 35cebb34, 53 tools, valid) 07:36: NEEDS_REMEDIATION. V-007 P1: armed file_list_view deletes a live file named through an interior `../` (scope check normalised, reference match raw) — ruled normalise both sides (Java Path.toUri().normalize()). V-008 P2: refusal pins substring-only, variant dropped. Classes: path-form asymmetry; loose error pins. Round orph2-r4fix (opus resume of 7c14caff) launched 07:37. Gate on 35cebb34 stopped (stale).

## Unit 2 — U1 MEM-LAYOUT PR-A (lane xo60-layout, Q-55-7) — repark#814 MERGED 2026-09-23 17:12 EDT (main 2de327b, drive-merge TREE-EQUAL); critic r8 PASS at a748abf6, gate 4 all 0, CI green; xr clone removed 17:15. Main replay (main 2de327b, ALLDONE 17:23, build/core/misc/proc rc 0; log wo/replay-main-814.log): 24 cells FULL-LINE IDENTICAL to the lane-head replay (wo/replay-layout-after.txt) — EQUAL 8, DIFFERENT 11 (8 P-ORPHAN listing on `file:` only = R5, 3 P-RTP-* on metadata file name + R6), SPARK-CANNOT 1, NOT-MEASURED 1, REFUSED-UNREGISTERED 2, REFUSED-REGISTERED 1. Before (21-cell inventory) -> after main: EQUAL 1 -> 8. Lane clone /tmp/xo60-layout removed 17:37. PR-B (hadoop-style metadata naming): fork slice 1 opus round running since 17:35 (lane xo60-hmeta).
- Critic history: r1 NEEDS_REMEDIATION (3) at 628bd3eb (V-001 P1 sub-path of a shared root bypassed the probe); r2 NEEDS_REMEDIATION (5) at 4ed57256 (V-001 P1 scan inside another catalog's table outside own; V-002 P1 uniqueness assert lost; V-003..V-005 P2); opus layout-r8 closed all five: ancestor `metadata/` probe (`metadata_probe_ancestors`), mutations M-anc/M-stop/M-whole-anc/M-ns/M-cat all RED (wo/mutr8/results.md), plus one more cell the class sweep found (a same-catalog scan inside a table nested in own).
- ORCHESTRATOR FAILURE: class A repeated (r1 V-001 -> r2 V-001). The r7 work order narrowed the critic's lean to `<own>/metadata` only. Lesson: when narrowing a critic's lean, enumerate the full path-relation x catalog table before ruling.
- Ruling layout-r8 Q1: the ancestor walk for a scan outside own goes to the storage root (fail-safe; stopping at the warehouse would reopen the class for scans inside another warehouse's table).
- Out of scope, noted: a data/ scan spelled file:///<p> or file:/<p> lists nothing (under-delete, safe; R5/R3 scheme residue); clippy --tests fails on pre-existing unwraps in crates/repark-spark/tests/session_extension.rs (not a repo gate).
Status 13:46: PR-A = layout + co-tenancy guard (bodies wo/prA-body-live.md). Mutations 11/11 RED + clippy/panic-ban 0 (wo/mutlay/results.md, at d1f8fa9b, code-identical). Gate 1 at 628bd3eb: CB=0 R=0 T=0 U=0 L=1 — L=1 is a pre-existing teardown error in test_ice_procs_route_1.py (culprit _record_ice_procs_route_1_oracle.py:739 session.stop(); claims NOTE 13:40); gate 2 without that file queued 13:45.
Replay after (wo/replay-layout-results.md), 23 cells: EQUAL 1 -> 8 (P-ORPHAN-YOUNG, P-ORPHAN-FILE-LIST-VIEW, P-TABLE-STATS-* 3, P-PART-STATS-* 2, R-MT-ENTRIES-SCHEMA kept); REFUSED-REGISTERED 11 -> 1. Eight P-ORPHAN listing cells go REFUSED -> DIFFERENT on the `file:` scheme prefix alone (R5); P-RTP-* 3 path now = Spark, metadata file name left for PR-B (+ staging id shape in -DEFAULT). SHOW CREATE 3 unchanged (xo-opus61's).
Before-inventory (21 cells): /tmp/xo-xo-opus60/wo/u1-inventory.md. Rounds: layout-r1 (opus, 06:48-07:2x) CONCLUDED: cbb494b4 (CatalogRegistry warehouse_layout_root; resolver branch after ns location, before policy; 9 mem_layout pins + 2 session pins, all mutation-proven) + a87ae65b (parity row ICE-CATALOG-MEM-LAYOUT-1, guide, ledger). layout-r2 (luna, python path expectations) queued 07:34. Still to do: co-tenancy guard (after #807 merges), map.md lockstep + ledger pin citations (last commit), rebase, replay. PR-B (v<N>.metadata.json) not started.

## Unit 3 — U2 evidence — DONE 06:41 (no code); harness rules implemented 06:49 on the owner's ruling
Proposal: /tmp/oc-worker/run28/u2-overrides-proposal.md; prototype /tmp/xo-xo-opus60/wo/u2-proto.py (5 EQUAL now; 5 stats cells EQUAL once U1 lands).
Claims: `QUESTION xo-opus60 U2 (OWNER)` 06:41. Lean: per-key `normalise` in overrides.json, not whole-cell verdicts.
- Owner ruling U2 (07:0x): APPROVED as proposed; implemented 06:49 in the scoreboard copy `/tmp/oc-worker/scoreboard/2026-09-23/`
  (`compare.py` gains `normalise()` applied to both legs; `overrides.json` gains 10 `normalise` entries, no verdicts; `.bak-2026-09-23` kept).
- Before/after on the 88b6f59f legs: EQUAL 616 -> 621, DIFFERENT 38 -> 33, all other classes unchanged.
  EQUAL now: P-ANCESTORS-OF, P-ANCESTORS-OF-ID, P-POS-ANCESTORS, P-CHERRYPICK-WAP, P-PUBLISH-CHANGES.
  P-TABLE-STATS-{DEFAULT,SNAPSHOT,COLUMNS}, P-PART-STATS-{DEFAULT,SNAPSHOT}: DIFFERENT on the table root only, closed by U1.
- Near-misses that still differ: a staged id that is not the published one; out-of-order commit times; a stats file outside `metadata/`.
- stage.sh copies compare.py/overrides.json forward from the previous day, so the next staging carries the patch.
- Parity-doc rows citing the ruling: in U1 PR-A's docs commit.

## Residues
R5. (new, measured 13:44) Java's remove_orphan_files lists local files as Hadoop-qualified `file:<path>`; RePark prints the path as the table location spells it (`<path>`). Eight P-ORPHAN listing cells differ on this alone after PR-A. Candidate: qualify scheme-less local paths with `file:` in the orphan output (small follow-up PR; would close 8 cells) — not started, not in PR-A.
R6. (measured 13:44) P-RTP-DEFAULT staging dir id: Java `copy-table-staging-<uuid>`, RePark `copy-table-staging-<16hex>-<6hex>`. After PR-B this cell still differs on it unless the harness normalises it or RePark uses a uuid.
- (#807, stated in ORPHAN-3) A CALL `location` strictly inside another table's directory is swept: none of Q-55-6's three cases. Candidate close in the U1 co-tenancy guard WO.
- (U1, in progress) Co-tenancy hazard measured by layout-r1: two memory catalogs on one warehouse share <wh>/ns/t; orphan sweep of one would delete the other's live files. Ruled: co-tenancy guard in U1 PR-A (wo/u1-cotenancy-guard.md).
R1. P-RDF-PARTIAL-PROGRESS (not this lane's): md.snapshots/layout.snapshots [3] and [4] are swapped. Spark commits the 2-record group
(deleted 2 files) then the 6-record group (deleted 3); RePark commits 6 then 2. Totals equal (8 records, 2 files at the end).
The predecessor's probe55k (Spark 4.1.2) found Spark's order under max-concurrent-file-group-rewrites=5 is completion order (a race), and
RePark == Spark on every max-concurrent=1 cell. Proposal on record: compare the commits as a multiset or re-record.

- 08:05 (tick 18) U1 layout-r3 (luna, fresh) CONCLUDED: 7 commits 4ab098d8..cbd8c2a7 (six python test files moved to <wh>/<ns>/<t>, check-disk-headroom SKILL.md line); 77 passed / 22 skipped on the seven files; diff read, no assertion loosened; comment gate 0. Full discovery gate (repark-core, repark-spark, python/repark/tests) queued 08:04 on head cbd8c2a7.

### Tick 28 (09:32)
- #807 local gate on 4a7b3365 all zero (CB=0 R=0 T=0 U=0 L=0). Mutation proof now running, then critic r4.
- xo60-layout discovery gate: Rust 0; Python unit tests: only the expected failure, `test_remove_orphan_files_refuses_the_shared_ctas_fallback_root`. The live phase was aborted at 80%. From 56% on, every test hit a teardown error because a test had stopped the shared SparkContext. That is an older problem (the same cascade appears in the 09-20 re-build2 and rc-incr logs). The live phase is unverified past 56%. The final gate for this lane will use named files only.

## Tick 30 (10:01) — #807 evidence banked at head 4a7b3365
- Local gate ALL ZERO (09:30). CI green (SUCCESS 9, SKIPPED 2).
- Mutation proof (orchestrator-run, /tmp/xo-xo-opus60/wo/mut807/results.md): baseline 42 passed; mutations i ii iii iv iv-b v vi vii viii ix ALL RED by failing tests (rc 101, no compile-only reds).
- P-ORPHAN replay at 4a7b3365 (/tmp/xo-xo-opus60/wo/replay807-matrix.json): before 10 REFUSED-REGISTERED + OLDER-THAN SPARK-CANNOT → after YOUNG EQUAL, OLDER-THAN SPARK-CANNOT (both refuse, 24 h floor), 9 DIFFERENT path-only (`repark_ctas/hc/` segment; `file:` scheme on Spark's side for 8 of them; FILE-LIST-VIEW differs in the segment only) — closure of those 9 is U1 MEM-LAYOUT.
- Critic r4 (Sol) fired 10:01 on 4a7b3365 with the mutation evidence.

- 10:07 (tick 31) #807 critic r4 (Sol, 36 tools, valid) on 4a7b3365: NEEDS_REMEDIATION, 4 prose findings V-009..V-012, no code finding (V-007/V-008 confirmed closed, mutation record checked). Class: prose scope claim ("only"/"exactly"/"every") broader than the code — the listing-path refusal of an unnormalised stored location was left out of the "three cases". PR body draft fixed by the orchestrator (V-009, V-011, V-012); ORPHAN-3 + src/map.md + class sweep on a docs-only luna round (wo/orph2-r6docs-luna.md). Then critic r5.
- 10:19 (tick 33) luna orph2-r6docs CONCLUDED: ee785717 docs only (crates/repark-spark/src/map.md, docs/spark-sql-iceberg-parity.md ORPHAN-3; no .rs/.py), comment gate 0, ledger/docs checks 0. V-010 closed in ORPHAN-3 + src/map.md (listing-path refusal named; "inside another table" non-refusal stated). Pushed via xpr.sh (remote head ee785717). Critic r5 (Sol) fired 10:19 on ee785717.
- 10:23 (tick 34) #807 critic r5 (Sol, 41 tools, valid) on ee785717: NEEDS_REMEDIATION on the LIVE PR body only (V-009, V-011 still in the old body; the draft closes both). Source review clean: V-007, V-008, V-010, V-012 and the listing-path issue CLOSED, all ten mutations RED by behaviour-targeted pins. Live body replaced with wo/pr807-body-next.md (gh pr edit, verified byte-identical); critic r6 (Sol, body-focused brief wo/critic-807-r6-filled.md) fired 10:23.
- 10:31 (tick 36) critic r6 (Sol) on ee785717: NEEDS_REMEDIATION V-013 (prose, same class as V-010/V-011). Live body fixed; luna orph2-r7docs (fresh round) sweeps the quantifier family (every/each/all/only/exactly/always) over all added .md lines. Lesson: a class sweep greps the whole quantifier family, not the single word the finding used.
- 10:47 (tick 38) luna r7 docs CONCLUDED; amended to 805c499e (author reset from noreply@openai.com; docs only since 4a7b3365, so source evidence holds). Pushed; critic r7 fired.
- 10:49 (tick 39) critic r7 (Sol, 23 tools, valid) on 805c499e: PASS, V-013 closed, no findings.
- 11:18 (tick 43) CI green at 805c499e (9 pass, 2 skipped), comment gate 0, local gate all zero -> #807 QUEUED (merge-queue 11:18, drive-merge unit merge-807-111853).

### Tick 44 (11:24) — #807 MERGED
- repark#807 merged 2026-09-23 15:19Z as 07a98452 (critic r7 Sol PASS at 805c499e, CI green, local gate all zero, comment gate 0). Replay banked before merge (wo/replay807-matrix.json: P-ORPHAN-YOUNG EQUAL, OLDER-THAN both refuse, 9 DIFFERENT path-only — closed by U1 layout).
- Disk: /tmp/xo55-orph2 and /tmp/xr-xo55-orph2 removed.
- Unit 2: xo60-layout rebased clean onto main (head 3029185c); opus layout-r4 (co-tenancy guard, Q-55-6 python test moved to <wh>/ns/events) launched 11:24.

### PR-A #814 critic r4 (15:39, on 38fcaef3) — ORCHESTRATOR FAILURE noted
NEEDS_REMEDIATION, 5 × P2 prose. V-001..V-003 repeat r3's class (prose scope claim broader than the code: the TempFallbackAllowed gate omitted in src/map.md, call/map.md, ORPHAN-4 heading). My r3 sweep covered the docs diff but not every map.md line and not headings: a second finding of the same class is the orchestrator's failure. V-004/V-005 a second class (evidence claim broader than the evidence: "code-identical" mutation tree; "every pin ... one mutation at a time"). Fixed myself in 30b3fe66 (prose only) + live body; critic r5 brief sweeps both classes over every added .md line and the body.

### PR-A #814 critic r5 (15:47, on 30b3fe66) — third prose round, same two classes
NEEDS_REMEDIATION, 8 × P2 prose, no code finding (Sol, 72 tools, valid). Class A (walk descriptions omitted the equal-location no-walk case, V-001..V-004; sweepability exception list omitted the metadata refusal, V-005; "every added prose line", V-008). Class B (M-over 6 vs 8 logged, V-006; C-028 cited a mutation that only the worker ran, V-007). Orchestrator sweep beyond the findings: every mutation count in the ledger checked against mutlay/mutr7/mutr8 logs (C-017 31 -> 32; C-026 names all 7 other reds); the preamble now separates logged runs from worker-reported M1..M9; src/map.md:3 restated against rules (a)-(c); "every refusal pin asserts" and "always refused" narrowed. Fixed myself in b35d24a9 (prose only) + live body. Lane-head replay at 30b3fe66 (native module from gate 4): P-ORPHAN-YOUNG + FILE-LIST-VIEW EQUAL; 8 listing cells DIFFERENT, `file:` prefix only (checked field by field); stats 5/5 EQUAL; RTP 3 DIFFERENT on the metadata file name only (PR-B) plus the staging id shape (R6); same verdicts as the 628bd3eb replay.

- 15:57 (tick 75): critic r6 (Sol, 38 tools, on b35d24a9) NEEDS_REMEDIATION, 3 x P2 prose: call/map.md:3 and :184 omitted the own-location exception of the co-tenancy refusal (class A, fourth round); ledger AT-7 overstated the probe's listing cost (class B). Fixed by the orchestrator in af5537b1 (.md only) plus the live body rule (b) (equals-or-contains case, found by the sweep). Pushed; critic r7 chained (brief wo/critic-prA-r7.md).

- 16:05 (tick 77): #814 critic r7 (Sol, 56 tools, af5537b1) NEEDS_REMEDIATION, 13 x P2 prose, 0 code. Class C: universal claims ("every ancestor / every metadata file is read", "is swept", "refuses only") that left out the early exit at the first refusal and the other guards. Fixed by the orchestrator in a748abf6 (.md only) plus the live body; the class sweep also scoped two more sentences (ledger C-011 "Only metadata/ ...", body "still sweep" → pinned layouts). Applying the orchestrating session's #805 ruling (b) by analogy: no re-gate, ONE scoped critic r8 (r7 closure + code/tests); a PASS, or only new prose findings, goes to the queue and any new prose becomes a numbered residue.

### PR-A repark#814 — critic r8 (tick 78, 16:12)
- Sol r8 (20260923T200537Z, 45 tools, valid) on a748abf6: **PASS**. All 13 r7 prose findings CLOSED; code/test diff unchanged since a94af4d2; no new finding.
- Comment gate 0 at a748abf6. Gate 4 all 0 (a94af4d2, last code change). CI on a748abf6 pending (3 of 8 required green at 16:10).
- Main moved to 0002a7f2 (IPI-23); overlap with PR-A in session.rs is disjoint hunks (import + read_sql vs memory-layout registry); GitHub MERGEABLE; r9-merge.sh updates the branch and re-waits for checks.
