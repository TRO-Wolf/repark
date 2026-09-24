# report-xo-opus59 — IPI-40 VIEWS PR2..PR6 (run 28, lane B) — HAND-OFF (stopping point, 2026-09-23 18:27 owner claim)

## HAND-OFF (written tick 73, 18:30 EDT). main = fd43f192.

### Open PR: repark#815 PR3 `fix/ipi-40-views-3` (V-ALTER-UNSET, D-VIEW-ALTER-PROPS) — NOT queue-ready
- GitHub head 3aee91e7 (CI green 9 SUCCESS / 2 SKIPPED, behind main). Base main.
- Last critic: Sol r2 NEEDS_REMEDIATION at 3aee91e7 (/tmp/oc-worker/codex-worker/xr-xb-views3/20260923T213938Z/handback.json). Open findings:
  V-001 writes after USE unpinned (class NAME RESOLUTION IGNORES USE); V-002 complete_view_name empty-namespace arm unpinned (UNPINNED BRANCH);
  V-003 alter_view_routing.rs:336 to_string check + 5 python raises (ERROR IDENTITY).
- Local clone /tmp/xb-views3 at 7be1dff8 (42 commits on origin/main fd43f192, rebased; NOT pushed). Commits f1aa40dc..7be1dff8 are the r2a round:
  82c0b4fc V-001, 140130a7 V-002, 9f8ddf0f V-003, 7be1dff8 map.md; all author TRO-Wolf; comment gate 0 at 7be1dff8; tests only
  (alter_view_routing.rs +230, test_ice_views_1.py +38, two map.md). The `_ctx` production cleanup was reverted by the executor.
- ROUND STILL RUNNING at hand-off: Sol high r2a, unit codex-xb-views3-222047.service, round /tmp/oc-worker/codex-worker/xb-views3/20260923T222047Z/
  (WO /tmp/oc-worker/run28/ticks-xo-opus59/071/wo-pr3-r2a-fixes.md). At 18:26 it was running `cargo test -p repark-spark view` through the build slot.
  Hand-back will land in that directory as handback.json.
- Local gate state: last all-zero gate was for an earlier head; NO gate/lint for 7be1dff8.
- Rulings binding PR3: 071/RULING-r2-halt.md (INSERT BY NAME bare table = pre-existing insert_by_name.rs:69, pinned as-is; bare DELETE/UPDATE on
  src return Ok, measured); 050/RULING-pr3-halts.md; parity.md §2.6 row D-VIEW-ALTER-1 (E6/E7/E8 residues).
- EXACT NEXT STEP: (1) read r2a handback.json; check `git log --format=%ae origin/main..HEAD` all TRO-Wolf, trailers, comment gate, clean status,
  diff = tests + map.md only; sweep rows present with mutation runs. (2) Fill __HEAD__ in 071/wo-pr3-r2b-sweeps.draft.md and launch it (fresh round,
  three whole-PR class sweeps: USE-writes, unpinned arms, error identity). (3) After r2b: rebase if main moved, gate 053/pr3-post-gate.sh
  (rm 053/pr3post.done first), lint 053/lint-pr3.sh (rm 053/pr3lint.done; includes check_map_md), xpr force-push, body from
  068/pr3-body-r3.prefill.md (+ BY NAME residue), critic r3 from 068/critic-brief-pr3-r3.tmpl.md (angle J, fill __HEAD__ x2; cite r1+r2 classes).
  (4) PASS at current head + CI green -> `815 PR3 HH:MM` into /tmp/oc-worker/run16/merge-queue.txt, drive-merge.sh 815.
  (5) After merge: replay, bank counts, rm -rf /tmp/xb-views3 /tmp/xr-xb-views3.

### Clones
| path | branch | head | round running | notes |
|---|---|---|---|---|
| /tmp/xb-views3 | fix/ipi-40-views-3 | 7be1dff8 | YES (Sol r2a 20260923T222047Z) | PR3; don't gate/rebase until the round ends |
| /tmp/xr-xb-views3 | fix/ipi-40-views-3 | 3aee91e7 | no | critic clone; only untracked .venv; xreview.sh resets it |
| /tmp/xb-views4 | fix/ipi-40-views-4 | 056fd4ad | no | PR4 V-SHOW-TBLPROPERTIES, stacked on PR3's OLD head refs/xo/pr3c457 (=c4579af2); not opened |

### Unit list items not done
- PR4 V-SHOW-TBLPROPERTIES (branch built, Spark leg measured xo-opus3 064/MEASURED-showprops.md). Next after PR3 merge:
  `git -C /tmp/xb-views4 rebase --onto origin/main refs/xo/pr3c457`, then WO 066/wo-pr4-r2-use-short-names.draft.md (fill __BASE__; add class pins:
  every write entry after USE bare+2-part, every new arm incl. empty-namespace, errors by variant + full message). Gate views 1..4 + repark-spark +
  053 write-path set; lint incl. check_map_md; replay; body 044/pr4-body.prefill.md; critic 046/critic-brief-pr4.tmpl.md (+ angle I USE).
  DBT-TBLPROPS-1 keeps both facts if xo-opus61 PR A merged first (claims 14:55).
- PR5 V-SHOW-CREATE: not started. Opus WO after PR3 queued and #810 merged; reuse crate::show_create::{execute_show_create,
  render_tblproperties_clause, spark_sql_string_literal} + crate::table_props_view::spark_table_properties. Flip ledger C-017
  (task/ledgers/staging/ice-views-1-ledger.md:40) to PROVEN in the final views PR.
- PR6 D-TEMP-VIEW: not started. WO drafts 010/wo-pr6a (opus; __LANE__=xb-views6 filled, __BASE__ open) and 010/wo-pr6b (luna). Q1 RULED:
  retire DBT-TEMPVIEW-1, register DBT-INCREMENTAL-1, reword three dbt messages (claims 34).
- Every new WO carries claims 17:59 (xo-opus61): no test may pin TokenizerError "Unexpected EOF while in a multi-line comment" (after #810:
  UNCLOSED_BRACKETED_COMMENT/42601).

### Residue for product
- INSERT BY NAME ignores USE for table targets (insert_by_name.rs:69; 071 ruling).
- Native bridge cannot carry `_LEGACY_ERROR_TEMP_*` conditions (errors.py:40).
- Engine-wide ALTER TABLE missing-target text `TableNotFound => No such table: TableIdent {…}` (claims 15:42).

### Counts (final)
MERGED 1 (#812 PR2, d1a70b7d; V-DESCRIBE EQUAL, lane VIEWS 8->9 of 15 EQUAL; PR3 lane-head 11/15). OPEN 1 (#815). Critic rejections 4
(#812 r1, r2; #815 r1, r2 — three of them orchestrator class failures: error identity x2 + loose Spark answers). Gate failures 0; CI red 1
(map.md lint gap). Rounds: luna 2, sol 10 done + 1 running; critics Sol 6.

---
# History (report as of 15:38)

## Inherited (TAKEOVER xo-opus3 -> xo-opus59, 06:27)
- #767 (PR1) merged 00:34 as 88b6f59f. Its critic r10 (T025655Z, 65 turns, head 2dd90fc6) = NEEDS_REMEDIATION, one P2 V-009
  (Break on Expr::Struct|Named leaves the battery green; named-struct field form measured EQUAL on Spark and RePark). Carried into PR2.
- Clones adopted, skip-worktree empty: xb-views2 @247b2100, xb-views3 @ee1a921d, xb-views4 @e834a23f. xb-views/xr-xb-views already removed.

## Cells (scoreboard main 88b6f59f, /tmp/oc-worker/scoreboard/2026-09-23/matrix.json) — BEFORE for this lane
VIEWS 5 EQUAL / 3 REFUSED-REGISTERED / 1 NOT-PARSED / 1 SPARK-CANNOT (V-ALTER-AS). D-VIEW-*: 3 EQUAL, D-VIEW-ALTER-PROPS NOT-PARSED. D-TEMP-VIEW REFUSED-REGISTERED.
Open for this lane (6): V-DESCRIBE, V-SHOW-TBLPROPERTIES, V-ALTER-UNSET, D-VIEW-ALTER-PROPS, V-SHOW-CREATE, D-TEMP-VIEW.
#767 banked from the scoreboard (post-merge main): VIEWS 1 -> 5 EQUAL, D-VIEW-CREATE/-OR-REPLACE/-SHOW-DROP EQUAL.

## PRs
- PR2 V-DESCRIBE (xb-views2): luna round 06:30 = rebase (2 own commits onto 88b6f59f, drop 175cb3d8) + V-009 pin + whole-enum Break sweep.

## Executor rounds
- luna: 1 (xb-views2, 06:30).

## Critic verdicts
(none yet in run 28)

## Questions asked: 0
## Residues (inherited): R-V007-ANYALL R-V008-FEATURES R-PR3-E8 R-PR3-E9 R-PR4-ORDER R-PR4-E9

## 10:30 PR2 opened: repark#812 (head f2d5d7e1, tree 7c2ac980)
- Lint at 7c59243c all 0 (fmt, clippy, panic-ban, file-size, docs-links, ruff lint/format); local gate CB=R=T=U=L=0.
- xpr.sh refused: V-009 commit 29d3b35b had author `Codex <noreply@openai.com>` (luna round). Author reset by rebase --exec, tree identical;
  new SHAs 93614b61 (V-009), f2d5d7e1 (subscript pin). Lesson: after every Codex round, check `%ae` before the push.
- Critic (Sol) launched on xr-xb-views2 at f2d5d7e1.

## 11:45 update
- PR2 repark#812: critic r1 NEEDS_REMEDIATION (4 findings, class UNPINNED BRANCH) plus a real defect the orchestrator found (V-005: a catalog without views answered "does not support views" instead of TABLE_OR_VIEW_NOT_FOUND). Fixed in a Sol round with a mutation sweep over 10 branches (015/SWEEP-unpinned-branch.md). Rebased onto main 07a98452; local gate all 0 at fe92a848 (1799 Rust tests, 92 unit + 92 live pytest). Lint running; critic r2 next.
- PR3: restacked on the PR2 head; the same class is being swept before its first critic (Sol round, 021/wo-pr3-presweep.md).

- 11:47 #812 critic r2 NEEDS_REMEDIATION: V-006, error identity pinned by text only (same family as r1's unpinned branch; my r1 sweep WO
  asked for one mutation per branch but did not ask that each propagated error's identity be pinned). Test-only Sol follow-up
  (ticks-xo-opus59/024/wo-pr2-r2-error-identity.md). The same class is carried ahead into PR3 before its critic, and into the PR4/5/6 work orders.
- 12:42 #812 critic r3 (Sol) PASS at 08e36b9d on main 854ac2b6 (local gate all 0, lint all 0 at that head). Waiting for CI, then the merge queue.
  The ice-views-1 ledger row C-017 (still OPEN) stays open until the last views PR (PR5 SHOW CREATE), which must flip it to PROVEN.

## 13:02 PR2 repark#812 MERGED as d1a70b7d (squash on 854ac2b6; tree identical to the gated/critic-PASS head 08e36b9d)
- Replay on main (tree d1a70b7d via the PR2 clone at 08e36b9d, 15 lane cells, harness --only; ticks-xo-opus59/037/replay.log, out dir
  /tmp/xo-xo-opus59/replay-pr2): V-DESCRIBE REFUSED-REGISTERED -> EQUAL. Lane: 8 -> 9 EQUAL (VIEWS 5 -> 6 EQUAL); open: D-VIEW-ALTER-PROPS,
  V-ALTER-UNSET (NOT-PARSED, PR3), V-SHOW-TBLPROPERTIES (PR4), V-SHOW-CREATE (PR5), D-TEMP-VIEW (PR6); V-ALTER-AS SPARK-CANNOT.
- PR2 totals: critic Sol r1 NR (4 unpinned-branch) + orchestrator V-005 (real defect, fixed), r2 NR (V-006 error identity, same family —
  orchestrator failure), r3 PASS. Rounds: luna 2, sol 2 (1 salvaged from a read-only resume). Gates green 3, 1 stopped for a main move.
- Clones /tmp/xb-views2 and /tmp/xr-xb-views2 removed 13:04.

## 14:36 PR3 repark#815 OPENED at c8a7808e on main d1a70b7d
- Local gate all 0 (repark-spark 1823 Rust passed; views 1/2/3 unit 107, live 107); lint all 0 at c8a7808e; comment gate 0; authors TRO-Wolf only.
- Lane-head replay (041/replay.log): D-VIEW-ALTER-PROPS NOT-PARSED -> EQUAL, V-ALTER-UNSET NOT-PARSED -> EQUAL. Lane 9 -> 11 EQUAL of 15.
  Post-merge replay on main still to run.
- Pre-critic sweeps: UNPINNED BRANCH (52 one-arm mutations, 021/SWEEP-unpinned-branch-pr3.md) + ERROR IDENTITY (32 assertions,
  038/SWEEP-error-identity-pr3.md). Real defect fixed pre-critic: FeatureUnsupported arm in execute_alter_view (same as PR2 V-005).
- Critic Sol r1 launched 14:36 (brief 041/critic-brief-pr3.md).

## 2026-09-23 14:46 — PR3 #815 critic r1 NEEDS_REMEDIATION
- Critic Sol r1 (xr-xb-views3/20260923T183641Z, 32 tools) on c8a7808e: 7 findings, class SPARK ANSWER PINNED LOOSELY (substring
  messages, UNSET missing-target forms, SET overwrite, two-part RENAME target, cell schemas, E6/E7 unregistered). CI red: map.md lockstep
  (crates/repark-common/src/map.md not updated for spark_error.rs) — my lint script lacked check_map_md; added.
- Rulings from precedent: E6 origin tail omitted (PR2 DESCRIBE pin, parity.md:4778); E7 condition None because the native bridge's
  condition pattern (errors.py:40) cannot carry `_LEGACY_ERROR_TEMP_1123` — product residue (bridge change, not views scope);
  E9/two-part = session default catalog `datafusion`. All three → new DECLARED row D-VIEW-ALTER-1 (§2.6).
- Class carried to PR4 before its critic (Sol pre-sweep, row draft D-VIEW-SHOWPROPS-1).

## 15:48 orchestrator findings while PR3 r1 remediation / PR4 pre-critic rounds finish
- Production defect found by measurement (not by a critic): view DDL name completion ignores USE (reads DataFusion's default catalog,
  USE writes the CatalogRegistry defaults). After `USE sc.ns`: ALTER VIEW v -> "unknown catalog `datafusion`", SHOW VIEWS IN ns same,
  SHOW TBLPROPERTIES v falls through to DataFusion. Spark answers P-SP-BARE-NAME/-TWO-PART = [["k","v"]]. Fix (precedent
  use_ddl::complete_name) also turns E9 / RENAME TO ns.w into EQUAL (to=spark_catalog). Evidence ticks-xo-opus59/050/MEASURED-use-resolution.md.
  Goes into #815 as round r1b (WO 050/wo-pr3-r1b-use-and-halts.md).
- PR3 sweep HALT rows ruled from measurement (050/RULING-pr3-halts.md): ALTER TABLE on a view name and SELECT of a renamed view's old name
  answer the engine-wide missing-target / missing-relation texts -> DECLARED in D-VIEW-ALTER-1, pinned. Engine-wide ALTER TABLE
  `TableNotFound => No such table: TableIdent {…}` text posted to claims as an unowned residue.
