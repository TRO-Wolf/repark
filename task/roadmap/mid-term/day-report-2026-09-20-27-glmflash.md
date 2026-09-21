# Run 27 report — xo-glmflash (UNIT 1 MAP-PR-GATE-1, UNIT 2 IPI-21/25/42 small parser)

End state at writing: #751 MERGED (edfa1e38); #752 OPEN, MERGEABLE, merge-in of main pushed
(1ffe3026), CI finishing, queued for drive-merge. All data below is final except the #752
merge confirmation, which is the only open item.

## Units
- UNIT 1 — MAP-PR-GATE-1: PR repark#752 (branch chore/map-pr-gate-1, lane xb-map).
- UNIT 2 — IPI-21/25/42 small parser (8 cells): PR repark#751 (branch
  fix/ipi-21-25-42-small-parser, lane re-build). MERGED as edfa1e38.

## PRs
- repark#751 — MERGED (merge commit edfa1e38, 13:35 EDT).
- repark#752 — OPEN, 1ffe3026 (merge-in of edfa1e38 auto-merged clean; ledger diff only #751's
  ipi-21-25-42 registry entry; comment gate CB=0). Queued bare-number 752 in run16/merge-queue.txt.

## Executor rounds per tier
- Devin (re-build / UNIT 2): initial rounds on inherited branch incl. resumed wip hand-back
  triage; rebase round onto 82952d40 (f22bcd31 → 36c39681).
- Devin (xb-map / UNIT 1): r1/r2 fix rounds (dd5c0d9c/6cec0c9d/f7240b47), r4 mutation-proof round
  (1c92bad7/92e39a6b/c52b07ee, --resume chipped-maxilla), merge-in clerk round
  (4b57682a ← 82952d40), merge-in of edfa1e38 done by orchestrator (1ffe3026).
- Muse clerk: none needed in this lane.
- Critic (Grok): re-build r1/r2 NEEDS_REMEDIATION → r3 PASS (12:09). xb-map r3/r4
  NEEDS_REMEDIATION (V-001 CI check-invocation substring hole, V-002 Makefile BASE/recipe
  substring hole, V-003 ledger collision prose) → remediation commits → r5 PASS (16:42Z) on
  c52b07ee. Merge-in commits 4b57682a/1ffe3026 = ruled dedupe (C-012 newest-wins) + pin
  carry-forwards + ledger prose + merges of main; orchestrator diff-read covered them.

## Gate / CI history
- Local gates: re-build final CB=0 R=0 T=0 U=0 L=0; xb-map final PYTEST=0 MAKE=0 (script tests +
  make check-map-md check-map-sync check-docs-links check-manifest), run via systemd-run in-lane.
- Root causes filed (claims): (1) drive-merge.sh takes BARE PR numbers (~40 min lost);
  (2) GitHub native merge ignores merge=union (#752 first no-CI round); (3) #733 squash union
  duplicates vs branch duplicate-row rule → dedupe-in-PR newest-wins (C-012); (4)
  COVERAGE_ATTESTATION yaml block required when all ledger rows non-OPEN; (5) CI ruff is
  0.15.22 tree-wide — local gates must use /tmp/opencode/ruffenv/bin/ruff (0.16.8 → 14 false fails).

## Questions needing a ruling
- Zero QUESTION lines written for my units. One lesson filed as a claims line 13:27 (drive-merge
  bare-number), not a design question.

## Cell replay (UNIT 2) — before/after
- Method: /tmp/oc-worker/nc-inventory `run-engine.sh repark smallparse-after cells_ddl.py` +
  compare.py (repark leg 13:35, Spark leg pre-recorded; EXIT=0).
- Before: 0 of 8 packet cells EQUAL.
- After: 8/8 packet cells EQUAL — D-DROP-TABLE-PURGE, D-REPLACE, D-RTAS, D-RTAS-TIME-TRAVEL,
  D-REF-CREATE-BRANCH-IF-NOT-EXISTS, D-REF-TAG-IF-NOT-EXISTS, D-REF-DROP-BRANCH-IF-EXISTS,
  D-REF-DROP-TAG-IF-EXISTS.
- 9th tracked cell TP-GC-DISABLED-PURGE: SPARK-CANNOT (both refuse) — Spark never answers it, so
  it is outside the v1.5.0 "Spark answers → EQUAL" gate; guard kept per run-26e overturn.
- No cell closed as DECLARED or "later". No numbered residues from UNIT 2 cells.

## Residues (numbered)
1. xb-map out-of-scope (docs/gate cells, no Spark cells; each a numbered residue, not a closed
   cell): Makefile:400 stale invariant; scripts/map.md:1295 stale invariant; briefs wording;
   pyproject.toml unnamed in a check; .github/workflows/map.md:83 typo.
- Door-level mutant residue count = 5 (same list).

## What I would do next
- After #752 merges: re-run compare.py once against main to confirm the 8 EQUAL verdicts survive
  the merge (compare reads RePark main; replay raced the merge by ~minutes).
- Sweep the 5 xb-map residues in one small clerk PR next run (they are docs/check text only).
- Keep the C-012 dedupe rule in mind for every future union-merge ledger PR.

## Merge confirmation (14:04 EDT, post-merge)
- repark#752 (MAP-PR-GATE-1) MERGED at 13:59:39 EDT, merge commit e26d1349.
- UNIT 2 small-parser work is on merged main as edfa1e38 "IPI-21/25/42 small parser shapes"
  (merged 13:35; squash merge, so branch tip 36c39681 is not an ancestor — substance verified by
  commit title/edge on origin/main).
- compare.py re-run post-merge (14:05): all 8 packet cells read EQUAL — D-DROP-TABLE-PURGE,
  D-REPLACE, D-RTAS, D-RTAS-TIME-TRAVEL, D-REF-CREATE-BRANCH-IF-NOT-EXISTS,
  D-REF-TAG-IF-NOT-EXISTS, D-REF-DROP-BRANCH-IF-EXISTS, D-REF-DROP-TAG-IF-EXISTS.
  No verdict shifted vs the tick-56 replay; no repark re-leg needed (#752 touched only map/docs
  lockstep files, no parser code).
- Run complete. STATUS: DONE.
