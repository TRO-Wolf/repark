# run27 report — IPI-29 Iceberg system functions (orchestrator xo-muse)

## Units and PRs

| Unit | Cells | PR | Result |
|---|---|---|---|
| IPI-29 Iceberg system functions (RePark-only, no pin bump) | 15 FUNCTIONS | repark#756 | MERGED TREE-EQUAL ed15699b 2026-09-20 19:39:20Z |

Branch `fix/ipi-29-system-functions`, lane `/tmp/xb-sysfn`. One unit = one PR.
Final head 208bb783; merge tree == branch tree (1dd6411d). Claimed 09:05 in
run27/claims.txt. Five rebases during the day (#753, #733, #751, #752, #757),
all conflict-free (12/12, 12/12, 16/16, 16/16, 17/17, zero conflicts).

## Before/after cell counts

- Before: 0/15 EQUAL (REFUSED-UNREGISTERED, no registry row).
- After (worker leg, out/repark-sysfn-after.json): 15/15 EQUAL.
- After post-merge (orchestrator replay, out/repark-sysfn-merged.json +
  matrix.json re-read this tick): 15/15 EQUAL, non_equal [].
- Residues: ZERO. No DECLARED closes, no numbered residue. V-* cells untouched
  per packet ruling H-08 (IPI-40 owns them).

Replay evidence: run27/ticks-xo-muse/055/replay.done (`n=15, EQUAL=15`),
055/replay.log (HARNESS=0, COMPARE=0, DONE=0), matrix.json FUNCTIONS rows
re-read directly (tick 56). compare.py `load()` overwrites in sorted-glob
order and repark-sysfn-merged.json sorts last, so the merged leg wins ties
(verified in source). The native build postdates the last content commit
(_native.abi3.so 15:01:09 > 208bb783 14:53:35); the MATURIN=1 line in the
replay log is a wrapper artifact (maturin invoked at the uv-workspace root),
not a stale build.

## Rounds per executor tier

| Tier | Rounds | Work | Result |
|---|---|---|---|
| muse (max) | 3, --resume chain on session 01a0bef2-… (runs.tsv turns 127/78/136) | WO-1 bucket/truncate, WO-2 temporal/version, WO-3 registration/SHOW | all exit 0 CONCLUDED, zero questions |
| Devin (free) | 1, stamp 20260920T165826Z | WO-4 critic-r1 remediation (V-001/V-002/V-003) | exit 0 CONCLUDED, 1 ruling question (Q1) |
| muse-clerk | 1, stamp 20260920T182209Z, 17 turns | WO-5 clippy too_many_lines clerk fix | exit 0 CONCLUDED, zero questions |
| Failed launches | 3 (tick 3 exit 209/STDOUT, tick 22 + tick 36 slot-blocked foregrounds) | — | no worker cost; all replaced by detached units |

Total: 5 executor rounds, all CONCLUDED, all handbacks accepted. 17 branch
commits (13 Muse Spark + 4 Devin SWE-2), all TRO-Wolf identity +
Authored-By trailers, zero co-author trailers. Comment gate hits=0 at every
check (handback, rebase, push, queue).

## Critic verdicts (Grok, mandatory, via xreview)

| Round | Scope | Verdict | Turns/cost |
|---|---|---|---|
| r1 20260920T155210Z | 1e5b2854 | NEEDS_REMEDIATION: V-001 [P1] SHOW intercept Some(Err) on non-exact IN; V-002 [P2] unbound sorted pins; V-003 [P2] case-sensitive catalog match | 37 turns, $0.97 |
| r2 20260920T175817Z | 8bfc072f | PASS (V-001/V-002/V-003 closed and load-bearing, Q1 binding independently reproduced, J-1 holds) | 28 turns, $0.70, zero questions |
| r3 20260920T190724Z | 208bb783 (final head) | PASS (WO-5 extraction behavior-preserving, V-findings re-verified with red mutations) | 24 turns, $0.55, zero questions |

Critic rejections: 1 (r1, remediated by WO-4 on Devin). r2 PASS was kept as
V-logic evidence only and NOT carried over the WO-5 content commit — r3 ran
on the final head per discipline. All verdicts dud-checked (turns>1, real
gates with red mutations, file:line premises) before acting.

## Questions asked

- Claims QUESTIONS filed: none.
- Worker question Q1 (bucket binding transpose 1->4, -7->5, 'abcdef'->5,
  'z'->7): settled by the orchestrator from measurement (sorted-set cell
  dump + fork bucket.rs formula at pin 3ed905c6 + independent pure-Python
  murmur3 reproducing 4/5/5/7) — worker's lean adopted, no claims QUESTION.
- V-003 fence (case-sensitive catalog match): orchestrator ruling (b) declare
  the fence, docs-only — settled from sibling-convention measurement
  (CatalogRegistry::get exact everywhere; router.rs meta_delete/describe_table
  do no folding), no claims QUESTION; critic r2/r3 accepted it.
- Late-critic findings H-01..H-10 (rc/rv29-findings.json, packet section 11):
  folded into work orders BEFORE the first launch; all held.

## Gate failures

- 1 CI red, tick 46: Rust lint `clippy::too_many_lines` on execute_inner
  (102/100 — the branch's +4-line SHOW preamble tipped main's 98 over).
  Real, on the final head. Fixed by WO-5 (preamble extracted to
  `rewrite_sql_for_execute`, no allow). Process failure was mine: tick-45
  xgate R-code is not `make rust-clippy`; lesson banked and enforced
  thereafter (Makefile targets at the final rebased head before every push).
- 2 failed xpr.sh pushes, tick 45: explicit-URL + push-disabled origin breaks
  implicit --force-with-lease; fixed with explicit lease
  (1e5b2854..8bfc072f, exit 0). Lesson banked; reused cleanly at ticks 49/52.
- Final-head evidence at queue time: xgate CB=R=T=U=L=0 + CLIPPY=0 +
  PANICBAN=0 (banked tick 52, citable) + CI 10 success + 2 skipping on
  208bb783 (all 12 heads verified) + r3 PASS + comment gate 0.

## Cost

- Muse: 4 rounds, 358 turns total per runs.tsv (127+78+136+17); Devin: 1
  round, free tier; clerk: included in the muse count (WO-5, 17 turns).
- Critic (Grok): 3 rounds, 89 turns, ~$2.22 ($0.97+$0.70+$0.55 as logged).
- CI cycles: 2 full runs on branch heads (run 35527439531 with the 1 known
  lint red; final run all green incl. 26m46s smoke) plus the pre-rebase
  green run. No wasted worker rounds: every launch concluded and shipped.

## What you would do next

1. Nothing remains on IPI-29: merged, replayed 15/15, zero residues.
2. Fix the replay wrapper's maturin probe: run it in the member package
   dir (or drop it — the .so mtime check already proves freshness). The
   MATURIN=1 wrapper artifact invites misreading.
3. Document that xgate's R-code is not `make rust-clippy` (it missed a real
   CI lint red twice-adjacent); either align xgate's clippy flags with CI
   or keep the enforced habit of Makefile targets at the final head.
4. Watch RP-42 (xo-opus, still unmerged at DONE): it rebases onto ed15699b
   and already composes with this unit's `rewrite_sql_for_execute` helper;
   no action owed from this lane.
