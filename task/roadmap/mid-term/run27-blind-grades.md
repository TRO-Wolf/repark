| Lane | engine | work order precision | plan correctness | critic handling | report honesty | state sufficiency | total /20 |
|---|---|---|---|---|---|---|---|
| A | xo-glmflash | 4 | 3 | 4 | 3 | 3 | **17** (devin) |
| A | xo-glmflash | 4 | 4 | 4 | 4 | 2 | **18** (terra) |
| B | xo-grok | 4 | 4 | 4 | 0 | 4 | **16** (devin) |
| B | xo-grok | 4 | 3 | 3 | 0 | 3 | **13** (terra) |
| C | xo-muse | 4 | 4 | 4 | 4 | 4 | **20** (devin) |
| C | xo-muse | 4 | 4 | 4 | 4 | 4 | **20** (terra) |
| D | xo-glm | 3 | 3 | 4 | 0 | 4 | **14** (devin) |
| D | xo-glm | 2 | 3 | 4 | 0 | 4 | **13** (terra) |

Disagreements of 2+ points: none

[devin] Lane-A (xo-glmflash) worst: Trail gaps on the re-build lane: executor rounds ran with no preserved work orders and the one surviving order (WO-10) contradicts the shipped gc.enabled behavior, leaving the central design flip unexplained.
   best: Remediation orders mandate executor-run mutation proofs ('mutate → red → restore → green') and give the exact assert text, so each critic round closed the hole class rather than the named instance.

[devin] Lane-B (xo-grok) worst: Repeated CI-red round-trips through a gap it had itself diagnosed — WO-09 admits 'Local gate does not run clippy (that is why the PR went up red)', yet unused-import, clippy, ledger-citation and ruff failures each cost a clerk round and a CI cycle (4 CI fixes before merge).
   best: Pre-measured git forensics in the orders: merge-base/merge-tree predictions with the literal conflict text and the exact resolution spelled out before the executor ever runs.

[devin] Lane-C (xo-muse) worst: Violated its own banked lesson: after recording at tick 32 that bare clippy isn't `make rust-clippy` and gates must run at the final head, it pushed at tick 45 without them, producing the real CI lint red that cost WO-5 and a rebase cycle — and the claims-tail audit bloats the state file with hundre
   best: Verdict hygiene: every critic verdict dud-checked (turns>1, real gates, file:line premises) before acting, and no PASS was ever reused across a content or rebase commit without a fresh round on the final head.

[devin] Lane-D (xo-glm) worst: Front-loading risk: one mega-order for the whole unit (two step-cap overflows) that also landed the invasive planner-default flip without any local smoke-equivalent, so the 27-test regression surfaced only in CI and consumed the rest of the run in remediation.
   best: Forensic root-causing — the tick-96 state entry clusters all 27 smoke failures into five measured causes (A–E) with per-cluster counts, and the r6 order maps each cluster to an exact fix with repros that must flip green.

## Re-grade of Lane-B (xo-grok), 2026-09-21 — the first grading used a false 'report not written' stub (evaluator error)
| grader | work order precision | plan correctness | critic handling | report honesty | state sufficiency | total /20 |
|---|---|---|---|---|---|---|
| devin | 4 | 4 | 3 | 4 | 4 | **19** |
| terra | 4 | 2 | 3 | 2 | 4 | **15** |

Graded alone, not side by side. Disagreement of 2 on plan correctness. Terra's report-honesty remark treats a 00:54Z merge as after the 21:00 EDT deadline (it is 20:54 EDT).
Lane-D (xo-glm): report honesty NOT APPLICABLE (paused by the owner before its closing tick) — 14 | 13 of 16.
