# Run 27 orchestrator bake-off — evaluation protocol
Written 2026-09-20 10:58 EDT, BEFORE any lane has finished a unit, at the owner's instruction: "we need to be
completely agnostic on the results and evaluate them as unbiased as possible". Nothing below may be changed after
results are seen; anything added later is labelled POST-HOC in the report.

## What this run can and cannot show
- The four lanes run DIFFERENT units (45 / 15 / 11 / map step + 8 cells, one of them resuming a half-built
  branch). Unit difficulty is confounded with the orchestrator. So this run yields CASE EVIDENCE per lane and
  pass/fail on minimum standards — NOT a ranking. A ranking needs the crossover in the last section.
- xo-opus runs a different job (close-out of stopped work) and is reported separately; it is not a contestant.
- One day, one unit each: n is tiny. No claim of "better" on a difference a single bad executor round could make.

## Known sources of bias, recorded now
1. The evaluator (the orchestrating session) built the harness, wrote the handbook and the unit lists, and
   answers the lanes' questions. One of its rulings today was wrong and a lane (xo-glmflash) corrected it; that
   story is vivid and must not be scored twice or stand in for that lane's other work.
2. The critic for every lane is Grok — also a contestant as orchestrator. It reviews executor code, not
   orchestration, but the overlap is declared.
3. Executors are chosen by each orchestrator (Devin or Muse): executor quality leaks into orchestrator scores.
4. Muse is on a subscription: no dollar cost is observable. Cost is compared on model seconds and ticks too.
5. Lanes share build slots, one JVM lock and one merge queue: waiting time is partly luck of the queue.

## Measures — all collected by script from artifacts, never from a lane's own report
| Measure | Source |
|---|---|
| inventory cells EQUAL before → after, per unit | the scoreboard harness replayed by the evaluator on merged main, the unit's cell ids from units.py — a lane's claimed count is reported beside it, and the gap is itself a measure |
| PRs opened / merged / closed unmerged | gh |
| critic verdicts per PR (PASS / NEEDS_REMEDIATION), findings by severity | /tmp/grok-worker/xr-*/…/out.json |
| local gate results per attempt | /tmp/oc-worker/<lane>-localgate.done history |
| CI failures per PR, by check | gh pr checks / run history |
| comment-gate or identity stops at xpr.sh | lane logs |
| executor rounds per unit, by tier, and rounds that produced no usable commit | the workers' runs.tsv + git log |
| questions asked; of those, answerable from the packet/INDEX (should not have been asked) vs genuinely open; rulings challenged with evidence | claims.txt, classified by the blind graders |
| rule breaches (waited inside a tick, launched a Claude model, touched another lane, skipped the critic, weakened a pin) | tick logs + critic findings + blind graders |
| ticks, model seconds, dollars, wall-clock to first PR and to first merge | xorch logs |
| post-merge defects: a regression on main within 48 h attributable to the lane's PR | next two scoreboards + CI on main |
| evaluator interventions per lane (rulings, fixes, restarts) | claims.txt + this session's log — reported as a covariate, not hidden |

## Blind quality grading
- Material: each lane's work orders, state files, PR bodies and its final report, with every model, lane and unit
  name replaced (Lane A–D assigned by a seeded shuffle; seed = the run's first merged PR number).
- Graders: two model families that are NOT contestants and not Claude — Codex (gpt-6-astra, medium, read-only
  sandbox) and Devin. Each grades independently against the fixed rubric below; disagreements of 2+ points are
  reported, not averaged away. The evaluator does not grade.
- Rubric (0–4 each): work-order precision (files, functions, cells, gate, what not to touch) · correctness of
  the plan against the packet · handling of critic findings (fixed exactly, nothing waved through) · honesty of
  the report against the artifacts · state-file sufficiency (could a fresh tick continue from it alone).

## Minimum standards (pass/fail, any lane)
No merged PR without critic PASS + gate zeros + CI green; no code comment merged; no foreign identity or banned
trailer pushed; no invented ruling; no weakened or deleted assertion accepted.

## Reporting rules
Every measure for every lane in one table, favourable or not, including measures that embarrass the harness.
Failures caused by the harness (driver races, a wrong ruling, a toolbox bug) are attributed to the harness, not
the lane. No adjectives in the results section; interpretation in a separate section, with the confounds restated.

## To get a ranking: crossover (proposed, owner decides)
Three more rounds with the units rotated so every orchestrator runs every unit class once (a 4 × 4 Latin
square over: large mechanical / small greenfield / resume-a-broken-branch / repo-tooling), same executor tier
fixed for all (Muse max), same critic. Rank only on that.

## POST-HOC addition, 2026-09-20 14:07
xo-glmflash set STATUS: DONE at about 14:00 with both of its units merged (repark#751, repark#752). Rather than leave
the engine idle for seven hours it was given a second list (unit name xo-glmflash2: IPI-26/27 parser and DDL, 34
cells — a larger and harder unit class than its first). Its first-list measures are frozen as of DONE; second-list
measures are reported in a separate row. Any other lane that reaches DONE early gets the same treatment.

## POST-HOC addition, 2026-09-20 15:58
xo-muse set STATUS: DONE at about 15:55 (first list: IPI-29, repark#756). Same rule as above: first-list measures
frozen; the engine continues as xo-muse2 on IPI-40 views (13 cells, a large greenfield unit), reported in its own row.
- Observed 15:58: xo-muse set DONE without writing its report (the list required one). EVALUATOR INTERVENTION (covariate): the
  second list asks the engine to write report-xo-muse.md from its frozen state file. The omission itself stays on the record.
- HARNESS DEFECTS found by lanes (attributed to the harness, not the lane): (1) xpr.sh used a bare --force-with-lease against an explicit URL,
  which fails with 'stale info' on a re-push — reported by xo-muse 13:58, fixed 16:50 for all lanes (explicit lease on the remote sha);
  (2) the local gate does not run `make rust-clippy`, so a clippy failure costs a full CI cycle — reported by xo-muse 14:12; NOT changed
  mid-run (it would change every lane's conditions); to be decided after the run.

## OWNER-DIRECTED CHANGE, 2026-09-20 18:25 (not an evaluator choice)
The owner's Muse budget resets in ~90 minutes; he asked to pause "a couple of the current testing lanes" and start 1–2 full-Muse lanes.
- PAUSED at 18:25: xo-glm (first list in progress: IPI-32, repark#758 open) and xo-glmflash2 (post-hoc second list, repark#759 open).
  Chosen because the owner's own orchestrator shortlist was Grok / Muse / Sol and the GLM lanes were added as an extra; NOT chosen on
  results. Consequence: xo-glm's first-list measures are TRUNCATED at 18:25, 2 h 35 min before the planned 21:00 finish — any
  comparison of totals must use per-hour rates or the common window 09:05–18:25. They resume later under the same lists.
- STARTED: xo-muse3 (ORC/Avro, handed over from xo-opus) and xo-muse4 (procedures). These are PRODUCTION lanes, not contestants: their lists
  carry lessons-ledger instructions (clippy before push, class sweep, mkdir traps), so they are not comparable with first-list lanes.
- xo-muse2 told to use Muse-max executors only from now (instruction change on a post-hoc list).
- The planned evidence-based choice of an engine for ORC/Avro at 21:00 is superseded by this direction.

## POST-HOC, 2026-09-20 23:46 — grader substitution (technical)
The Astra grader failed at launch: "The 'gpt-6-astra' model requires a newer version of Codex" (CLI 0.150.0; the evaluator does not upgrade
the owner's CLI unasked). Substitute: gpt-5.6-terra at MEDIUM effort, read-only sandbox, same brief, same bundle — also OpenAI-family,
also neither a contestant nor Claude. Decided before reading the Devin grader's output.

## EVALUATOR ERROR AND CORRECTION, 2026-09-21 05:14
The unit lists said "write /tmp/oc-worker/run27/report-<your unit>.md". Two lanes read "unit" as the parity unit and wrote
`report-ipi-29.md` (Muse lane, 15:56, BEFORE it set DONE) and `report-ipi-51.md` (Grok lane, 21:03, at the deadline tick). The evaluator's
scripts looked only for `report-xo-<lane>.md` and recorded "no report" for both. Consequences, all the evaluator's fault, none the lanes':
- The statement "xo-muse set DONE without writing its report" (15:57 entry above) is WRONG and is withdrawn, with the intervention it triggered.
- The blind graders were given a "not written" stub for the Grok lane (Lane-B), so its report-honesty score of 0 from both graders is INVALID.
  Lane-B is being re-graded by the same two graders with the real report; the first Lane-B totals (16 | 13) are superseded.
- xo-glm (Lane-D) was paused by the owner at 18:27, 2.5 h before the finish, and never had a closing tick: its report-honesty 0 is NOT APPLICABLE,
  not a fault. Its totals are reported on four criteria (out of 16).
- Lessons-ledger entry B3 ("DONE without the report") and the muse/grok addenda lines built on it are withdrawn; replaced by harness entry
  A15 (state the exact report path in the list).
