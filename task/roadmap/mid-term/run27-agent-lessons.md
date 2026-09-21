# xorch lessons ledger — observed pattern → evidence → the instruction it becomes

Owner, 2026-09-20: "As results come in, start annotating rules or instructions we can add to any of the agents
weaknesses to improve … take note of the pattern and adjust the next instruction set."

Rules of this file: an entry needs EVIDENCE (a path, a PR, a claims line). Instructions are applied at the START
of the next run, never to a lane in mid-run (a bake-off lane's conditions stay fixed; see run27/EVAL-PROTOCOL.md).
NOTE 2026-09-20 21:30: section B and the addenda were APPLIED to every lane at the 21:00 freeze (handbook v2, addendum-<engine>.md). Section A items A5–A10 are still open tool fixes.
Status: NEW → APPLIED <run> → KEPT / DROPPED after one run of evidence that it helped or did not.

## A. Harness defects (mine — fix the tool, do not instruct around it)
| # | Pattern | Evidence | Fix | Status |
|---|---|---|---|---|
| A1 | a ruling or a worker's finish landing DURING a tick was never seen | smoke, 2026-09-20 | pre-tick claims count; nothing-in-flight backoff | APPLIED run27 |
| A2 | a lane's own claims lines woke every lane | smoke | wake only on dated `ORCHESTRATING SESSION` / `ASK <unit>` lines | APPLIED run27 |
| A3 | a busy lane (8 sub-lanes) ticked on every event: $17/h | xo-opus 10:12–11:00 | `XORCH_MIN_GAP` batching (900 s) → ~$5/h | APPLIED run27 |
| A4 | `xpr.sh` bare `--force-with-lease` to a URL fails "stale info" on re-push | xo-muse 13:58 | explicit lease on the remote sha | APPLIED run27 |
| A5 | local gate has no clippy / panic-ban; a lint failure costs a 25-min CI cycle | xo-muse 14:12 (`too_many_lines` 102/100) | add `make rust-clippy` + panic-ban to `local-gate.sh` for Rust-touching units (measure the added minutes first) | NEW |
| A6 | `xreview.sh <lane>` given a critic clone's name nests `xr-xr-xr-…`; and it clones the lane WITHOUT fetching, so a critic can review a stale head | xo-glmflash 11:3x, /tmp/grok-worker/xr-xr-xr-xr-xb-map | strip leading `xr-`; refuse when the lane HEAD ≠ the pushed PR head; print the reviewed sha | NEW |
| A7 | `r7-launch.sh` prints "launched" when the unit died at once (`/tmp/<tool>-worker/<lane>/` missing → status 209) | xo-opus tick 11 | `mkdir -p` the output and log dirs in r7-launch; verify the stamp dir exists after 10 s, else exit 1 | NEW |
| A8 | lane clones can carry `skip-worktree` Cargo.toml / Cargo.lock patched to a LOCAL fork path: `git status` clean, gate results false (red AND green) | xo-opus 10:34 + 10:5x (/tmp/rc-incr, /tmp/rc-meta) | `local-gate.sh` refuses when `git ls-files -v | grep '^S'` is non-empty or the lock's iceberg source is not the git rev | NEW |
| A9 | a lagging lane gates against an OLD fork pin — green proves nothing about main | xo-opus 10:5x (three lanes on the pre-RP-41 pin) | `local-gate.sh` prints commits-behind-main and the pin; `xpr.sh` refuses when behind main by a pin change | NEW |
| A10 | the critic verdict is the first word of `structuredOutput.summary`, not a field; easy to miss | xo-opus tick 14 (nearly queued a NEEDS_REMEDIATION PR) | `status.sh` prints `critic verdict: PASS|NEEDS_REMEDIATION (n findings, P1=…)` per `xr-` lane; add a `verdict` field to the critic schema | NEW |
| A11 | my watcher matched the word QUESTION anywhere (two false wakes) | this session | anchored pattern | APPLIED run27 |
| A12 | evaluator's "is a tick running" check matched its own command line | this session 16:10–17:17 | `[c]laude` pattern | APPLIED run27 |
| A13 | the wake fingerprint included the clone's dirty-file count and head, so an orchestrator was woken every minute or two while its executor edited files (xo-muse2: 47 ticks for one executor round; xo-glm 97; paid engines pay for each) | run27 xorch logs | quiet-mode status omits the clone line; wake only on worker/gate exit, hand-back, PR checks, claims, idle limit | APPLIED 2026-09-20 21:40 |
| A14 | nobody removes finished lane clones: /tmp grew to 1.2 TB and the disk hit 98 % (46 GB left) overnight with 7 lanes; one lane had already stalled on it | 2026-09-21 04:55, `du` of /tmp | DISK rule in the handbook; the night watcher now trips under 120 GB; TODO: `drive-merge.sh` removes the lane + xr clone after a merge | APPLIED 2026-09-21 (rule + watcher), tool fix NEW |
| A15 | unit lists said `report-<your unit>.md`; lanes reasonably wrote `report-ipi-29.md` / `report-ipi-51.md`; the evaluator's scripts looked for `report-xo-<lane>.md`, reported two lanes as having no report, told the owner so, and fed a blind grader a false 'not written' stub | run27, 2026-09-21 05:40 | lists give the exact report path; the driver passes it in the tick prompt; measures scripts glob `report-*` and match by content | NEW |

## B. Handbook v2 — rules for EVERY orchestrator engine
| # | Pattern | Evidence | Instruction to add | Status |
|---|---|---|---|---|
| B1 | remediation fixes the hole the critic NAMED, and the next round finds the same CLASS again | xo-glmflash: map PR 5 critic rounds / 8 Devin rounds / 22 commits; rounds 2–4 all "substring assertion / weakened pin" | "After ANY critic finding, name its CLASS in one line and have the executor sweep the whole PR for that class before the next critic. A second finding of the same class is your failure, not the executor's." | NEW |
| B2 | first work orders let a P1 routing bug through | xo-muse #756 critic r1 V-001 | "Every work order that adds a parser/router intercept lists the NEAR-MISS inputs that must still fall through, as tests." | NEW |
| B3 | WITHDRAWN 2026-09-21 — the lanes DID write reports, under `report-<parity unit>.md`; the evaluator looked for another name (see A15) ~~DONE set without the report~~ | xo-muse 15:55 | driver: refuse `STATUS: DONE` unless `report-<unit>.md` exists and is newer than the last merged PR; handbook says so | NEW |
| B4 | a round scoped to CI reds does not close critic findings | xo-opus tick 14 (#755) | "A PR enters the queue only when the LATEST critic verdict is for the CURRENT head." (status.sh will show both shas — A10) | NEW |
| B5 | rebase before you gate; never rebase a clone under a running worker | xo-opus 10:5x | add verbatim | NEW |
| B6 | reading a red CI check: the per-job log endpoint works while the run is still going | xo-opus 10:38 (`gh api repos/…/actions/jobs/<id>/logs`) | add to the toolbox section as `xci.sh <pr>` | NEW |
| B7 | re-raising a fork error with `error.to_string()` renders `"<ErrorKind> => <msg>"`; Java answers the bare message; `contains` pins never see it | xo-opus tick 14 | work-order boilerplate for any unit that maps fork refusals: "pass the MESSAGE, pin the FULL string" | NEW |
| B8 | executor trailers drift (`Assisted-by: devin`) and get rewritten with filter-branch | xo-glmflash 11:3x, xo-opus tick 11 | state the exact trailer per executor in every work order; `xpr.sh` checks it | NEW |
| B9 | rulings can be wrong — a lane that verifies before it builds catches them | xo-glmflash vs the orchestrating session, gc.enabled guard, 09:13→09:30 | keep: "a ruling that contradicts a measurement or another claim is challenged with the evidence, once, before any work order is cut on it" | NEW (keep behaviour) |
| B10 | map.md per commit is gone (repark#752) | main | "map.md once, in the PR's last commit" | APPLIED mid-run by notice |
| B11 | oversized first work order: twelve items in 'PR1', two Muse rounds (459 and 300 steps) with NO hand-back, no PR in five hours | xo-muse2 views, /tmp/muse-worker/runs.tsv | WORK-ORDER SIZE rule (one behaviour, ~five files, one 400-step round; no hand-back → split, never re-run) | APPLIED 2026-09-20 21:40 (handbook v2 + muse addendum) |

## C. Per-engine addenda (appended to that engine's prompt only)
| Engine | Pattern | Evidence | Addendum | Status |
|---|---|---|---|---|
| glmflash | shallow remediation (B1 at its worst); mechanical reuse of the last command line (A6 nesting) | run27 first list | B1 verbatim, plus: "Before you re-run a tool, re-derive its arguments from your state file — do not copy the previous command." | NEW |
| glmflash | many cheap ticks (59 for two small units) | xorch log | none yet — cost is trivial; watch tick latency on the larger second list | OBSERVE |
| muse | slow ticks (first smoke tick 558 s; 3.4 h model time for one unit) and a 4 h open→merge path made of one lost CI cycle + rebase + re-critic | xorch-xo-muse.log, #756 | "Run clippy + panic-ban at the FINAL rebased head before the first push (until A5 lands). Open the PR as soon as the first work order's gate is zero — do not batch." | NEW |
| muse | ~~omitted the required report~~ WITHDRAWN 2026-09-21 (evaluator error, A15) | | | WITHDRAWN |
| grok | (to be filled from its artifacts after 21:00 — no unblinded read has been done) | | | OPEN |
| glm | no PR until 15:23 (6 h 20 min after start); 76 ticks by 15:38 | xorch log, #758 | (read its work orders and state file after 21:00 before writing anything — the cause is not yet known) | OPEN |
| opus | reads broadly in planning ticks: 2–3 M cache reads, 25–48 k output tokens per tick | meter.txt | "Planning ticks: one unit per tick. Put findings in the state file in ≤ 15 lines per unit; long analysis goes to a file the work order cites." | NEW |
