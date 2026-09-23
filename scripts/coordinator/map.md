# map — scripts/coordinator/

The tick-driven lane coordinator: bash does the waiting, three files do the remembering, a model
does one bounded round of thinking per tick. One lane = one systemd unit = one `drive.sh` process.
Moved into the repo on 2026-09-22 from the campaign's scratch tooling (`_lib/xorch/`, renamed:
the owner found `xorch` unpronounceable). Nothing here is a CI gate; it is the operator's tooling
for running the campaign's lanes, checked in so it has a versioned home.

## Purpose

An orchestrating session used to be one long-running model conversation that waited, in context
and on the bill, for every build, critic and CI run. The coordinator turns that into a row of short,
stateless model calls ("ticks") with free waits between them. The model keeps nothing between ticks;
everything the next tick needs is in the run directory's files:

| File (in the run directory) | Written by | Holds |
|---|---|---|
| `list-<unit>.md` | the lead session, once | the lane's assignment, what proves it, what it must not touch |
| `state-<unit>.md` | the lane, every tick | line 1 `STATUS: WORKING|WAITING|DONE`, then `LANES:`, `PRS:`, free notes |
| `claims.txt` | everyone, append only | ownership claims, `QUESTION` lines up to the lead, `ORCHESTRATING SESSION:` rulings down |
| `until-<unit>` | the lead, optional | a deadline as `HHMM` or an epoch; overrides the launch argument |
| `report-<unit>.md` | the lane, on its finish tick | the lane's own account of what it did |
| `ticks-<unit>/NNN/` | the driver | the compiled prompt, engine output and `meter.txt` for every tick |
| `coordinator-<unit>.log` | the driver | one line per tick start/end and per wake reason |

The lead never messages a lane: it edits the list, appends to claims, or writes the until-file.
The lane never edits code: it cuts a PR-sized work order for a worker round on a scratch clone and
reads the worker's `handback.json`. Workers do not delegate; lanes do not start lanes.

## Contents

- `env.sh` — the only place paths live. Sourced by the six path scripts (`start`, `drive`, `status`,
  `gate`, `review`, `pr`); the engines read only `COORDINATOR_*` knobs with defaults. `COORDINATOR_ROOT` (campaign
  root, default `/tmp/oc-worker`), `COORDINATOR_LIB` (sibling tooling: `build-slot.sh`,
  `local-gate.sh`, `comment_ban.py`, `drive-merge.sh`; default `$ROOT/_lib`), `COORDINATOR_SCRATCH`
  (where lane clones and worker output live, default `/tmp`), `COORDINATOR_MERGE_QUEUE`,
  `COORDINATOR_GH_OWNER`. The handbook is a template over the same names (`{{HERE}}`, `{{LIB}}`,
  `{{ROOT}}`, `{{SCRATCH}}`, `{{MERGE_QUEUE}}`), filled by the driver at tick time. Today the
  sibling tooling in `$LIB` (`r7-launch.sh`, `r9-lane.sh`, `local-gate.sh`, `drive-merge.sh`) still
  writes under `/tmp` and `/tmp/oc-worker` regardless of these variables, so a non-default root or
  scratch only moves the coordinator's own files and is not a supported way to run; the variables
  exist for the day that tooling takes the same names.
- `start.sh <run-dir> <unit> <engine> [until]` — launches one lane as a transient user unit in
  `repark.slice` (`Restart=always`, four restarts per two hours, exit 0 never restarted); every
  `COORDINATOR_*` variable set in the launcher's environment is forwarded into the unit. Refuses a
  missing list, an unknown engine, a missing lib directory, or a slice without a memory cap.
- `drive.sh` — the loop. Each iteration: stop if state says DONE; compile the tick prompt (handbook
  + engine addendum + list + state + `status.sh full` + the tick instruction); run
  `engine-<engine>.sh`; log the meter; write the digest marker `.digest-<unit>`; then wait. `WORKING` re-ticks after five seconds; `WAITING` sleeps until
  the `status.sh quiet` fingerprint changes, a ruling or `ASK <unit>` line lands in claims, the idle
  limit (`COORDINATOR_MAX_IDLE`, 1500 s, doubling from 60 s while nothing is in flight) or the
  deadline passes. `COORDINATOR_MIN_GAP` batches wake events. Three failed ticks in a row exit 1.
- `status.sh <run-dir> <unit> [full|quiet]` — the world as the lane sees it: active worker units,
  the newest round per worker family (exit, hand-back present), the clone's head and dirtiness, the
  local-gate result, the PR states (cached five minutes), the merge-queue tail, and the count of
  claims lines addressed to it. `quiet` omits the time, the clone line and the merge-queue tail so
  the driver can fingerprint it; when the PR cache refreshes and `gh` fails, the previous line for
  that PR is kept rather than replaced, so a `gh` outage does not wake the driver.
- The digest (`status.sh … full`, first block, `## since your last tick`) — measured on 2026-09-22
  over 294 ticks: the median tick spent 9 turns and $0.45, its first 4–6 turns re-discovering the
  world by hand, and every later turn re-reading that. The driver now computes the delta in bash so
  the model reads about 1.5 KB and acts. It is relative to the marker `$RUN/.digest-<unit>`, one line
  `lines=<claims lines> main=<sha> t=<epoch>` whose mtime is `t`. The driver takes `t` and the
  claims line count just before it compiles the prompt, and writes the marker after the tick ends.
  Anything that lands while the model is acting is therefore shown again next tick, never lost; the
  worst case is a line the lane already saw. The block holds: main's short sha from
  `gh api …/commits/main` (cached five minutes in `.maincache-<unit>`) and whether it moved against
  the marker; per lane, every worker round whose `exit` is newer than the marker (`ended:` with
  exit, hand-back `yes|no|text`, the hand-back's status and commit count, or the `meter.txt` /
  `runs.tsv` line when there is no hand-back), active units started after `t` (`started:`), and
  `<lane>-*.done` files newer than the marker; one `pr` line per PR with CI folded into
  `green|red:<names>|pending:<k>`, the head's short sha and `behind_main` from `mergeStateStatus`;
  every claims line after the marker's count that names the unit or is a ruling (with no marker,
  or a claims file that shrank, the last 15 such lines); the claims total; free disk and memory.
  `ended:` names a round `<family>-<lane>`, not the systemd unit, because the unit's
  `HHMMSS` suffix is the launch time, which can differ from the round stamp. The PR lines come
  from the same single `gh pr view` per PR as the older block, cached in `.prcache-<unit>.d`
  beside it. `quiet` shows none of the digest's times or counts; it adds only the lane's done-file
  names and the `pr` lines, so the fingerprint moves on a push, a CI change or a new done-file.
  Every external call degrades to `(gh failed)` or an absent line rather than aborting.
- `verdict.sh <lane>` — the critic-verdict reader. The longest 10 % of ticks (28+ turns, 28 % of
  the cost) were critic verdicts and hand-backs judged by reading raw files. It picks the newest
  round, by stamp, across `$ROOT/codex-worker/xr-<lane>/` and `$SCRATCH/grok-worker/xr-<lane>/`, and
  prints `VERDICT PASS|NEEDS_REMEDIATION|VOID|RUNNING|NONE`, the engine, round and head the
  critic states, `tools= turns= cost= valid=`, and one `finding <id>:` line per question (the
  first 160 chars). Codex: the verdict word is the first word of `handback.json`'s summary, and
  turns and tools come from the `runs.tsv` row for the lane and stamp (columns 7 and 8).
  Grok: the first word of `out.json`'s `structuredOutput.summary`. Tools are the sum of
  `modelUsage.*.modelCalls`, because the CLI's JSON carries no tool-call count. Cost comes from
  `total_cost_usd`. A round is `VOID` when tools < 3, when the summary does not start with a
  verdict word, or when the hand-back is missing, and the reason is printed. A round with no
  `exit` file is `RUNNING`. Exit codes: 0 PASS, 1 NEEDS_REMEDIATION, 2 VOID, 3 RUNNING or NONE.
  The head is the first sha after the word "head" in the summary, else its first 40-hex sha.
- `engine-<name>.sh <workdir> <prompt-file> <out-dir>` — one bounded model call, writing the raw
  output plus `meter.txt`: `grok` and `glm` write turns/steps and `cost_usd` (0 when the CLI
  omits it); `muse` writes tool count and terminal state and `sol` token counts — neither reports
  a cost, so their lanes show no dollar figure, which means unknown, not free. `grok` (grok-4.7, effort `COORDINATOR_EFFORT` default xhigh), `grok47` (alias),
  `muse` (muse-spark-1.3-contributor, max), `glm` (opencode on zai/glm-5.3), `glmflash` (alias on
  glm-5.3-flash), `sol` (codex on gpt-5.6-sol, high). Model, effort, turn cap and tick timeout come
  from `COORDINATOR_MODEL`, `COORDINATOR_EFFORT`, `COORDINATOR_TURNS`, `COORDINATOR_TICK_TIMEOUT`
  (`COORDINATOR_VARIANT` for `glm`). Files: `engine-grok.sh`, `engine-grok47.sh`, `engine-muse.sh`,
  `engine-glm.sh`, `engine-glmflash.sh`, `engine-sol.sh`. The Claude engine is not in the repo.
- `gate.sh <lane> <crates…> <pytest paths…>` — queues the local gate under `build-slot.sh` and
  returns at once; the result lands in `$ROOT/<lane>-localgate.done`.
- `review.sh <lane> <brief>` — clones the lane's clone to `rv-<lane>`, points origin at GitHub with
  pushes disabled, fetches main, and launches the Grok critic (`COORDINATOR_CRITIC`, default the
  grok-worker skill) read-only on it. The verdict is the first word of the hand-back summary.
- `pr.sh <lane> "<title>" <body-file> [draft]` — refuses a dirty tree, a clone without the pre-push
  hook, a foreign commit identity, a banned trailer, a comment-gate failure or banned PR-body text;
  then pushes with an explicit lease and opens or updates the PR. The banned-trailer patterns are
  spelled with bracket classes so the tree never carries the literals the pre-push hook forbids.
- `handbook.md` — the standing instructions every tick starts with: how ticks work, the hard rules
  (comment ban, no Claude models, no AWS, identity and trailers, questions before rulings), the
  toolbox, the order of work for a unit, and the rules learned from run 27.
- `addendum-glm.md`, `addendum-glmflash.md`, `addendum-muse.md` — per-engine additions from the
  lessons ledger, appended to the handbook for that engine only (`drive.sh` loads
  `addendum-<engine>.md` when it exists).
- `critic-brief-template.md` — the brief a lane fills and hands to `review.sh`.
- `lessons.md` — the ledger: observed pattern → evidence → the instruction it became. Applied at the
  next run's start, never mid-run. Rows keep the pre-rename names; its header says how they map.

## I want to…

| … | do |
|---|---|
| start a lane until 06:00 | `scripts/coordinator/start.sh <run-dir> co-muse9 muse 0600` after writing `<run-dir>/list-co-muse9.md` |
| rule on a lane's question | `echo "$(date '+%F %H:%M') ORCHESTRATING SESSION: <unit> Q1 — …" >> <run-dir>/claims.txt` (append only, never `>`) |
| hand a unit over | edit `list-<unit>.md`; the next tick reads it |
| extend or cut a deadline | `date -d '2026-09-23 06:00' +%s > <run-dir>/until-<unit>` (an epoch survives midnight; `HHMM` does not) |
| read a lane's critic verdict | `scripts/coordinator/verdict.sh <lane>`; branch on its exit code (0 PASS, 1 NEEDS_REMEDIATION, 2 VOID, 3 RUNNING or NONE) |
| see every lane | `for u in <run-dir>/state-*.md; do head -1 "$u"; done`, or `status.sh <run-dir> <unit>` for one |
| stop a lane now | `systemctl --user stop <unit>`; its state stays where it was |

## Known weak spots (open, from run 27)

A running `drive.sh` keeps the code it started with, so a patch reaches only lanes started after it
(A17); the claims file is a plain text file any writer can clobber (A16 — a locked append helper is
owed); a provider outage counts as failed ticks and can spend the restart budget; a critic PASS must
not outlive the diff it judged (B12 — `review.sh --reuse <hash>` is owed). Evidence for each is in
`lessons.md`.
