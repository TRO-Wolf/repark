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

- `env.sh` — the only place paths live. Sourced by every script. `COORDINATOR_ROOT` (campaign
  root, default `/tmp/oc-worker`), `COORDINATOR_LIB` (sibling tooling: `build-slot.sh`,
  `local-gate.sh`, `comment_ban.py`, `drive-merge.sh`; default `$ROOT/_lib`), `COORDINATOR_SCRATCH`
  (where lane clones and worker output live, default `/tmp`), `COORDINATOR_MERGE_QUEUE`,
  `COORDINATOR_GH_OWNER`. The handbook is a template over the same names (`{{HERE}}`, `{{LIB}}`,
  `{{ROOT}}`, `{{SCRATCH}}`, `{{MERGE_QUEUE}}`), filled by the driver at tick time.
- `start.sh <run-dir> <unit> <engine> [until]` — launches one lane as a transient user unit in
  `repark.slice` (`Restart=always`, four restarts per two hours, exit 0 never restarted). Refuses a
  missing list, an unknown engine, a missing lib directory, or a slice without a memory cap.
- `drive.sh` — the loop. Each iteration: stop if state says DONE; compile the tick prompt (handbook
  + engine addendum + list + state + `status.sh` + the claims tail + the tick instruction); run
  `engine-<engine>.sh`; log the meter; then wait. `WORKING` re-ticks at once; `WAITING` sleeps until
  the `status.sh quiet` fingerprint changes, a ruling or `ASK <unit>` line lands in claims, the idle
  limit (`COORDINATOR_MAX_IDLE`, 1500 s, doubling from 60 s while nothing is in flight) or the
  deadline passes. `COORDINATOR_MIN_GAP` batches wake events. Three failed ticks in a row exit 1.
- `status.sh <run-dir> <unit> [full|quiet]` — the world as the lane sees it: active worker units,
  the newest round per worker family (exit, hand-back present), the clone's head and dirtiness, the
  local-gate result, the PR states (cached five minutes), the merge-queue tail, and the count of
  claims lines addressed to it. `quiet` omits the volatile fields so the driver can fingerprint it.
- `engine-<name>.sh <workdir> <prompt-file> <out-dir>` — one bounded model call, writing the raw
  output plus `meter.txt` (turns/steps/tools and cost where the CLI reports it; blank is unknown,
  not zero). `grok` (grok-4.7, effort `COORDINATOR_EFFORT` default xhigh), `grok47` (alias),
  `muse` (muse-spark-1.3-contributor, max), `glm` (opencode on zai/glm-5.3), `glmflash` (alias on
  glm-5.3-flash), `sol` (codex on gpt-5.6-sol, high). Model, effort, turn cap and tick timeout come
  from `COORDINATOR_MODEL`, `COORDINATOR_EFFORT`, `COORDINATOR_TURNS`, `COORDINATOR_TICK_TIMEOUT`.
  The Claude engine is not in the repo.
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
- `addendum-<engine>.md` — per-engine additions from the lessons ledger, appended to the handbook
  for that engine only.
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
| see every lane | `for u in <run-dir>/state-*.md; do head -1 "$u"; done`, or `status.sh <run-dir> <unit>` for one |
| stop a lane now | `systemctl --user stop <unit>`; its state stays where it was |
| run this from another root | `COORDINATOR_ROOT=… COORDINATOR_SCRATCH=… scripts/coordinator/start.sh …` (both are passed into the unit) |

## Known weak spots (open, from run 27)

A running `drive.sh` keeps the code it started with, so a patch reaches only lanes started after it
(A17); the claims file is a plain text file any writer can clobber (A16 — a locked append helper is
owed); a provider outage counts as failed ticks and can spend the restart budget; a critic PASS must
not outlive the diff it judged (B12 — `review.sh --reuse <hash>` is owed). Evidence for each is in
`lessons.md`.
