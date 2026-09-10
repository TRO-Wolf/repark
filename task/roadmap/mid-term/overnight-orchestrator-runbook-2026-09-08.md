# Overnight orchestrator runbook — running the cheap-tier slate unattended

**Date:** 2026-09-08 · **For:** an orchestrating session on a cheaper tier than the author
(Opus 5 by the owner's naming; the same text works for any session that has the shell, the
launcher skills and this repository) · **Companion:**
[cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md), whose §1 this runbook turns
into exact commands. On any conflict the slate's cards win on *what*, this file wins on *how*,
and [../../../AGENTS.md](../../../AGENTS.md) wins on everything.

## 0. What the owner must grant before the first overnight run

| Grant | Default if not granted |
|---|---|
| G-1 Squash-merge on green CI for PRs whose diff stays inside the card's Home files. | Open the PR, flip it ready, stop; the owner merges in the morning. |
| G-2 Decision authority per §6 (bounded). | Park the unit on the first hand-back that needs a decision. |
| G-3 A stop time (local) and a usage ceiling. | Stop at 03:00 local, or on the first rate-limit or usage-limit signal, whichever comes first. |
| G-5 Seed commits: the orchestrator may make a card's step-0 dependency commit itself when the card spells the exact lines (CFG-1 D-3 does). | Park the card until the owner seeds it. |
| G-4 Which launchers may run: `oc-worker` (GLM), `muse-worker` (Muse), `grok-worker` (Grok — granted 2026-09-09 evening with ~82 % weekly quota left, for slate 2's Ballista and REVIEW-1 cards). | GLM and Muse only. |

The owner launches the session (§9). The session never launches another orchestrator.

## 1. Session start (five reads, nothing else)

1. `AGENTS.md` "Read first" pointers, `STATUS.md` "Current milestone".
2. The slate: §0 rulings, §1 preamble, and only the cards for the lanes this run will open.
3. This file.
4. The memory index at `~/.claude/projects/-home-john-CodeRepos-LocalRepark-repark/memory/MEMORY.md`
   (same project path, so the session loads it when its working directory is the live checkout):
   `campaign-state.md` for what merged last, `process-mode.md` for the tier rulings,
   `oc-worker-skill.md` / `muse-worker-skill.md` for launcher gotchas.
5. `git -C ~/CodeRepos/LocalRepark/repark status --porcelain` — the live checkout carries the
   owner's or Codex's uncommitted work. **Never commit, checkout, stash or reset there.** All
   work happens in `/tmp/oc-<lane>` clones.

## 2. Open a lane

```bash
LANE=explain1; UNIT=df-explain-1; BR=feat/df-explain-1
rm -rf /tmp/oc-$LANE && git clone -q ~/CodeRepos/LocalRepark/repark /tmp/oc-$LANE
cd /tmp/oc-$LANE && git remote set-url origin https://github.com/TRO-Wolf/repark.git \
  && git remote set-url --push origin no_push \
  && git fetch -q origin main && git checkout -q -B $BR origin/main && make install-hooks >/dev/null
```

Seed commit (only when the card's step 0 is `O`): edit `Cargo.toml` / `pyproject.toml` exactly
as the card says, run `make verify`, commit with the identity in §5, no trailer.

The unit ledger is born on the branch by the worker's first step (the preamble says so); if a
worker forgets it, the next brief's first line is "create the ledger first".

## 3. Assemble a brief and launch one round

```bash
mkdir -p /tmp/oc-worker/$LANE
{ sed -n '/^```text$/,/^```$/p' task/roadmap/mid-term/cheap-tier-slate-2026-09-08.md | sed '1d;$d' \
    | sed "s/<UNIT>/$UNIT/; s/<N>/1/; s/<branch>/$BR/; s/<MODEL-LINE>/GLM 5.3 Flash (zai\/glm-5.3-flash) <noreply@z.ai>/";
  echo; echo "## The step"; echo;
  awk '/^### Card DF-EXPLAIN-1/,/^---$/' task/roadmap/mid-term/cheap-tier-slate-2026-09-08.md;
  echo; echo "Do STEP 1 of the Steps table only."; } > /tmp/oc-worker/$LANE/brief-1.md
```

(The first `sed` extracts the §1.2 preamble; the `awk` copies the whole card so the worker sees
its Decisions; the last line names the step.) Then append the per-round fence, verbatim — a Muse
round ignored the preamble's comment ban once, and repeating it here fixed that:

```text
Fence for this round: NO comments in code. Before you commit, run
  git diff --cached -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' | grep -P '^\+\s*(//|#(?! noqa))'
and it must print nothing; Rust `///` and `//!` count as comments. Facts go in map.md or the ledger.
```

Read the brief once before launching.

Launch (GLM; every launcher runs under `systemd-run` because a round outlives the 10-minute
Bash cap):

```bash
systemd-run --user --collect --quiet --unit="oc-$LANE-$(date -u +%H%M%S)" \
  --setenv=HOME=$HOME --setenv=PATH="$PATH" --setenv=USER=$USER -p WorkingDirectory=$HOME \
  -p StandardOutput=append:/tmp/oc-worker/$LANE/launch.log -p StandardError=append:/tmp/oc-worker/$LANE/launch.log \
  -- ~/.claude/skills/oc-worker/oc-worker.sh --lane $LANE --repo /tmp/oc-$LANE \
     --brief /tmp/oc-worker/$LANE/brief-1.md --role worker --max-turns 300
```

Muse (tier I steps): same wrapper around
`~/.claude/skills/muse-worker/muse-worker.sh --lane $LANE --repo /tmp/oc-$LANE --brief … --role worker --max-steps 400`
(no `--model` or `--effort` flag: the launcher's defaults are `muse-spark-1.3-contributor` at
`--effort max`, the owner's ruling of 2026-09-09; a brief never lowers them)
(the skill adds `--trust-workspace`; confirm `grep -c untrusted <run>/stderr.log` prints `0`).
Grok (under G-4): actor `~/.claude/skills/grok-worker/grok-worker.sh --lane $LANE --repo /tmp/grok-$LANE --brief … --role sepmo-actor --max-turns 300`;
critic `… --role critic-quality|critic-logic|critic-security --sandbox read-only --max-turns 120`
on a fresh clone of `main`. The launcher refuses `~/CodeRepos` and denies push/gh/aws. Run dirs
land in `/tmp/grok-worker/<lane>/<stamp>/`; read them with `python3 ~/.claude/skills/grok-worker/handback.py <run>`.
**Two Grok patterns to check on every hand-back:** (1) the turn-1 stall — `num_turns` 1 with a
placeholder summary: resume the same session with a proceed mandate ("never end a turn to report
progress; CONCLUDED is valid only with commits/report present"), and after two stalls launch a
fresh session; (2) fabrication — a `CONCLUDED` JSON naming commits with `num_turns` 1 and an
untouched lane: discard it, relaunch. A critic's report goes to a file the brief names, never
only into the JSON (long JSON text is truncated).

Run directories differ by launcher: `oc-worker` → `/tmp/oc-worker/<lane>/<stamp>/`, `muse-worker` →
`/tmp/muse-worker/<lane>/<stamp>/`, `grok-worker` → `/tmp/grok-worker/<lane>/<stamp>/`; the wait
loop and `handback.py` calls name the right one.

Wait **in the foreground**. A headless `-p` session ends the moment it ends a turn, so a
session that launches a lane and then "waits for the hand-back" by ending its turn has exited
and left the worker orphaned (this happened on the first night, 2026-09-09, after 27 tool
calls). Run this as one Bash call with the maximum timeout (600000 ms) and repeat the call until
the `exit` file exists; never use a background task to wait, and never end a turn while a lane
is running or §7 has steps left and §8 has not fired:

```bash
until ls /tmp/oc-worker/$LANE/*/exit >/dev/null 2>&1; do sleep 30; done
```

Concurrency: at most **two** worker lanes at once, plus a **third** lane when it is a Grok
round on `repark-distributed` (its own crate, feature-gated) or a read-only Grok critic; the
builder cap (no more than two concurrent cargo builds) and the one-JVM rule still bind. A fresh clone's first Python round runs
`maturin develop`, so before launching any lane wait until no build is running:
`while pgrep -x cargo >/dev/null || pgrep -x maturin >/dev/null || pgrep -x rustc >/dev/null; do sleep 30; done`
(`pgrep -f` would match your own shell's command line and never return). Put every launch
sequence in a script file under `/tmp/oc-worker/<lane>/` and run the file; an inline compound
command loses its newlines. A live-Spark step (any
`REPARK_PARITY_LIVE=1` measurement) runs alone: no other lane open, `pgrep -f java` empty, and the
brief exports `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` (the default `java` is 11).

Tier rule (R-15, after six dropped GLM rounds across runs 1–3): any round expected to produce a
document or a ledger with measured evidence, and any round expected to exceed ~30 tool calls
(measurements, multi-file Rust), runs on Muse; GLM keeps code-and-test rounds. Every brief says
*write the file as you go, commit when a section is complete*.

## 4. Read the hand-back, audit, decide

```bash
RUN=$(ls -d /tmp/oc-worker/$LANE/*/ | tail -1)
python3 ~/.claude/skills/oc-worker/handback.py $RUN
cd /tmp/oc-$LANE && git log --oneline origin/main..HEAD && git diff --stat origin/main..HEAD
```

Audit checklist, in order; any `no` sends the round back with a follow-up brief:

1. A commit exists for the step (a hand-back naming green gates with no commit is a fabrication).
2. `git diff origin/main..HEAD --name-only` stays inside the card's Home list plus the ledger
   and maps. Anything else: revert that file in a follow-up, do not merge it.
3. No comments in code: `git diff origin/main..HEAD -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' | grep -P '^\+\s*(//|#(?! noqa))'`
   must print nothing (Rust `///` and `//!` included; the one-line docstring the thinness gate
   demands is the only allowed docstring).
4. The pin was red first: the ledger's evidence cell pastes the failing run.
5. The card's gates re-run **by you** and pass: `cargo test -p <crate> <filter>` and `make verify`
   for Rust; `.venv/bin/python -m pytest python/repark/tests/<file>.py -q` for Python.
6. Commit message ends with the model's `Authored-By:` line and carries no session URL.

After any `git merge origin/main` in a lane, check `test ! -e .git/MERGE_HEAD` before the next
step; a merge inside a `||` fallback can leave a conflict in progress silently. The one file that
conflicts on every merge is `python/repark-parity/tests/map.md`, where each unit appends a CAP-1
ratchet line: keep both rows.

Follow-up brief = the preamble + "Findings from audit" list + "Do STEP n again" (or the next
step). Resume the same session (`--followup F --resume <id>` from `runs.tsv`); after two
resumes that still fail the same finding, close the lane and log it (§8).

## 5. Gate, push, PR, merge

Identity for every orchestrator commit (ledger `move`, departure edit):
`git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com commit …`
with **no trailer** and never the session URL anywhere.

```bash
cd /tmp/oc-$LANE && make preflight                      # alone; read its exit code
git fetch -q origin main && git merge-base --is-ancestor origin/main HEAD \
  && git push -q https://github.com/TRO-Wolf/repark.git $BR \
  && gh pr create --repo TRO-Wolf/repark --base main --head $BR --title "<type>(<unit>): <what>" --body-file /tmp/oc-worker/$LANE/pr.md
```

PR body: what, the clause table verdicts, the gates run and their counts, the ledger path, the
line `🤖 Generated with [Claude Code](https://claude.com/claude-code)`; no session URL.

Merge only under G-1, only on green, one gated chain, then the two side effects separately:

```bash
gh pr checks <n> --repo TRO-Wolf/repark --json bucket --jq 'all(.bucket=="pass")' | grep -q true \
  && git fetch -q origin main && git merge-base --is-ancestor origin/main HEAD \
  && gh pr merge <n> --repo TRO-Wolf/repark --squash --delete-branch --subject "<type>(<unit>): <what> (#<n>)" \
  && sleep 5 && M=$(gh api repos/TRO-Wolf/repark/commits/main --jq .sha) \
  && [ "$(gh api repos/TRO-Wolf/repark/commits/$M --jq .commit.tree.sha)" = "$(git rev-parse HEAD^{tree})" ] && echo TREE-EQUAL
~/.claude/skills/grok-worker/notify.sh "<unit> merged as <sha> — <one line>"     # separate command
git status --porcelain | wc -l                                                    # 0, then: rm -rf /tmp/oc-$LANE
```

Before the merge the unit's last commit carries the departure edit: ledger `move` to
`completed/` (`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`),
STATUS.md trued up for this unit alone, maps in lockstep. If the departure edit needs a
STATUS.md sentence you are unsure of, leave the ledger in `staging/` and say so in the PR body;
the owner finishes it.

## 6. Decision authority (G-2)

May decide alone, logging a new `D-n` row in the card and a line in the ledger:

- test names, fixture layout, file names inside the card's Home;
- filing a residue row when a measurement disagrees with a card's *expectation* but the card
  already says "measure" (the measurement wins; the card's guess was a guess);
- a follow-up step order inside one card;
- wording of docs the card names.

Must park the lane (leave the branch pushed, PR in draft, ledger clause `OPEN`, one line in §8):

- any public name or signature (`eager`, `compute`, config key names, SQL grammar accepted);
- any semantic change a ruling in the slate §0 or a D-row already fixed;
- a dependency beyond the card's seed commit;
- anything touching `.github/`, `repo-manifest.toml`, crate tiers, `briefs/next-sequence.md`
  ordering, or the live checkout;
- a card whose measurement step contradicts its D-2/D-1 families (SQL-DESCRIBE-1, PROFILES-1).

## 7. Order for one night

Runs 1–4 (2026-09-08/09) worked slate 1. Run 5 onward works
[cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md) (maintenance policy, torture
suite, Never-OOM) after finishing slate 1's leftovers. Two lanes at a time, each to its PR before
the next opens; a card whose steps are all merged is skipped:

1. Slate-1 leftovers, one lane, in this order: DF-EAGER-1 steps 2–3, CFG-1 steps 3–4,
   DISPLAY-BRIDGE-1, AP-0, PROFILES-1 steps 1–3 (Muse, release build, box alone), CONF-UNREAD-1.
2. MAINT-POLICY-1 steps 1–4 (three Muse rounds then GLM) in the other lane from the start.
3. TORTURE-1 steps 1–2 (GLM) when a lane frees, then steps 3–5 (Muse, Muse alone with the JVM,
   GLM).
4. NEVEROOM-1 step 1 (GLM); step 2 (Muse) only when no other lane is open; step 3 (GLM).
5. AP-1 (Muse, GLM) once AP-0 is on `main`.
6. **Grok lane**, third, from the start: the BALLISTA-M1-A seed (G-5), then M1-A steps 1–2,
   M1-B, M1-C, M1-D; between Ballista rounds, REVIEW-1 critic rounds two at a time over the
   merged units, reproductions re-run by you, fix cards appended.

## 8. Stop conditions and the morning report

Stop opening lanes when any of these fires; finish or park the open ones; write the report:

- the G-3 stop time or a rate-limit / usage-limit error on any tool result;
- `make preflight` red twice on the same branch;
- free space under 40 GB on `/` (`df -h /`), or a worker clone over 30 GB;
- a worker edits outside its Home twice;
- a merge chain fails after `gh pr merge` printed success (stop everything; the tree check is
  the owner's to read).

Morning report, both places, before the session ends:

1. Append to `~/.claude/projects/-home-john-CodeRepos-LocalRepark-repark/memory/campaign-state.md`:
   one dated line per lane (unit, step reached, PR number, merged sha or parked reason, rounds,
   worker cost from `runs.tsv`).
2. `task/roadmap/mid-term/overnight-report-<date>.md` on a docs branch (PR, not merged): the
   same table plus every decision taken under §6 and every parked question, one line each.
3. One Slack note through `notify.sh` with the count merged, the count parked, and the report PR.

## 9. Launching the overnight session (owner runs this)

From a plain shell, not inside a Claude session. The live checkout may sit on any branch with
uncommitted work, so the documents are read from a fresh clone of `main` while the working
directory stays the live checkout (that path is what loads the project memory and `CLAUDE.md`):

```bash
git clone -q https://github.com/TRO-Wolf/repark.git /tmp/repark-main
systemd-run --user --collect --quiet --unit="overnight-$(date -u +%Y%m%dT%H%M)" \
  --setenv=HOME=$HOME --setenv=PATH="$PATH" --setenv=USER=$USER \
  -p WorkingDirectory=$HOME/CodeRepos/LocalRepark/repark \
  -p StandardOutput=append:/tmp/overnight.log -p StandardError=append:/tmp/overnight.log \
  -- claude -p --model opus --effort medium --max-turns 400 --dangerously-skip-permissions \
     "Read /tmp/repark-main/task/roadmap/mid-term/overnight-orchestrator-runbook-2026-09-08.md and run it. The slate it names is in the same directory. Grants tonight: G-1 yes, G-2 yes, G-3 stop <time> local, G-4 GLM and Muse, G-5 yes. Start at §1. Never end a turn while a lane is running; wait in the foreground per §3."
```

Effort: `medium` for GLM-only nights (the audits are checklist work); `high` when G-4 includes
Muse tier-I rounds, whose diffs are Rust parsers and wiring. §1 step 5 forbids the session from
committing in the working directory. The owner reads `/tmp/overnight.log` and the report in the
morning. Cost is the session's own tokens: one audit per worker round is the unit of spend, so a
night of eight rounds is about eight audits plus the reads in §1.

## Pointers
- Up: [map.md](map.md) · The cards: [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md)
- Launchers: `~/.claude/skills/oc-worker/SKILL.md`, `~/.claude/skills/muse-worker/SKILL.md`, `~/.claude/skills/grok-worker/SKILL.md`
- Rules of the road: [../../../AGENTS.md](../../../AGENTS.md), [../../../docs/testing.md](../../../docs/testing.md), [../../../briefs/next-sequence.md](../../../briefs/next-sequence.md) standing rules
