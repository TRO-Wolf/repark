---
name: compact-context-docs
description: >-
  Run the post-landing truth-up that keeps RePark's context documents lean,
  current, and single-homed: reconcile STATUS.md to what actually merged,
  replace restatements elsewhere with pointers, sweep stale lifecycle claims
  ("not yet", "planned", old phase numbers), keep every touched map.md in
  lockstep, archive closed campaigns to docs/history/, and validate with
  make ci. Use this skill after a unit, campaign, or release lands, or when a
  stale claim is spotted in a document agents are expected to trust — and also
  as the PICKUP RITUAL at the start of a unit, scoped to the delta that just
  merged. The goal is compaction, not deletion — a closed campaign's record
  moves to the archive; truth is never simply removed.
---

# Skill: compact-context-docs — true up and compress the context documents

An agent-facing runbook for the post-landing ritual that keeps the repository's context
documents lean, current, and single-homed. Run it after a unit, campaign, or release lands —
or whenever a stale claim is spotted in a document an agent is expected to trust. It defines
no policy; on any conflict the spine wins: [AGENTS.md](../../../AGENTS.md) (precedence — in
particular the single-home rule) and [STATUS.md](../../../STATUS.md) (the single source of truth
for current state).

**The goal is compaction, not deletion.** A closed campaign's record moves to the archive
(`docs/history/`); truth is never simply removed. The live documents shrink because history
leaves them, not because it disappears.

## What counts as a context document

- **[STATUS.md](../../../STATUS.md)** — release state, delivery, active/deferred workstreams.
  Every other document that mentions state must *point here*, never restate it.
- **Every touched directory's `map.md`** — navigation truth, kept in lockstep by
  `scripts/check_map_md.sh` (the pre-commit oracle).
- **`task/`** — the rules in force (`lessons.md`), the metrics ledger (`metrics.md`), and the
  per-unit ledgers, filed by state under [task/ledgers/](../../../task/ledgers/map.md)
  (`staging/` in flight → `completed/` frozen → `archive/yyyy-mm/` immutable; the directory is
  the status, and `scripts/ledger_lifecycle.py` is the only thing that moves them).
- **`briefs/`** — slate briefs for *running* campaigns only. A closed campaign's slate is
  archived with it under `docs/history/`.
- **`repo-manifest.toml`** — the validated structural mirror (`make check-manifest`).
- **Docs with lifecycle claims** — any doc that says "not wired yet", "phase N", "arrives
  with X", or carries a date. These rot silently once X ships.

## The ritual

0. **File the finished ledgers — mechanically, before reading anything.** `make ledger-archive`
   moves every `task/ledgers/completed/` ledger to `archive/yyyy-mm/` under its merge date,
   rewrites every link to it across the tree and stages the result; it costs no tokens and is
   idempotent. Nothing in this step needs judgement, so nothing in it should be done by hand.
1. **True up STATUS.md first.** Read what actually landed (the merged PRs, the shipped tag),
   then rewrite the affected STATUS.md sections to the new truth and refresh the last-updated
   stamp. Everything downstream reconciles *to* STATUS.md.
2. **Sweep for restatements.** Any other doc restating what STATUS.md now says gets its
   restatement replaced with a pointer. One home per fact — a fact stated twice is a fact
   that will drift.
3. **Sweep for stale lifecycle claims.** Search the docs for language the landing falsified —
   "not yet", "planned", "when it exists", "deferred", old phase numbers, superseded dates —
   and fix each occurrence in its authoritative home.
4. **map.md lockstep.** Every directory whose contents changed gets its `map.md` updated in
   the same commit. New directory → new `map.md` + a Contents row in the parent's map.
5. **Archive what closed.** The unit's own ledger leaves `staging/` in the departure commit:
   `python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`
   (a charter stays in `staging/` until the event it names has happened). A finished campaign's
   brief and design move to `docs/history/` per [docs/history/map.md](../../../docs/history/map.md),
   linking to the campaign's ledgers in the monthly archive rather than carrying them; its
   cost/caught/missed row lands in `task/metrics.md`; `briefs/` keeps only running campaigns
   (or only its map).
6. **Validate mechanically.** `make ci` — the manifest checker, the map oracle and
   `check-ledgers` turn structural drift into a red gate. Fix red before opening the PR.
7. **One PR, normal review.** Doc truth-ups follow the same PR discipline as code: full CI,
   content hygiene, owner merge. Never fold an unrelated doc sweep into a feature PR.

## Pickup ritual (scoped mode)

The same ritual runs at the *start* of a unit, not only after one lands — a unit that begins on a
stale base spends its first hour disbelieving the documents. Scoped mode is the whole ritual with
one narrow subject:

1. **Confirm the base.** Fetch, confirm the prior unit's PR actually merged, and confirm the local
   base contains that unit's departure edit (the slate/brief row it removed or rewrote, and its
   ledger in `task/ledgers/completed/`). If either is missing, stop and rebase before anything else.
2. **File the finished ledgers.** `make ledger-archive` (step 0 above) — the prior unit's ledger
   takes its archive name from the merge date, so this runs only after the merge is on `main`.
3. **Run the drift checks.** `make check-map-sync` (map links), `make check-ledgers` and `make ci`
   as usual — structural drift is read from a gate, not from memory.
4. **Scope the compaction to the just-merged delta only.** Steps 1–5 above apply to what that PR
   changed and to nothing else.
5. **Land it as a docs-only first commit** on the new branch, before any code of the unit.

The scoped ritual is narrow enough to delegate: a smaller model can run it end to end provided
the orchestrator reviews the docs-only first commit before any code of the unit lands. Which tier
is a tool mechanic per [AGENTS.md](../../../AGENTS.md) "Delegated work", not a rule stated here.

**This mode is not a license for a general doc rewrite.** A stale claim outside the merged delta is
noted for its own unit; widening the scope here buries the unit's real diff under a doc sweep, which
the ritual's step 7 already forbids.

## Gotchas

- The most dangerous stale doc is the one an agent reads *instead of* the code — runbooks and
  onboarding docs rot faster than architecture docs because nothing mechanical validates their
  prose claims. Step 3 exists for them.
- "Compacting" STATUS.md by deleting a deferred-work item loses the only record that the
  deferral was deliberate. Move it or keep it; never silently drop it.
- Adapter files (this directory included) must stay pointer-thin. If a truth-up is about to
  add a project fact to an adapter, the fact belongs in the spine and the adapter gets a
  pointer.
