# Skill: compact-context-docs — true up and compress the context documents

An agent-facing runbook for the post-landing ritual that keeps the repository's context
documents lean, current, and single-homed. Run it after a unit, campaign, or release lands —
or whenever a stale claim is spotted in a document an agent is expected to trust. It defines
no policy; on any conflict the spine wins: [AGENTS.md](../../AGENTS.md) (precedence — in
particular the single-home rule) and [STATUS.md](../../STATUS.md) (the single source of truth
for current state).

**The goal is compaction, not deletion.** A closed campaign's record moves to the archive
(`docs/history/`); truth is never simply removed. The live documents shrink because history
leaves them, not because it disappears.

## What counts as a context document

- **[STATUS.md](../../STATUS.md)** — release state, delivery, active/deferred workstreams.
  Every other document that mentions state must *point here*, never restate it.
- **Every touched directory's `map.md`** — navigation truth, kept in lockstep by
  `scripts/check_map_md.sh` (the pre-commit oracle).
- **`task/`** — the rules in force (`lessons.md`), the metrics ledger (`metrics.md`), and the
  per-unit ledgers of work in flight.
- **`briefs/`** — slate briefs for *running* campaigns only. A closed campaign's slate is
  archived with it under `docs/history/`.
- **`repo-manifest.toml`** — the validated structural mirror (`make check-manifest`).
- **Docs with lifecycle claims** — any doc that says "not wired yet", "phase N", "arrives
  with X", or carries a date. These rot silently once X ships.

## The ritual

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
5. **Archive what closed.** A finished campaign's brief and working record move to
   `docs/history/` per [docs/history/map.md](../../docs/history/map.md); its cost/caught/missed
   row lands in `task/metrics.md`; `briefs/` keeps only running campaigns (or only its map).
6. **Validate mechanically.** `make ci` — the manifest checker and map oracle turn structural
   drift into a red gate. Fix red before opening the PR.
7. **One PR, normal review.** Doc truth-ups follow the same PR discipline as code: full CI,
   content hygiene, owner merge. Never fold an unrelated doc sweep into a feature PR.

## Gotchas

- The most dangerous stale doc is the one an agent reads *instead of* the code — runbooks and
  onboarding docs rot faster than architecture docs because nothing mechanical validates their
  prose claims. Step 3 exists for them.
- "Compacting" STATUS.md by deleting a deferred-work item loses the only record that the
  deferral was deliberate. Move it or keep it; never silently drop it.
- Adapter files (this directory included) must stay pointer-thin. If a truth-up is about to
  add a project fact to an adapter, the fact belongs in the spine and the adapter gets a
  pointer.
