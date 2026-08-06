# map — task/

## Purpose

Session-start state: in-flight work and the DO / DO-NOT rules in force. Edit these as work
lands; per-unit ledgers accumulate here as units execute (same contract as the private v1
repository's `task/` directory).

## Contents

- [todo.md](todo.md) — the phase-0 unit in flight plus the phase 1–3 port backlog; execution
  state only (phase definitions live in [../docs/port/PLAN.md](../docs/port/PLAN.md)).
- [lessons.md](lessons.md) — DO / DO-NOT rules in force (append date-stamped; supersede, don't
  delete). Seeded 2026-08-06 from v1.

## I want to...

| ...do this | go to |
|---|---|
| See what's in flight | [todo.md](todo.md) |
| Check a rule before acting | [lessons.md](lessons.md) |
| Read the port plan behind the backlog | [../docs/port/PLAN.md](../docs/port/PLAN.md) |
| Read the brief driving phase 0 | [../briefs/phase-0-bootstrap.md](../briefs/phase-0-bootstrap.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../AGENTS.md](../AGENTS.md) (durable contract); these trackers are the moving state.
- Unit ledgers (one `<unit>-ledger.md` per delivered unit, with gate evidence and provocation
  proofs per [../docs/testing.md](../docs/testing.md)) join this directory from phase 1 on.

## Debug

- If work and trackers disagree, the code is truth — update the tracker.
- Stale checkboxes are a known failure mode (lessons.md, 2026-08-06): verify against source and
  git history before scoping from todo.md alone.
