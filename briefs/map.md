# map — briefs/

## Purpose

Version-controlled slate briefs — the hand-off documents that drive delegated-agent work units.
Each brief carries the unit definitions, design pre-decisions, STOP gates, and (where one
happened) the greylight Q&A appendix. The repo copy is canonical once a slate is handed off, so
campaign history survives any single machine and PR reviewers can read the brief that produced a
branch. (Same contract as the private v1 repository's `briefs/` directory.)

## Contents

- [frontdoor-campaign.md](frontdoor-campaign.md) — the Agent-Agnostic Front-Door campaign slate
  (2026-08-08): the first post-milestone campaign — five independently mergeable, behavior-preserving
  units (FD-1 truthful front door / FD-2 neutral contract / FD-3 mechanize structure / FD-4 reduce
  doc weight / FD-5 seam honesty), executing the settled design in
  [../docs/design/agent-agnostic-frontdoor.md](../docs/design/agent-agnostic-frontdoor.md);
  documentation + mechanical-gate work only, no engine code change.

**Closed campaigns are archived, not kept here.** The four port briefs (`phase-0`…`phase-3`,
2026-08-06 → 2026-08-08) moved to
[../docs/history/port-v2/](../docs/history/port-v2/map.md) on 2026-08-09 with the unit ledgers they
drove; this directory holds the slates of campaigns that are still running.

## I want to... → go to

| I want to... | go to |
|---|---|
| See what a delivered unit was ASKED to do | the dated brief for its slate — in this directory while the campaign runs, in [../docs/history/](../docs/history/map.md) once it closes |
| Find the standing rules briefs inherit | [../AGENTS.md](../AGENTS.md) "Delegated-agent standing rules" |
| Write a new brief | copy the newest brief's structure; standing rules by reference, not restatement |
| Read the port briefs (phases 0–3) | [../docs/history/port-v2/README.md](../docs/history/port-v2/README.md) |
| See the port phases those briefs executed against | [../docs/port/PLAN.md](../docs/port/PLAN.md) |
| Read the design the Front-Door campaign implements | [../docs/design/agent-agnostic-frontdoor.md](../docs/design/agent-agnostic-frontdoor.md) |
| Read the settled designs the port phases implemented | [../docs/design/map.md](../docs/design/map.md) |

## Pointers

- Up: [../map.md](../map.md)
- Briefs are operational documents, subordinate to the engineering contracts (the precedence chain
  in [../AGENTS.md](../AGENTS.md) "Precedence"). A brief may narrow scope; it never relaxes a
  contract rule.
- **Import gate:** anything added here must pass the repository's forbidden-content greps (no
  account ids, ARNs, bucket names, credentials, personal identifiers, local absolute paths,
  session identifiers). Env-var NAMES are fine; values never. Execution-local appendices are
  stripped before import.

## Debug

- A brief referencing a rule that contradicts [../AGENTS.md](../AGENTS.md): the contract wins
  (SEPMO doctrine D1 — surface the conflict, don't silently follow the brief).
- A brief's execution details seem missing: execution-local appendices (paths, grep lists) are
  deliberately stripped from repo copies; the decision record, if any, lives with the
  orchestrating session, not here.
