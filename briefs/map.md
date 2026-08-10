# map — briefs/

## Purpose

Version-controlled slate briefs — the hand-off documents that drive delegated-agent work units.
Each brief carries the unit definitions, design pre-decisions, STOP gates, and (where one
happened) the greylight Q&A appendix. The repo copy is canonical once a slate is handed off, so
campaign history survives any single machine and PR reviewers can read the brief that produced a
branch. (Same contract as the private v1 repository's `briefs/` directory.)

## Contents

**No campaign is running, so this directory holds only this map** (2026-08-10). That is the
steady state between campaigns, not a gap: a slate lands here when its campaign starts and leaves
when the campaign closes.

**Closed campaigns are archived, not kept here.** The four port briefs (`phase-0`…`phase-3`,
2026-08-06 → 2026-08-08) moved to [../docs/history/port-v2/](../docs/history/port-v2/map.md) on
2026-08-09 with the unit ledgers they drove; the Agent-Agnostic Front-Door slate
(`frontdoor-campaign.md`, 2026-08-08 → 2026-08-10) moved to
[../docs/history/frontdoor/](../docs/history/frontdoor/map.md) at that campaign's close-out, with
its design and its one unit ledger. Where the next campaign stands is
[../STATUS.md](../STATUS.md) "Active workstreams".

## I want to... → go to

| I want to... | go to |
|---|---|
| See what a delivered unit was ASKED to do | the dated brief for its slate — in this directory while the campaign runs, in [../docs/history/](../docs/history/map.md) once it closes |
| Find the standing rules briefs inherit | [../AGENTS.md](../AGENTS.md) "Delegated-agent standing rules" |
| Write a new brief | copy the newest archived brief's structure; standing rules by reference, not restatement |
| Read the port briefs (phases 0–3) | [../docs/history/port-v2/README.md](../docs/history/port-v2/README.md) |
| See the port phases those briefs executed against | [../docs/port/PLAN.md](../docs/port/PLAN.md) |
| Read the Front-Door campaign's slate, design and retrospective | [../docs/history/frontdoor/README.md](../docs/history/frontdoor/README.md) |
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
