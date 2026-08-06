# map — briefs/

## Purpose

Version-controlled slate briefs — the hand-off documents that drive delegated-agent work units.
Each brief carries the unit definitions, design pre-decisions, STOP gates, and (where one
happened) the greylight Q&A appendix. The repo copy is canonical once a slate is handed off, so
campaign history survives any single machine and PR reviewers can read the brief that produced a
branch. (Same contract as the private v1 repository's `briefs/` directory.)

## Contents

- [phase-0-bootstrap.md](phase-0-bootstrap.md) — the phase-0 bootstrap brief (2026-08-06):
  gates before code — testing contract, mechanical gates, map.md discipline, agent contracts,
  SEPMO, tier-1 CI, ported and green on an empty workspace; five workstreams, five commits,
  panel verification. In-repo copy ends above the execution-local appendix by design.

## I want to... → go to

| I want to... | go to |
|---|---|
| See what a delivered unit was ASKED to do | the dated brief for its slate |
| Find the standing rules briefs inherit | [../AGENTS.md](../AGENTS.md) "Delegated-agent standing rules" |
| Write a new brief | copy the newest brief's structure; standing rules by reference, not restatement |
| See the port phases a brief executes against | [../docs/port/PLAN.md](../docs/port/PLAN.md) |

## Pointers

- Briefs are operational documents, subordinate to the engineering contracts
  ([CLAUDE.md](../CLAUDE.md) precedence chain). A brief may narrow scope; it never
  relaxes a contract rule.
- **Import gate:** anything added here must pass the repository's forbidden-content greps (no
  account ids, ARNs, bucket names, credentials, personal identifiers, local absolute paths,
  session identifiers). Env-var NAMES are fine; values never. Execution-local appendices are
  stripped before import.

## Debug

- A brief referencing a rule that contradicts AGENTS.md/CLAUDE.md: the contract wins
  (SEPMO doctrine D1 — surface the conflict, don't silently follow the brief).
- A brief's execution details seem missing: execution-local appendices (paths, grep lists) are
  deliberately stripped from repo copies; the decision record, if any, lives with the
  orchestrating session, not here.
