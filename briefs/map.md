# map — briefs/

## Purpose

Version-controlled slate briefs — the hand-off documents that drive delegated-agent work units.
Each brief carries the unit definitions, design pre-decisions, STOP gates, and (where one
happened) the greylight Q&A appendix. The repo copy is canonical once a slate is handed off, so
campaign history survives any single machine and PR reviewers can read the brief that produced a
branch. (Same contract as the private v1 repository's `briefs/` directory.)

## Contents

- [v2-engine-hardening.md](v2-engine-hardening.md) — the **V2 Engine Hardening** slate
  (2026-08-10, running): the campaign's per-unit definitions and acceptance gates — H-1's four
  correctness units (the divergence registry, session timezone, the time-travel view leak, the
  `$`-metadata ruling), H-2's parity deepening with the ranked gap list it clears, H-3's
  performance instrument, baseline and spill matrix, H-4's evidence-only optimization rules, and
  H-5's verification close. Executes the design in
  [../docs/design/v2-engine-hardening.md](../docs/design/v2-engine-hardening.md). The first slate
  since the port whose units change engine code, so the testing contract is restated as binding
  and every unit declares its verification panel. **Amended in flight** (2026-08-10, H-1d's fix
  pass): H-1b's edit list presupposed a registry row H-1d's own admission rule forbade it to write
  (an issue with no disposition and no pin gets no row), so that line now tells H-1b to *create*
  the row if the re-port leaves a residual difference — the conflict and its resolution are
  recorded in [../task/h1d-ledger.md](../task/h1d-ledger.md). A brief is amended in place, dated
  and traceable, never silently rewritten.

A slate lands here when its campaign starts and leaves when the campaign closes; between
campaigns this directory holds only its map.

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
| Read the running campaign's slate | [v2-engine-hardening.md](v2-engine-hardening.md) |
| See what the running campaign DECIDED and why | [../docs/design/v2-engine-hardening.md](../docs/design/v2-engine-hardening.md) |
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
