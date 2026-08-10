# map — docs/history/frontdoor/

## Purpose

The archived record of the **Agent-Agnostic Front-Door campaign** (2026-08-08 → 2026-08-10, the
first campaign after milestone one): the design it executed, the slate that carved it into five
units, the one unit ledger it filed, and the retrospective that closed it. History, not law — the
rules are [AGENTS.md](../../../AGENTS.md), the current state is [STATUS.md](../../../STATUS.md).

Every file here carries a dated **ARCHIVED** banner. The directory is immutable except link repair
and dated corrections (see [README.md](README.md) "Rules for this directory").

## Contents

- [README.md](README.md) — what the campaign was, what it left behind (all of it live), what each
  file here records, and the rules that govern this directory.
- [agent-agnostic-frontdoor.md](agent-agnostic-frontdoor.md) — the settled design (2026-08-08):
  the ten proposal dispositions (§3), the pivotal authority ruling (§4 — one neutral authoritative
  contract, thin adapters), the non-goals (§5), the eight checkable success items (§6, with their
  dated outcome), and the lossless-archival reconciliation identity (§7).
- [frontdoor-campaign.md](frontdoor-campaign.md) — the FD-1…FD-5 execution slate: each unit's
  goal, edits, verification depth and acceptance gate, plus the campaign's own "what done means"
  clause and its dated discharge.
- [fd3-ledger.md](fd3-ledger.md) — the FD-3 unit ledger: the manifest + crate-DAG gate work, eight
  design decisions, the gate table, ten provocation proofs (including the two that closed
  demonstrated bypasses), and the dated final disposition of its four open items.
- [retrospective.md](retrospective.md) — the campaign retrospective: per-unit scorecard, the eight
  acceptance items assessed against the tree, what the adversarial pass caught and what escaped,
  the cold-read trial transcript, the promotion check that made this archival lossless, the lesson
  candidates, and the feed-forward proposals.

The retrospective's **metrics** are not here — they live in the appendable ledger
[task/metrics.md](../../../task/metrics.md), because later campaigns add to it.

## I want to...

| ...do this | go to |
|---|---|
| Understand the campaign in one screen | [README.md](README.md) |
| See what a unit was ASKED to do, and its acceptance gate | [frontdoor-campaign.md](frontdoor-campaign.md) |
| See why authority moved to one neutral contract | [agent-agnostic-frontdoor.md](agent-agnostic-frontdoor.md) §4 |
| Check that an archived rule still binds — and where it lives now | [retrospective.md](retrospective.md) "Promotion check" |
| See a mechanical gate's provocation proofs | [fd3-ledger.md](fd3-ledger.md) |
| See what the campaign cost, caught and missed | [retrospective.md](retrospective.md) + [task/metrics.md](../../../task/metrics.md) |
| Read the cold-read trial of the five-hop read path | [retrospective.md](retrospective.md) §3 item 8 |
| See the current state instead | [STATUS.md](../../../STATUS.md) |
| Read the port's archive (the campaign before this one) | [../port-v2/README.md](../port-v2/README.md) |

## Pointers

- Up: [../map.md](../map.md)
- The read path this campaign built and signposted is
  [README.md](../../../README.md) → [STATUS.md](../../../STATUS.md) →
  [ARCHITECTURE.md](../../../ARCHITECTURE.md) → [DEVELOPMENT.md](../../../DEVELOPMENT.md) →
  [AGENTS.md](../../../AGENTS.md). Nothing on it routes here except where provenance is the point.
- The gates this campaign armed are live and owned elsewhere:
  [repo-manifest.toml](../../../repo-manifest.toml) + `scripts/check_manifest.py`, and the
  allowed-edge table inside `scripts/check_crate_dag.py` ([scripts/map.md](../../../scripts/map.md)).

## Debug

| Symptom | First check |
|---|---|
| An archived claim contradicts today's behavior | The archive is dated; [STATUS.md](../../../STATUS.md) wins, and the contradiction is a bug in the current document |
| A rule seems to exist only in a file here | [retrospective.md](retrospective.md) "Promotion check" names its current home; if it does not, that is a real gap — fix the current document, never the archive |
| A link into `briefs/frontdoor-campaign.md`, `docs/design/agent-agnostic-frontdoor.md` or `task/fd3-ledger.md` fails | Same basename, here (moved 2026-08-10); [task/map.md](../../../task/map.md) carries the redirect |
| You need to change something here | Only link repair or a **dated** correction is allowed; anything else belongs in a current document |
