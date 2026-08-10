# The Agent-Agnostic Front-Door campaign — the archived record

**Archived 2026-08-10** (campaign close-out). Everything in this directory is **history**: it
records the first campaign this repository ran after the port, between 2026-08-08 and 2026-08-10.
It is deliberately **off the normal read path** and is **not a source of live rules** — see
[retrospective.md](retrospective.md) "Promotion check" for the audit that proves it.

**Current state lives in [STATUS.md](../../../STATUS.md); the rules live in
[AGENTS.md](../../../AGENTS.md).** If this directory and a current document disagree, the current
document wins and the disagreement is a bug in the current document, not a rule hidden here.

## What the campaign was

Milestone one left a repository whose front door still described a port in flight, whose
authoritative contract was duplicated across two files and named a specific tool, and whose
structural facts — which crates exist, which layer each sits in, which dependency edges are legal —
lived only in prose that nothing checked. The campaign's object was to make the repository legible
and safely modifiable by **any** contributor, human or automated, without depending on a tool name,
a model name, or porting-era vocabulary.

- **Five sequenced units**, each independently mergeable, each leaving `main` green:
  FD-1 one truthful front door · FD-2 the neutral contributor interface · FD-3 mechanize structural
  truth · FD-4 reduce active documentation weight · FD-5 seam honesty + the deferred refactor.
- **Documentation and mechanical-gate work only.** Across 140 changed files, **zero non-comment
  lines of engine code** changed.
- **Merged 2026-08-09** (#24, #25, #26, #28, #29), inside a 23-hour window; **closed 2026-08-10**,
  after the close-out unit discharged the two acceptance items that were still unmet at the fifth
  merge.

What it left behind, all of it live: [STATUS.md](../../../STATUS.md) as the single status source of
truth; [AGENTS.md](../../../AGENTS.md) as the single vendor-neutral authoritative contract, with
thin adapters that carry no facts and therefore cannot drift;
[ARCHITECTURE.md](../../../ARCHITECTURE.md) and [DEVELOPMENT.md](../../../DEVELOPMENT.md);
[repo-manifest.toml](../../../repo-manifest.toml) with its validator; an explicit allowed-edge
dependency policy; a `## Component contract` section in all nine crate-root maps; and the port's
own record archived at [../port-v2/README.md](../port-v2/README.md).

## What lives here

| File | What it records |
|---|---|
| [agent-agnostic-frontdoor.md](agent-agnostic-frontdoor.md) | The settled design (2026-08-08): the disposition of all ten proposal recommendations, the pivotal authority ruling (§4), the non-goals, the checkable definition of success (§6), and the lossless-archival reconciliation identity (§7). |
| [frontdoor-campaign.md](frontdoor-campaign.md) | The execution slate: what each of FD-1…FD-5 was **asked** to do, and the acceptance gate each was scored against. |
| [fd3-ledger.md](fd3-ledger.md) | The one unit ledger the campaign filed — FD-3's design decisions, gate results and ten provocation proofs. That only one of five units filed a ledger is itself a finding; see the retrospective. |
| [retrospective.md](retrospective.md) | The campaign retrospective: the per-unit scorecard, the acceptance items assessed against the tree, what the adversarial pass caught and what escaped, the cold-read trial transcript, the promotion check for this archival, and the feed-forward proposals. |

The quantitative half of the retrospective — the eight-metric ledger — is **live**, not archived:
[task/metrics.md](../../../task/metrics.md), because later campaigns append to it.

## Rules for this directory

The same four that govern [../port-v2/](../port-v2/README.md), restated here only because a reader
may land here first:

1. **Immutable**, with exactly two exceptions: link repair, and a **dated** correction that is
   labelled as one. Nothing here is silently rewritten to match today.
2. **Every status claim carries its effective date.** A slate that said "In-repo campaign slate"
   when it was written now says what it became, with the date it became it.
3. **Current documents link here only where provenance matters** — never to state a rule.
4. The universal `map.md` discipline holds inside the archive: this directory carries
   [map.md](map.md), as does [docs/history/](../map.md).

One thing to read with those rules in mind: the slate's orchestration section names model tiers and
tool roles. Those are **process notes about how this campaign was drawn up**, marked as such in the
slate itself, and they were never project rules — making the project's authoritative surface
tool-neutral was this campaign's own object.
