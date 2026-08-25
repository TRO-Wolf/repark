# map — docs/history/

## Purpose

The **archive**: completed campaigns **and mid-campaign phase promotions**, kept for provenance and
deliberately off the normal read path. Material lands here only after a promotion audit proves that
every rule it carries also lives in a current document — so nothing here has to be read in order to
work in this repository.

Current state is [STATUS.md](../../STATUS.md); the rules are [AGENTS.md](../../AGENTS.md).

## Contents

- [port-v2/](port-v2/map.md) — the v1 → v2 port (2026-08-06 → 2026-08-08, closed at milestone one):
  the four phase briefs, the seventeen unit ledgers, the port execution log with its three
  retrospectives, and the [promotion ledger](port-v2/promotion-ledger.md) that made the archival
  lossless. Start at [port-v2/README.md](port-v2/README.md).
- [frontdoor/](frontdoor/map.md) — the Agent-Agnostic Front-Door campaign (2026-08-08 → 2026-08-10,
  the first post-milestone campaign, closed on its own acceptance items): the settled design, the
  FD-1…FD-5 slate, the one unit ledger the campaign filed, and the
  [retrospective](frontdoor/retrospective.md) whose "Promotion check" section is this archival's
  lossless audit. Start at [frontdoor/README.md](frontdoor/README.md).
- [iceberg-maintenance-wave/](iceberg-maintenance-wave/map.md) — the Iceberg write-path
  maintenance wave (2026-08-21 → 2026-08-23, closed by MW-5): the design, the slate, and
  the promotion check. Unit ledgers stay in [task/ledgers/](../../task/ledgers/map.md).
  Start at [iceberg-maintenance-wave/README.md](iceberg-maintenance-wave/README.md).
- [hardening-h1/](hardening-h1/map.md) — V2 Engine Hardening **H-1 phase** (mid-campaign promotion
  2026-08-11, G-9): the ten unit ledgers delivered through the H-1 close gate (repark #35–#46),
  including the parallel G/N corpus units, plus `g4-artifacts/` and the
  [promotion ledger](hardening-h1/promotion-ledger.md). The campaign continues into H-2. Start at
  [hardening-h1/README.md](hardening-h1/README.md).
- [pyc/](pyc/map.md) — **Python convention conformance (PYC)**: the workstream's STATUS record, cut 2026-08-22 (closed by #216).
- [lrs/](lrs/map.md) — **Low-risk sweep (LRS)**: the workstream's STATUS record, cut 2026-08-21 (closed by #191).

## I want to...

| ...do this | go to |
|---|---|
| Know the current state / what happens next | [../../STATUS.md](../../STATUS.md) |
| Understand how the engine got here | [port-v2/README.md](port-v2/README.md) |
| Understand how the front door got here | [frontdoor/README.md](frontdoor/README.md) |
| Read the H-1 phase record (mid-campaign) | [hardening-h1/README.md](hardening-h1/README.md) |
| Read the Iceberg maintenance-wave record | [iceberg-maintenance-wave/README.md](iceberg-maintenance-wave/README.md) |
| Check that an archived rule still binds — and where it lives now | [port-v2/promotion-ledger.md](port-v2/promotion-ledger.md) · [frontdoor/retrospective.md](frontdoor/retrospective.md) "Promotion check" · [hardening-h1/promotion-ledger.md](hardening-h1/promotion-ledger.md) · [iceberg-maintenance-wave/README.md](iceberg-maintenance-wave/README.md) "Promotion check" |
| Find a unit's gate evidence / provocation proofs / census arithmetic | the unit's ledger in [port-v2/](port-v2/map.md), [frontdoor/](frontdoor/map.md), or [hardening-h1/](hardening-h1/map.md) |
| See what a closed campaign cost, caught and missed | that campaign's retrospective + [../../task/metrics.md](../../task/metrics.md) |
| Archive a completed campaign or closed phase | run its promotion audit first, then `git mv` its briefs and designs into a new `docs/history/<campaign>/` with its own `README.md` + `map.md`. Its unit ledgers are **not** moved: since DL-1 (2026-08-23) they are already in [../../task/ledgers/archive/](../../task/ledgers/archive/map.md) by merge month, and the campaign folder links to them there |

## Pointers

- Up: [../map.md](../map.md)
- Archived material is **immutable** except link repair and dated corrections; archived status claims
  carry an effective date. The full rule set is in
  [port-v2/README.md](port-v2/README.md) "Rules for this directory".
- Evidence that is still an **input** to a gate does not belong here: the deferred/added test
  ledgers ([task/port/](../../task/port/map.md)) stay live. The recorded census runs were evicted
  from the tree on 2026-08-23 and are reachable by SHA ([docs/port/census.md](../port/census.md) §7).

## Debug

| Symptom | First check |
|---|---|
| A rule seems to exist only in an archived file | [port-v2/promotion-ledger.md](port-v2/promotion-ledger.md) or [hardening-h1/promotion-ledger.md](hardening-h1/promotion-ledger.md) names its current home; if it does not, that is a real gap — fix the current document, never the archive |
| A link into `task/p*-ledger.md` or `briefs/phase-*.md` does not resolve | Those moved here on 2026-08-09 (same basename) — see [port-v2/README.md](port-v2/README.md) "Where the ledgers used to live" |
| A link into `briefs/frontdoor-campaign.md`, `docs/design/agent-agnostic-frontdoor.md` or `task/fd3-ledger.md` does not resolve | Those moved to [frontdoor/](frontdoor/map.md) on 2026-08-10 (same basename) |
| A link into `task/h1*-ledger.md`, `task/g*-ledger.md`, `task/n2-merge-ledger.md`, or `task/g4-artifacts/` does not resolve | Those moved to [hardening-h1/](hardening-h1/map.md) on 2026-08-11 (same basename; mid-campaign) |
| An archived claim contradicts today's behavior | The archive is dated; [STATUS.md](../../STATUS.md) wins |
