# map — docs/history/hardening-h1/

## Purpose

Archived unit ledgers for the **H-1 phase** of the V2 Engine Hardening campaign (and the
parallel G/N corpus units delivered through the same close gate), promoted mid-campaign on
**2026-08-11** by G-9. History, not law — the rules are [AGENTS.md](../../../AGENTS.md); the
current state is [STATUS.md](../../../STATUS.md). The campaign itself is **not** closed.

The lossless audit is [promotion-ledger.md](promotion-ledger.md). Start there.

## Contents

- [README.md](README.md) — what this archive is (mid-campaign), where to start, the rules.
- [promotion-ledger.md](promotion-ledger.md) — the promotion audit (still-live carries named;
  ghost fix; g4-artifacts map.md gap note).
- [h1d-ledger.md](h1d-ledger.md) — H-1d divergence registry unit.
- [h1a-ledger.md](h1a-ledger.md) — H-1a session-timezone (both splits) + § Split B extraction fix.
- [h1c-ledger.md](h1c-ledger.md) — H-1c `$`-metadata introspection rider (ADR-0006).
- [h1b-ledger.md](h1b-ledger.md) — H-1b time-travel ephemeral-view leak fix.
- [g4-tests-split-ledger.md](g4-tests-split-ledger.md) — G-4 `tests.rs` → `tests/` split.
- [g4-artifacts/](g4-artifacts/map.md) — G-4 identity-gate evidence (before/after lists, name map, logs).
- [g5-sweep-ledger.md](g5-sweep-ledger.md) — G-5 registry sweep.
- [g6-chores-ledger.md](g6-chores-ledger.md) — G-6 hardening chores.
- [g7-decimal-ledger.md](g7-decimal-ledger.md) — G-7 decimal128 differential corpus (Python half).
- [n2-merge-ledger.md](n2-merge-ledger.md) — N-2 / H-2 gap G3 MERGE INTO differential corpus.
- [g8-file-size-ledger.md](g8-file-size-ledger.md) — G-8 general Rust file-size gate.

## I want to...

| ...do this | go to |
|---|---|
| Confirm the archival was lossless | [promotion-ledger.md](promotion-ledger.md) |
| See how a divergence gets declared, pinned and mirrored | [h1d-ledger.md](h1d-ledger.md) |
| Read why the session timezone is a build-time knob | [h1a-ledger.md](h1a-ledger.md) |
| Read the extraction fix and what it deliberately did NOT close | [h1a-ledger.md](h1a-ledger.md) "§ Split B" |
| See how an open question gets FIXED instead of declared | [h1c-ledger.md](h1c-ledger.md) + [../../adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](../../adr/0006-hide-iceberg-metadata-tables-from-enumeration.md) |
| Find out why a `__repark_tt_*` name is on a session | [h1b-ledger.md](h1b-ledger.md) |
| Re-run the G-4 identity gate against the name map | [g4-artifacts/map.md](g4-artifacts/map.md) |
| Read the MERGE INTO differential corpus ledger | [n2-merge-ledger.md](n2-merge-ledger.md) |
| Read the decimal128 corpus ledger + paste-true rows | [g7-decimal-ledger.md](g7-decimal-ledger.md) |
| See the current campaign state instead | [STATUS.md](../../../STATUS.md) |
| Read the port's archive / front-door archive | [../port-v2/README.md](../port-v2/README.md) · [../frontdoor/README.md](../frontdoor/README.md) |

## Pointers

- Up: [../map.md](../map.md)
- Every file here carries a dated ARCHIVED banner (2026-08-11, G-9). Immutable except link repair
  and dated corrections.
- Metrics for the campaign stay in the live [task/metrics.md](../../../task/metrics.md) ledger
  (append-only; not archived with this phase).

## Debug

| Symptom | First check |
|---|---|
| A rule seems to exist only in an archived H-1 ledger | [promotion-ledger.md](promotion-ledger.md) names its current home |
| A link into `task/h1*-ledger.md` / `task/g*-ledger.md` / `task/n2-merge-ledger.md` fails | Those moved here on 2026-08-11 (same basename) — see [../../task/map.md](../../../task/map.md) "Where the closed campaigns' ledgers went" |
| Looking for H-2 work | Not here — H-2 unit ledgers re-accumulate under [task/](../../../task/map.md) |
