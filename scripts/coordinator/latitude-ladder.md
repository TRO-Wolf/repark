# Latitude ladder — what an orchestrator may do unprompted, batched, or never

Purpose: the explicit autonomy ladder for any orchestrator of this campaign's lanes.

Source: work order ORCHESTRATOR-DOCTRINE-1 (owner-accepted proposal, 2026-09-26); every ruling
below is final per that order. Related: `handbook.md` (hard rules), `bands.md` (executor bands),
`lane-contract.md` (the lane interface), `rulings-2026-09-26.md` (the dated rulings this ladder
applies).

## Tier 1 — do unprompted

An orchestrator takes these actions on its own authority and records them in the lane state or
ledger. Dated rulings, 2026-09-26.

- Relaunch a merge driver after a GitHub transient: "no checks reported", a TLS timeout, or checks
  not yet registered.
- Resume a Muse lane whose round exited without a hand-back, with a FINISH order on the same
  session id.
- Rebase a queued branch when main moved.
- Rule a worker HALT whose lean stays inside the work order; record the ruling in the ledger.
- Merge a fork PR whose only red is the accepted class (see `rulings-2026-09-26.md`), after reading
  the job log.
- Dispatch the one scoped Opus-medium verifier on a product-Rust PR.
- Fold S2/S3 findings via a guided Muse round.

## Tier 2 — batch for owner approval

List these, then act on a "go". Never take them on Tier-1 authority.

- New work orders that widen a unit's scope.
- Carve-outs of a parity cell (grammar in `rulings-2026-09-26.md`).
- Opus design rounds.
- Any AWS spend.
- Variant sizing.

## Tier 3 — always stop

Never do these; stop and escalate.

- A tag, version bump, or release PR.
- Editing STATUS.md in a run (the handbook's hard rule agrees).
- Working around a classifier denial.
- Touching the owner's live checkout (the handbook's hard rule agrees).
- Fork Cargo.toml edits (except ORC/rustls).
- Raising a file-size ceiling.
- Spend past the weekly threshold.

## Usage-threshold table — PROPOSED (owner to confirm)

The numbers below are the owner's to confirm; they are not rules until confirmed.

| Weekly Claude usage | Orchestrator posture |
|---|---|
| 8 % | Conservation plan: Opus executors design-only, verifiers scoped, no S2/S3 Opus fixers. |
| 18 % | Overnight Muse-only; orchestrator event-driven, at most 15 turns/h; no Opus design rounds until the morning re-evaluation. |
| 25 % | Switch to the distributed tick-driven coordinator (`start.sh`) on non-Claude engines. |
| 50 % | Claude orchestrator reads only; all execution on Muse/Grok/GLM. |

Usage comes from the transcript breakdown (`claude_usage.py` in this directory) plus each
engine's own budget; see `orchestrator-adapter-muse.md`.
