# map — task/

## Purpose

Session-start state: in-flight work and the DO / DO-NOT rules in force. Edit these as work
lands; per-unit ledgers accumulate here as units execute (same contract as the private v1
repository's `task/` directory).

## Contents

- [todo.md](todo.md) — the phase-1 unit in flight plus the phase 2–3 port backlog; execution
  state only (phase definitions live in [../docs/port/PLAN.md](../docs/port/PLAN.md)).
- [lessons.md](lessons.md) — DO / DO-NOT rules in force (append date-stamped; supersede, don't
  delete). Seeded 2026-08-06 from v1.
- [p1b-repark-iceberg-ledger.md](p1b-repark-iceberg-ledger.md) — unit ledger for phase-1 PR-B
  (repark-iceberg declared-rename unit: fidelity + census evidence, forced-edit class 6 spans,
  fork-audit findings, deny/audit restorations).
- [p1a-workspace-arming-ledger.md](p1a-workspace-arming-ledger.md) — unit ledger for phase-1
  PR-A (workspace arming + repark-common + gate arming): scope, commit plan, gate results,
  provocation proofs.
- [port/](port/map.md) — port-execution accounting
  ([port/deferred-tests.md](port/deferred-tests.md): the deferred-test manifest and its
  reconciliation rule).

## I want to...

| ...do this | go to |
|---|---|
| See what's in flight | [todo.md](todo.md) |
| Check a rule before acting | [lessons.md](lessons.md) |
| See PR-A's gate evidence / provocation proofs | [p1a-workspace-arming-ledger.md](p1a-workspace-arming-ledger.md) |
| See PR-B's fidelity / census / fork-audit evidence | [p1b-repark-iceberg-ledger.md](p1b-repark-iceberg-ledger.md) |
| See which v1 tests are deferred | [port/deferred-tests.md](port/deferred-tests.md) |
| Read the port plan behind the backlog | [../docs/port/PLAN.md](../docs/port/PLAN.md) |
| Read the brief driving phase 1 | [../briefs/phase-1-engine-core.md](../briefs/phase-1-engine-core.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../AGENTS.md](../AGENTS.md) (durable contract); these trackers are the moving state.
- Unit ledgers: one `<unit>-ledger.md` per delivered unit, with gate evidence and provocation
  proofs per [../docs/testing.md](../docs/testing.md), linked from this map in the same commit.

## Debug

- If work and trackers disagree, the code is truth — update the tracker.
- Stale checkboxes are a known failure mode (lessons.md, 2026-08-06): verify against source and
  git history before scoping from todo.md alone.
