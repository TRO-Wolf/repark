# map — task/port/

## Purpose

Port-execution accounting artifacts — the working ledgers the copy-then-re-home port maintains
across phases (as opposed to the plan itself, which lives in
[../../docs/port/PLAN.md](../../docs/port/PLAN.md)).

## Contents

- [deferred-tests.md](deferred-tests.md) — the deferred-test manifest for the phase-1 cone: every
  v1 test not yet ported, with its target phase, under the hard reconciliation rule
  (ported ∪ deferred) = v1 totals at the pinned SHA. PR-B/PR-C fill the per-crate sections.

## I want to...

| ...do this | go to |
|---|---|
| See which v1 tests are deferred and to what phase | [deferred-tests.md](deferred-tests.md) |
| Read the census/relocation rules the manifest serves | [../../docs/design/session-api.md](../../docs/design/session-api.md) §7 + [../../docs/testing.md](../../docs/testing.md) "Relocation discipline" |
| Read the port plan / phases | [../../docs/port/PLAN.md](../../docs/port/PLAN.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: per-unit ledgers live in [../](../map.md) (one `<unit>-ledger.md` per delivered unit).

## Debug

- Manifest and `cargo test --workspace -- --list` disagree: the reconciliation rule in
  [deferred-tests.md](deferred-tests.md) is the gate — fix the manifest or the port, never the
  rule. Escalate to: [../map.md#debug](../map.md).
