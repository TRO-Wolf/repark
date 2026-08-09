# map — task/

## Purpose

Working state for **current** work: the rules in force, the ledger of each unit in flight, and the
acceptance inputs that gates still read. Finished campaigns do not accumulate here — they are
archived under [../docs/history/](../docs/history/map.md) once their rules have been promoted to a
current document.

Current state (release, delivered surface, what happens next) is **[../STATUS.md](../STATUS.md)**,
not this directory.

## Contents

- [todo.md](todo.md) — a **pointer only**: the live backlog is [../STATUS.md](../STATUS.md), and a
  unit's working plan is its own ledger. The file keeps its name because live code, docs and one
  runtime error message cite this path.
- [lessons.md](lessons.md) — DO / DO-NOT rules in force (append date-stamped; supersede, don't
  delete). Seeded 2026-08-06 from the private v1 repository.
- [fd3-ledger.md](fd3-ledger.md) — unit ledger for **FD-3** of the Agent-Agnostic Front-Door
  campaign (mechanize structural truth: `repo-manifest.toml` + the `check_manifest` validator,
  the crate-DAG upgrade to explicit allowed edges with dependency kinds, and the manifest↔map
  consistency rule): scope, design decisions, gate results, and the provocation proofs for both
  new/upgraded gates.
- [port/](port/map.md) — **live acceptance inputs**: the deferred-test manifest and its
  reconciliation rule ([port/deferred-tests.md](port/deferred-tests.md)), the machine-readable
  deferral allowlist ([port/deferred-python-tests.txt](port/deferred-python-tests.txt)) and its
  mirror additions ledger ([port/added-python-tests.txt](port/added-python-tests.txt)). The census
  comparator still subtracts these, so they are not history.
- [census/](census/map.md) — **evidence**: the recorded census runs, `baseline-fc3f48102/` (the port
  pin) and `v2-a5be8a7/` (the acceptance run). Never hand-edited; a re-run replaces a whole
  directory in one commit.

## Where the port ledgers went

The seventeen `p1*` / `p2*` / `p3*` unit ledgers, the four phase briefs and the port's `todo.md`
execution log moved to [../docs/history/port-v2/](../docs/history/port-v2/map.md) on **2026-08-09**
(Front-Door FD-4), keeping their basenames. A citation of `task/p3e-facade-ledger.md` — a few
survive in Rust doc comments — means
[../docs/history/port-v2/p3e-facade-ledger.md](../docs/history/port-v2/p3e-facade-ledger.md), and so
on. Nothing was lost in the move; the audit is
[../docs/history/port-v2/promotion-ledger.md](../docs/history/port-v2/promotion-ledger.md).

## I want to...

| ...do this | go to |
|---|---|
| See the live backlog / what happens next | [../STATUS.md](../STATUS.md) |
| Check a rule before acting | [lessons.md](lessons.md) |
| Start a new unit's ledger | copy the shape of [fd3-ledger.md](fd3-ledger.md); link it from this map in the same commit |
| See why a dependency edge or a manifest field is gated, and the proofs it fires | [fd3-ledger.md](fd3-ledger.md) |
| See which v1 tests are deferred, and why | [port/deferred-tests.md](port/deferred-tests.md) |
| Feed the census comparator its allowlists | [port/map.md](port/map.md) |
| Run or compare a census | [../docs/port/census.md](../docs/port/census.md) |
| Read the port's record (briefs, unit ledgers, retrospectives) | [../docs/history/port-v2/README.md](../docs/history/port-v2/README.md) |
| Read the port plan the phases executed | [../docs/port/PLAN.md](../docs/port/PLAN.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../AGENTS.md](../AGENTS.md) (the durable contract) and [../STATUS.md](../STATUS.md)
  (current state); this directory holds the moving parts of work in flight.
- Unit ledgers: one `<unit>-ledger.md` per delivered unit, with gate evidence and provocation proofs
  per [../docs/testing.md](../docs/testing.md), linked from this map in the same commit. When a
  campaign closes, its ledgers are archived under [../docs/history/](../docs/history/map.md) after a
  promotion audit — never deleted.

## Debug

- `pg-integration-report.md` may appear here untracked: `python/repark/tests/test_pg_acceptance.py`
  writes it (CWD-relative) on every facade run. It is gitignored on purpose — a run output, not a
  record. Do not `git add` it.
- If work and trackers disagree, the code is truth — update the tracker.
- A link into `task/p*-ledger.md` fails: see "Where the port ledgers went" above.
- Looking for a backlog item that is not in [../STATUS.md](../STATUS.md)? Check
  [../docs/history/port-v2/promotion-ledger.md](../docs/history/port-v2/promotion-ledger.md) — if it
  was live at archival, that table says where it went.
