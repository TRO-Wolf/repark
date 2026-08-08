# map — task/

## Purpose

Session-start state: in-flight work and the DO / DO-NOT rules in force. Edit these as work
lands; per-unit ledgers accumulate here as units execute (same contract as the private v1
repository's `task/` directory).

## Contents

- [todo.md](todo.md) — the phase-2 slate in flight plus the phase-3 and post-milestone-one
  backlog; execution state only (phase definitions live in
  [../docs/port/PLAN.md](../docs/port/PLAN.md)).
- [lessons.md](lessons.md) — DO / DO-NOT rules in force (append date-stamped; supersede, don't
  delete). Seeded 2026-08-06 from v1.
- [p1b-repark-iceberg-ledger.md](p1b-repark-iceberg-ledger.md) — unit ledger for phase-1 PR-B
  (repark-iceberg declared-rename unit: fidelity + census evidence, forced-edit class 6 spans,
  fork-audit findings, deny/audit restorations).
- [p1a-workspace-arming-ledger.md](p1a-workspace-arming-ledger.md) — unit ledger for phase-1
  PR-A (workspace arming + repark-common + gate arming): scope, commit plan, gate results,
  provocation proofs.
- [p2a-functions-ledger.md](p2a-functions-ledger.md) — unit ledger for phase-2 PR-1
  (repark-functions verbatim port + phase-2 docs): scope, declared edit classes, the 62-test
  identity-map census obligation; IN FLIGHT.
- [p3a-arming-ledger.md](p3a-arming-ledger.md) — unit ledger for phase-3 PR-1 (design + brief
  in-repo, crate-DAG tier-4 pre-declaration, rust CI job split, testing.md row-2 note,
  dialect doc rider): scope, required-check transition, provocation proofs.
- [p3b-ml-ledger.md](p3b-ml-ledger.md) — unit ledger for phase-3 PR-2 (`repark-ml` verbatim
  port + workspace wiring + the EC-7 map rewrite): scope, the 34-test identity census with its
  empty `--list` diff, gate results; IN FLIGHT.
- [p2b-spark-skeleton-ledger.md](p2b-spark-skeleton-ledger.md) — unit ledger for phase-2 PR-2
  (repark-spark skeleton: spine port, temporary refuse arms, `SparkDialect`/`SparkExtension`
  seams, G8 subquery-guard pin, deferred-#1 landing; census PARTIAL — closes PR-3b); IN
  FLIGHT.
- [p2c-spark-ddl-ledger.md](p2c-spark-ddl-ledger.md) — unit ledger for phase-2 PR-3a
  (repark-spark DDL restoration: ctas/create_table/alter/namespace_ddl handlers, catalog_ops
  TRIM restoration, refuse-arm replacement, deferred rows #2/#4–#7 landing; staged census —
  closes PR-3b); IN FLIGHT.
- [p2d-spark-dml-ledger.md](p2d-spark-dml-ledger.md) — unit ledger for phase-2 PR-3b
  (repark-spark DML + refs: merge/insert_overwrite/ref_ddl/call, MoR-valve hoist to
  repark-iceberg, lib-root battery move-only port, 334-name census CLOSE, deferred row #3);
  IN FLIGHT.
- [p2f-ansi-m1-ledger.md](p2f-ansi-m1-ledger.md) — unit ledger for phase-2 PR-5 (the ANSI
  door, milestone 1: NEW code — no census; `AnsiDialect` + guard set + wrong-door sniff + the
  curated `WITH (…)` vocabulary + Q15 routing; the R1/R2 day-1 spikes; the Q13 surface registry
  and both doors' matrix row counts); IN FLIGHT.
- [p2g-ansi-m2-ledger.md](p2g-ansi-m2-ledger.md) — unit ledger for phase-2 PR-6 (the ANSI
  door, milestone 2 — the door CLOSES: ALTER/MERGE/time-travel/branch-tag DDL/the refuse set;
  the repark-core R2 config fix that unblocks Q8 introspection; the Q11 TA toll; the Q13/G5
  two-session cross-door protocol; the `session-api.md` seam freeze and the ADR-0002
  design-pass discharge; final matrix counts for BOTH doors); IN FLIGHT.
- [p2e-ta-ledger.md](p2e-ta-ledger.md) — unit ledger for phase-2 PR-4 (repark-ta: verbatim
  crate port incl. the 148 `.bin` goldens, NEW `TaExtension`, `SparkExtension` composition
  restoring the p2b TA-omission rider, two-pass TA census, deferred rows #8–#14 landing —
  manifest remainder 4); IN FLIGHT.
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
| See the phase-2 PR-1 scope / edit classes | [p2a-functions-ledger.md](p2a-functions-ledger.md) |
| See the phase-2 PR-2 scope / refuse-arm riders | [p2b-spark-skeleton-ledger.md](p2b-spark-skeleton-ledger.md) |
| See the phase-2 PR-3a restoration checklist | [p2c-spark-ddl-ledger.md](p2c-spark-ddl-ledger.md) |
| See the phase-2 PR-3b census close / exclusions | [p2d-spark-dml-ledger.md](p2d-spark-dml-ledger.md) |
| See the phase-2 PR-5 design-ruling application / spike results / surface-matrix counts | [p2f-ansi-m1-ledger.md](p2f-ansi-m1-ledger.md) |
| See the phase-2 PR-4 TA census / rider discharge | [p2e-ta-ledger.md](p2e-ta-ledger.md) |
| See the phase-2 PR-6 per-Q delivery record / cross-door session profiles / final matrix counts | [p2g-ansi-m2-ledger.md](p2g-ansi-m2-ledger.md) |
| Find out why `information_schema` used to be off, and what fixed it | [p2g-ansi-m2-ledger.md](p2g-ansi-m2-ledger.md) "The R2 core fix" |
| See the phase-3 PR-2 `repark-ml` identity census / map-rewrite rationale | [p3b-ml-ledger.md](p3b-ml-ledger.md) |
| Read the brief driving phase 3 | [../briefs/phase-3-python-facade.md](../briefs/phase-3-python-facade.md) |
| Read the brief driving phase 2 | [../briefs/phase-2-sql-doors.md](../briefs/phase-2-sql-doors.md) |
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
