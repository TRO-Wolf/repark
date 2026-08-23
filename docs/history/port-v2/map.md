# map — docs/history/port-v2/

## Purpose

The archived record of the **v1 → v2 port** (2026-08-06 → 2026-08-08, closed at milestone one):
what each phase was asked to do, what each unit actually delivered, and the audit proving that
archiving it lost no live rule. History, not law — the rules are [AGENTS.md](../../../AGENTS.md),
the current state is [STATUS.md](../../../STATUS.md).

Every file here carries a dated **ARCHIVED** banner. The directory is immutable except link repair
and dated corrections (see [README.md](README.md) "Rules for this directory").

## Contents

- [README.md](README.md) — what the port was, the source pin `fc3f48102`, how parity was verified
  (the four census cohorts and where the evidence lives), what each file here records, and which
  port-era decisions are still **current** ADRs.
- [promotion-ledger.md](promotion-ledger.md) — the lossless-archival audit: every rule in every file
  below, classified HOMED / PROMOTED / SUPERSEDED / HISTORICAL, with its authoritative home today,
  plus the thirteen promotions FD-4 landed — eleven before the `git mv`, two more found at its
  adversarial review — and the recorded residue. *(Count corrected 2026-08-10; it read "eleven",
  which is the pre-move subset, not the total the ledger states.)*
- **Execution briefs** (what a phase was asked to do): [phase-0-bootstrap.md](phase-0-bootstrap.md),
  [phase-1-engine-core.md](phase-1-engine-core.md), [phase-2-sql-doors.md](phase-2-sql-doors.md),
  [phase-3-python-facade.md](phase-3-python-facade.md).
- **Phase-1 unit ledgers:** [p1a-workspace-arming-ledger.md](p1a-workspace-arming-ledger.md)
  (workspace arming + `repark-common` + the first gates),
  [p1b-repark-iceberg-ledger.md](p1b-repark-iceberg-ledger.md) (the declared-rename catalog+write
  merge, the fork pin, the fork audit), [p1c-repark-core-ledger.md](p1c-repark-core-ledger.md)
  (the Session re-home, the four forced edits, the session-test audit).
- **Phase-2 unit ledgers:** [p2a-functions-ledger.md](p2a-functions-ledger.md),
  [p2b-spark-skeleton-ledger.md](p2b-spark-skeleton-ledger.md),
  [p2c-spark-ddl-ledger.md](p2c-spark-ddl-ledger.md),
  [p2d-spark-dml-ledger.md](p2d-spark-dml-ledger.md) (the Spark door, spine → DDL → DML + the
  334-name census close), [p2e-ta-ledger.md](p2e-ta-ledger.md) (`repark-ta` + `TaExtension`),
  [p2f-ansi-m1-ledger.md](p2f-ansi-m1-ledger.md) and
  [p2g-ansi-m2-ledger.md](p2g-ansi-m2-ledger.md) (the ANSI door M1/M2, the day-1 spikes, the
  surface matrices, the cross-door two-session protocol, the seam freeze).
- **Phase-3 unit ledgers:** [p3a-arming-ledger.md](p3a-arming-ledger.md),
  [p3b-ml-ledger.md](p3b-ml-ledger.md), [p3c-binding-ledger.md](p3c-binding-ledger.md) (the PyO3
  binding, its edit classes and provocation proofs), [p3d-parity-ledger.md](p3d-parity-ledger.md)
  (the parity package, the report comparator, the baseline),
  [p3e-facade-ledger.md](p3e-facade-ledger.md) (the facade wheel, the empirical deferral ledger,
  finding B-1), [p3f-tier2-ledger.md](p3f-tier2-ledger.md) (tier-2 CI + its security panel),
  [p3g-close-ledger.md](p3g-close-ledger.md) (the acceptance run = milestone one).
- [port-execution-log.md](port-execution-log.md) — the port's live tracker until it closed (was
  `task/todo.md`): the phase checklists and the three SEPMO retrospectives.

## I want to...

| ...do this | go to |
|---|---|
| Understand the port in one screen | [README.md](README.md) |
| Check where an archived rule lives now | [promotion-ledger.md](promotion-ledger.md) |
| See what a phase was asked to deliver | that phase's brief |
| See what a PR actually delivered, and at what cost | that unit's ledger |
| Read a phase retrospective | [port-execution-log.md](port-execution-log.md) |
| Find the census evidence itself (in history since 2026-08-23) | [../../port/census.md](../../port/census.md) §7 |
| Run or compare a census today | [../../port/census.md](../../port/census.md) |
| See the current backlog instead | [STATUS.md](../../../STATUS.md) |

## Pointers

- Up: [../map.md](../map.md)
- Plan of record for the port (still live): [../../port/PLAN.md](../../port/PLAN.md)
- Live acceptance inputs the archive refers to: [task/port/](../../../task/port/map.md) (deferred +
  added test ledgers); the recorded census runs: [../../port/census.md](../../port/census.md) §7.

## Debug

| Symptom | First check |
|---|---|
| A ledger contradicts the code | The code is truth; the ledger is dated. Do not "fix" the ledger to match — record the current fact in the live document that owns it |
| A cross-reference to `task/p*-ledger.md` or `briefs/phase-*.md` fails | Same basename, here (moved 2026-08-09); [task/map.md](../../../task/map.md) carries the redirect |
| A ledger says a rider is OPEN | Check [promotion-ledger.md](promotion-ledger.md) first — open riders were promoted to [STATUS.md](../../../STATUS.md) or a component contract; the ledger text is the original record |
| You need to change something here | Only link repair or a **dated** correction is allowed; anything else belongs in a current document |
