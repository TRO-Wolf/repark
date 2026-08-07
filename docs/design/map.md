# map — docs/design/

## Purpose

Settled design documents — the deliberate design passes the port plan requires before a phase's
code lands. Each document is the record of a decided design (provenance, forced-edit ledger,
omissions ledger, revisit triggers), not a proposal; changing a decision here means a new dated
design pass, not an in-place edit.

## Contents

- [session-api.md](session-api.md) — the phase-1 repark-core Session API design (settled
  2026-08-06): the three-crate layout (`repark-common` / `repark-iceberg` / `repark-core`), the
  Session type and two-phase lifecycle, the internal engine API with the `SqlDialect` /
  `SessionExtension` seams, the `ExecutionBackend` boundary, the complete forced-edit ledger
  (§5), census accounting (§7), the omissions ledger (§8), and the server landing map (§6).
- [sql-doors.md](sql-doors.md) — the phase-2 two-SQL-doors design (settled 2026-08-07):
  delegate-first architecture (verbatim Spark-door port, NEW ANSI door), the four tier-3
  crates + three hoists (§1), the Q1–Q15 ANSI rulings (§2), the seam freeze (§3), census +
  matrix testing discipline (§4), the sequencing fidelity gate (§5), and top risks (§6).

## I want to...

| ...do this | go to |
|---|---|
| Understand the phase-1 crate layout / Session API | [session-api.md](session-api.md) |
| See exactly which product-code edits the port makes | [session-api.md](session-api.md) §5 |
| Check why an improvement was deliberately resisted | [session-api.md](session-api.md) §8 |
| Understand the phase-2 doors / ANSI rulings | [sql-doors.md](sql-doors.md) |
| Read the brief that executes the phase-1 design | [../../briefs/phase-1-engine-core.md](../../briefs/phase-1-engine-core.md) |
| Read the brief that executes the phase-2 design | [../../briefs/phase-2-sql-doors.md](../../briefs/phase-2-sql-doors.md) |
| See the port phases the design fits into | [../port/PLAN.md](../port/PLAN.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../adr/map.md](../adr/map.md) records the load-bearing "why" decisions these designs
  build on (fork ownership, two doors, copy-then-re-home, server-prep disciplines).

## Debug

First checks: if a design clause and ported code disagree, the design's forced-edit ledger (§5)
is the complete list of intentional deltas — anything outside it is a port defect, not a design
change. Escalate to: [../map.md#debug](../map.md).
