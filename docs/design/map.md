# map — docs/design/

## Purpose

Settled design documents — the deliberate design passes the port plan required before a phase's
code landed, plus the post-milestone campaign designs. Each document is the record of a decided
design (provenance, forced-edit ledger, omissions ledger, revisit triggers), not a proposal;
changing a decision here means a new dated design pass, not an in-place edit.

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
- [python-facade.md](python-facade.md) — the phase-3 Python binding + facade + census design
  (settled 2026-08-08, competition-synthesized): census-first verbatim port, the ten edit
  classes (§3), the Q1–Q10 rulings incl. the deferred `repark.sql` re-home with its
  release-prep gate (§4), the three hard findings handled (§5), the end-to-end census +
  acceptance procedure with the stability run and the report comparator (§6), the CI delta
  incl. the net-new tier-2 live-AWS design (§7), and the seven-PR slate (§9).
- [agent-agnostic-frontdoor.md](agent-agnostic-frontdoor.md) — the Agent-Agnostic Front-Door
  campaign design (settled 2026-08-08): the first post-milestone campaign — documentation +
  mechanical-gate work only, no engine-behavior change. The disposition of all 10 proposal
  recommendations (§3), the pivotal authority decision (§4, Option A: neutral `AGENTS.md`, thin
  `CLAUDE.md` adapter), non-goals (§5), the checkable definition of success (§6), and the
  lossless-archival reconciliation identity (§7). Executed by the FD-1…FD-5 slate in
  [../../briefs/frontdoor-campaign.md](../../briefs/frontdoor-campaign.md).

## I want to...

| ...do this | go to |
|---|---|
| Understand the phase-1 crate layout / Session API | [session-api.md](session-api.md) |
| See exactly which product-code edits the port makes | [session-api.md](session-api.md) §5 |
| Check why an improvement was deliberately resisted | [session-api.md](session-api.md) §8 |
| Understand the phase-2 doors / ANSI rulings | [sql-doors.md](sql-doors.md) |
| Understand the phase-3 port / census gate / edit classes | [python-facade.md](python-facade.md) |
| Understand the Agent-Agnostic Front-Door campaign | [agent-agnostic-frontdoor.md](agent-agnostic-frontdoor.md) |
| Read the brief that executes the phase-1 design | [../../briefs/phase-1-engine-core.md](../../briefs/phase-1-engine-core.md) |
| Read the brief that executes the phase-2 design | [../../briefs/phase-2-sql-doors.md](../../briefs/phase-2-sql-doors.md) |
| Read the brief that executes the phase-3 design | [../../briefs/phase-3-python-facade.md](../../briefs/phase-3-python-facade.md) |
| See the port phases the design fits into | [../port/PLAN.md](../port/PLAN.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../adr/map.md](../adr/map.md) records the load-bearing "why" decisions these designs
  build on (fork ownership, two doors, copy-then-re-home, server-prep disciplines).

## Debug

First checks: if a design clause and ported code disagree, the design's forced-edit ledger (§5)
is the complete list of intentional deltas — anything outside it is a port defect, not a design
change. Escalate to: [../map.md#debug](../map.md).
