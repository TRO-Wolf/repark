# map — docs/design/

## Purpose

Settled design documents — the deliberate design passes the port plan required before a phase's
code landed. A post-milestone **campaign** design lives here while its campaign runs and is archived
with that campaign when it closes. Each document is the record of a decided
design (provenance, forced-edit ledger, omissions ledger, revisit triggers), not a proposal;
changing a decision here means a new dated design pass, not an in-place edit.

## Contents

- [session-api.md](session-api.md) — the phase-1 repark-core Session API design (settled
  2026-08-06): the three-crate layout (`repark-common` / `repark-iceberg` / `repark-core`), the
  Session type and two-phase lifecycle, the internal engine API with the `SqlDialect` /
  `SessionExtension` seams, the `ExecutionBackend` boundary, the complete forced-edit ledger
  (§5), census accounting (§7), the omissions ledger (§8), and the server landing map (§6).
- [session-extension-conf-seam.md](session-extension-conf-seam.md) — the **superseding design
  note** (settled 2026-08-10) the 2026-08-08 seam freeze requires: `SessionExtension::configure`
  takes a `SessionBuildConf<'_>` (the builder conf map PLUS the values `build()` already resolved)
  rather than the bare map, so the validated session timezone reaches the function layer without a
  second resolution. Prices the break, records the two rejected alternatives, and states what
  stays frozen (`SqlDialect::execute`, `register`, session-scoped extensions).
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
- [v2-engine-hardening.md](v2-engine-hardening.md) — the **V2 Engine Hardening** campaign design
  (settled 2026-08-09, addendum 2026-08-10, landed at kickoff 2026-08-10): the campaign running
  now, and the first since the port to touch engine code. Goal and the five done-criteria (§1),
  the ground truth it is scoped against — the three known correctness issues, the verification
  surface, the already-existing performance instruments (§2) — the six phases H-0…H-5 (§3), the
  dated decisions D1–D9 (§4), the north-star workload W1 with its absolute sanitization rule (§5),
  the delegated external lanes (§6), the one held owner gate and the one discharged before kickoff
  (§7), non-goals (§8). Executed by the
  slate in [../../briefs/v2-engine-hardening.md](../../briefs/v2-engine-hardening.md).
- [product-contract.md](product-contract.md) — **product-honesty contracts** (settled 2026-08-11)
  for three consumer-facing statements (G3-E3 / G3-E4 / G3-E7): Catalog-API-only table
  introspection (`list_tables` supported; `SHOW TABLES IN` pinned-unimplemented ST-1; bare
  `SHOW TABLES` conf-gated), each `sql()` as one eager commit boundary with no multi-statement
  atomicity and no promised transaction API, and catalog visibility after DDL (list-on-access
  guarantees + free-SQL OOB residual). Every claim cites a real test or pinned refusal by name.

**A campaign design leaves this directory when its campaign closes.** The Agent-Agnostic Front-Door
design (settled 2026-08-08, implemented by FD-1…FD-5) moved to
[../history/frontdoor/agent-agnostic-frontdoor.md](../history/frontdoor/agent-agnostic-frontdoor.md)
at that campaign's close-out on 2026-08-10, with its slate and its unit ledger. The phase designs
and product-contract stay live because the engine still obeys them.

## I want to...

| ...do this | go to |
|---|---|
| Understand the phase-1 crate layout / Session API | [session-api.md](session-api.md) |
| See exactly which product-code edits the port makes | [session-api.md](session-api.md) §5 |
| Check why an improvement was deliberately resisted | [session-api.md](session-api.md) §8 |
| See why the frozen `SessionExtension` seam changed | [session-extension-conf-seam.md](session-extension-conf-seam.md) |
| Understand the phase-2 doors / ANSI rulings | [sql-doors.md](sql-doors.md) |
| Understand the phase-3 port / census gate / edit classes | [python-facade.md](python-facade.md) |
| Understand the V2 Engine Hardening campaign (running) | [v2-engine-hardening.md](v2-engine-hardening.md) |
| See what a hardening unit must do and how it is accepted | [../../briefs/v2-engine-hardening.md](../../briefs/v2-engine-hardening.md) |
| See the product contracts for list_tables / sql() boundaries / post-DDL visibility | [product-contract.md](product-contract.md) |
| Understand the Agent-Agnostic Front-Door campaign | [../history/frontdoor/README.md](../history/frontdoor/README.md) (archived 2026-08-10) |
| Read the brief that executed the phase-1 design | [docs/history/port-v2/phase-1-engine-core.md](../history/port-v2/phase-1-engine-core.md) |
| Read the brief that executed the phase-2 design | [docs/history/port-v2/phase-2-sql-doors.md](../history/port-v2/phase-2-sql-doors.md) |
| Read the brief that executed the phase-3 design | [docs/history/port-v2/phase-3-python-facade.md](../history/port-v2/phase-3-python-facade.md) |
| See the port phases the design fits into | [../port/PLAN.md](../port/PLAN.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../adr/map.md](../adr/map.md) records the load-bearing "why" decisions these designs
  build on (fork ownership, two doors, copy-then-re-home, server-prep disciplines).

## Debug

First checks: if a design clause and ported code disagree, the design's forced-edit ledger (§5)
is the complete list of intentional deltas — anything outside it is a port defect, not a design
change. Escalate to: [../map.md#debug](../map.md).
