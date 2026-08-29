# map — docs/design/

## Purpose

Settled design documents — the deliberate design passes the port plan required before a phase's
code landed. A post-milestone **campaign** design lives here while its campaign runs and is archived
with that campaign when it closes. Each document is the record of a decided
design (provenance, forced-edit ledger, omissions ledger, revisit triggers), not a proposal;
changing a decision here means a new dated design pass, not an in-place edit.

## Contents

- [format-v3-track.md](format-v3-track.md) — **the format-v3 track's scope audit
  (2026-08-21; §5 the delivery sequence, Steps 1–2 done 2026-08-28, RP-3 SHA frozen):** what roadmap item A12 got wrong once the surfaces were actually run. The engine
  already reads Spark-written deletion vectors and appends to a v3 table with correct row
  lineage, both verified by round trip; `rewrite_data_files` silently reassigned that lineage,
  which is why the audit ships a guard. Carries the `system.register_table` signature read from
  the Iceberg jar (§4), the revised six-unit slate (§5), the two fork items the track needs (§6),
  and §7 — what was measured and what is not claimed. **V3-1 (2026-08-21):** §4's
  `V3-ADOPT-1` is an admitted registry row, not queued; the CALL write names the Hadoop
  convention. §5 and §7 name the Spark-written fixture as landed.
  **Errata 2026-08-24 (MW-7):** §3b's v2 sentence ("Spark … leaving all six position deletes in
  place") holds for that 9 %-deleted fixture and is not general — on delete-heavy v2 shapes Spark
  ends at zero delete files. Registry `RDF-1`.

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
  **PYC-3 (2026-08-22):** dated footnote — pydantic v2 is a second wheel hard dep.
- [spark-function-parity.md](spark-function-parity.md) — the **Spark function parity** campaign
  design (settled 2026-08-20): close the `pyspark.sql.functions` gap and move the semantics behind
  every name out of Python into Rust. Goal and the three done-criteria (§1), the measured ground
  truth — surface, classification, kernel ownership, and the two-door asymmetry that became clause
  C-012 (§2) — the higher-order/lambda seam with its per-function cost table (§3), the
  repatriation model and the 55 non-compliant names (§4), decisions D-1…D-6 (§5), risks R-1…R-5
  (§6), the unit roster and 2026-08-28 per-unit delivery order (§7), the owner ruling on the four sub-project families
  (§8), and non-goals (§9). CAP-1 points its live file-size premise at the source guards while
  preserving the dated kickoff measurement. Evidence:
  [task/fnp-0-census/](../../task/fnp-0-census/map.md).
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
at that campaign's close-out on 2026-08-10, with its slate and its unit ledger. The Iceberg
write-path maintenance wave (settled 2026-08-21, MW-0…MW-5) moved to
[../history/iceberg-maintenance-wave/](../history/iceberg-maintenance-wave/README.md) on
2026-08-23. The phase designs and product-contract stay live because the engine still obeys them.

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
| Understand the Iceberg maintenance wave | [../history/iceberg-maintenance-wave/README.md](../history/iceberg-maintenance-wave/README.md) (archived 2026-08-23) |
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
