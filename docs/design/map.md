# map — docs/design/

## Purpose

Settled design documents — the deliberate design passes the port plan required before a phase's
code landed. A post-milestone **campaign** design lives here while its campaign runs and is archived
with that campaign when it closes. Each document is the record of a decided
design (provenance, forced-edit ledger, omissions ledger, revisit triggers), not a proposal;
changing a decision here means a new dated design pass, not an in-place edit.

## Contents

- [sql-harden-cutover-matrix.md](sql-harden-cutover-matrix.md) — **SQL-HARDEN-1 (2026-09-04):**
  the cutover pipeline cutover shapes S1–S7 measured on memory Spark vs repark — 9 programs, 0 EQUAL,
  9 DIVERGES, four registry rows filed, `V3-COV-7` cited. Harness:
  `python/repark/tests/test_sql_harden_cutover.py`. Critic round 2: S6 names follow the
  passed namespace; DATE-FN-1 incidental pins. pins: sql-harden-1-cutover-shapes/C-004
- [v3-statement-coverage.md](v3-statement-coverage.md) — **V3-COV (2026-09-03):** the v3
  statement-coverage matrix that discharges the north star's §2 pillar 4 — 81 statement programs
  over 12 statement classes and all seven `CALL system.*` procedures, 267 comparison cells,
  72 EQUAL, 1 refused by both engines, 8 rows filed, 2 defects FIXED in the same unit. Harness:
  `python/repark/tests/test_v3_statement_coverage.py`. Read it before adding a statement surface:
  a new statement class that is not in §3 is not covered, whatever the nightly legs say.
- [format-v3-track.md](format-v3-track.md) — **the format-v3 track's scope audit
  (2026-08-21; §5 the delivery sequence, Steps 1–3 done 2026-08-30, RP-3 consumed at `d408da42`;
  Step 4 V3-3 keep-refusal dated 2026-08-30; Step 5 V3-5 `V3-DANGLE-1` FIXED 2026-08-31):** what roadmap item A12 got wrong once the surfaces were actually run. The engine
  already reads Spark-written deletion vectors and appends to a v3 table with correct row
  lineage, both verified by round trip; `rewrite_data_files` silently reassigned that lineage,
  which is why the audit ships a guard. Carries the `system.register_table` signature read from
  the Iceberg jar (§4), the revised six-unit slate (§5), the two fork items the track needs (§6),
  and §7 — what was measured and what is not claimed. **V3-1 (2026-08-21):** §4's
  `V3-ADOPT-1` is FIXED (RP-3 / fork #235, 2026-08-30): Hadoop `vN` writes bump to `v(N+1)`.
  S3 Tables `register_table` is registry `S3T-1` (fork R126). §5 and §7 name the Spark-written
  fixture as landed.
  **Corrected 2026-09-02 (LIVE-v3):** §7's "`expire_snapshots` … was not exercised against a
  table with expirable snapshots" now carries its dated fix — the live v3 leg's created-table
  sequence expires thirteen of fourteen snapshots (`14 → 1`), pinned by
  `python/repark/tests/test_v3_acceptance_local.py`. pins: live-v3-aws-legs/C-004
  **Corrected 2026-09-02 (LIVE-v3-M):** §7's "Nothing was measured on Glue or S3 Tables" carries
  its dated fix too — `aws-acceptance` run 33635288918 measured both, registry `S3T-V3-1`.
  pins: live-v3-first-measurement/C-002
  **Errata 2026-08-24 (MW-7):** §3b's v2 sentence ("Spark … leaving all six position deletes in
  place") holds for that 9 %-deleted fixture and is not general — on delete-heavy v2 shapes Spark
  ends at zero delete files (errata 2026-09-02: true at 4.0.1 / 1.10.0; at 1.11.0 Spark leaves the
  delete file dangling, F-RDF1-1). Registry `RDF-1` (FIXED 2026-09-02 for a delete file naming one
  data file; a delete file naming several is the residue).

- [v1-0-api-review-2026-09-02.md](v1-0-api-review-2026-09-02.md) — **the v1.0 API review packet
  (2026-09-02, base `3eb6b71`):** one row per public surface — 35 rows covering all 913 public
  Python names, the 50 dialect-neutral door surfaces on both matrices, the seven `CALL`
  procedures, the conf keys, the format-v3 opt-in, packaging and the error taxonomy — each with
  its pins, its measured example coverage, the divergence-registry rows that land inside it, and a
  freeze recommendation the owner answers `yes` / `no` / `yes except <members>`. Recommends YES on
  15 rows, YES-except on 15, NO on 5; all 81 open registry rows map to exactly one surface. Rows
  also as [v1-0-api-review-2026-09-02.json](v1-0-api-review-2026-09-02.json) for the status board.
  **Answered 2026-09-02 (API-FREEZE):** the owner ruled `R0 yes` with the board's wording and every
  row's **decision equals its recommendation**, so the packet carries a `decision` column (and the
  JSON a `decision` field) and the Counts table carries the decided totals — 30 frozen rows, 5
  pre-stable, 888 registered names. §5's "unwritten" row is discharged: the additive-only and
  major-version rules are now [../release.md](../release.md) "Versioning policy".
  pins: api-freeze/C-001, C-002
  Reviews the ruling in [python-facade.md](python-facade.md) §4; the gate that calls for it is
  [../../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md) §3.

- [v1-0-api-freeze.json](v1-0-api-freeze.json) — **the frozen-surface register (2026-09-02,
  API-FREEZE):** every row of the packet with its decision, `frozen` flag, the members the
  decision leaves pre-stable, and — for the 30 frozen rows — the exact frozen names: 781 Python
  members (650 carrying the required-parameter list an AST walk can read), 85 door surfaces with
  their per-door disposition, 5 conf keys, 6 packaging facts, 11 error classes. Generated by
  `python3 ../../scripts/build_api_freeze.py --write` and pinned by
  `python/repark-parity/tests/test_api_freeze.py`; the rule it enforces is
  [../release.md](../release.md) "Versioning policy". Regenerate it in the same change as any
  intended additive move. pins: api-freeze/C-003

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
  F-Y10-1 (2026-08-30): `repark-sql` also depends on `repark-functions` for AnsiDialect
  session-build integer overflow; still no `datafusion-spark`.
- [python-facade.md](python-facade.md) — the phase-3 Python binding + facade + census design
  (settled 2026-08-08, competition-synthesized): census-first verbatim port, the ten edit
  classes (§3), the Q1–Q10 rulings incl. the deferred `repark.sql` re-home with its
  release-prep gate (§4), the three hard findings handled (§5), the end-to-end census +
  acceptance procedure with the stability run and the report comparator (§6), the CI delta
  incl. the net-new tier-2 live-AWS design (§7), and the seven-PR slate (§9).
  **PYC-3 (2026-08-22):** dated footnote — pydantic v2 is a second wheel hard dep.
- [spark-function-parity.md](spark-function-parity.md) — the **Spark function parity** campaign.
  F-Y10-1 closed 2026-08-30; FNP-7b is unblocked. SMALLINT/Int16 wrap is a dated
  residue (2026-08-30). **2026-08-31:** FNP-7 ships; `try_avg(INTERVAL)` is deferred to
  FNP-11 (registry BL-13). Remaining order is FNP-9/10 → FNP-8 → FNP-11/12 → FNP-Z.
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
- [torture-suite.md](torture-suite.md) — the **v1.2 (was v0.8) torture-test dataset suite** design (settled
  2026-08-31). What the roadmap section costs once the existing suite is run at the roadmap's own
  scale: four of the five families already ship as checked-in generators from DS-1…DS-4
  (2026-08-16) and have only ever run at 64 rows. §1 is the measured ground truth — a full 1M
  generation is 171.6 s and 2.5 GB, and five DS-4 findings are still open, three of them
  consequences of the 10 000-row `samplingRows` cap that 64 rows cannot see. §2 names the four real
  gaps (scale, dataset identity, baselines, the absent secrets mechanism), §3 the five units
  TT-1…TT-5 with checkable acceptance and the reason the cut is not one-per-bullet, §4 the dated
  decisions D-1…D-6 (Python generators stay where they are; table identity over file bytes; ≥ 1M
  floor, 10M ceiling; data never enters git; two CI scales in two places), §5 the measurement-bed
  contract v1.2's W-0…W-2 and the v0.9 spill matrix consume, §6 risks, §7 what is not claimed.
  TT-5 (opt-in secrets flagging) is the only unit with product surface and is risk tier **high**.

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
| Understand the v1.2 (was v0.8) torture-test dataset suite (units, generators, CI) | [torture-suite.md](torture-suite.md) |
| See what a 1M-row generation of every family costs | [torture-suite.md](torture-suite.md) §1.1 |
| See what v1.2 (was v0.8) deliberately does not claim | [torture-suite.md](torture-suite.md) §7 |
| Answer the v1.0 freeze question for one public surface | [v1-0-api-review-2026-09-02.md](v1-0-api-review-2026-09-02.md) §2 |
| See which rows a v1.0 `yes` cannot yet bind, and why | [v1-0-api-review-2026-09-02.md](v1-0-api-review-2026-09-02.md) §5 |
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
