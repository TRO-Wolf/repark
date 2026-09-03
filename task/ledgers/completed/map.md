# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [b-mor-3-rewrite-position-deletes-v3-ledger.md](b-mor-3-rewrite-position-deletes-v3-ledger.md) —
  **B-MOR-3 (2026-09-03), delivered:** owner ruling BUILD — `rewrite_position_delete_files`
  returns Spark's four zeros on a DV-only v3 table and converts an admitted parquet group
  to one PUFFIN per data file. Floor residue `B-MOR-3-FLOOR-1` / F-24. Branch
  `feat/b-mor-3-rewrite-position-deletes-v3`. `risk_tier: standard`. Model:
  grok-4.6 → glm-5.3-flash (continuation).
- [v1-gate-audit-ledger.md](v1-gate-audit-ledger.md) — **V1-GATE (2026-09-03), in flight:**
  the v1.0 north-star gate statement. Audits all twenty §3 rows into §3.1 (glyph, claim,
  residual → registry row, class and date, pin), reads the fork rows the gate leans on at the
  consumed pin `ff4764d3`, writes the one dated gate line, re-dates `S3T-V3-1` with the live
  re-dispatch, and files the published gate board. Result: every row ✅ or dated DECLARED, no
  BACKLOG blocker inside a v1.0-requires cell. Carries a dated ERRATA block: the audit is scoped
  to each row's requires cell with the surface residuals (`RDF-1`, `ORPHAN-1/2`, `MANIFEST-1/3`)
  listed beside it, `B-MOR-3`'s class is stated as DELIBERATE by analogy to OD-2 with the owner
  line still pending (ERRATA-A), and §2 pillar 4's full v3 statement coverage is recorded as
  **not discharged** with the evidence for that reading (ERRATA-B) and queued as V3-COV.
  `risk_tier: standard`. Branch `docs/v1-gate-audit`.
- [v3-cov-statement-coverage-ledger.md](v3-cov-statement-coverage-ledger.md) —
  **V3-COV (2026-09-03), delivered:** the full v3 statement-coverage comparison against PySpark
  that discharges the north star's §2 pillar 4 — 81 programs, 267 cells, 71 EQUAL, 9 rows filed,
  2 defects FIXED red-first. `risk_tier: standard`. Branch `feat/v3-cov-statement-coverage`.
  Matrix: [../../../docs/design/v3-statement-coverage.md](../../../docs/design/v3-statement-coverage.md).
  Carries one RULING (`V3-COV-3`, the delegated partitioned INSERT's unstable `_row_id`) —
  closed by RP-8 the same day.
- [rp-8-repin-f21-f22-ledger.md](rp-8-repin-f21-f22-ledger.md) — **RP-8 (2026-09-03),
  delivered:** the fork repin `ff4764d3` → `c1d6c9de`, consuming F-19/F-20 (`#261`), F-21
  (`#262`) and F-22 (`#263`). The deletion-vector container close takes over the legacy-delete
  collect, merge and file-scoped removal in one delete-manifest pass, so RePark's own
  `write/merge/dv_close/legacy_deletes.rs` (493 lines) is deleted; `V3-UPGRADE-DV-PLAIN-1`,
  `V3-UPGRADE-DV-PART-1`, `F-v3-10-partition-file-order` and `V3-COV-3` all FIXED at Spark's
  measured values; `V3-FILEORDER-1` stays DECLARED and widens to the fork's `INSERT INTO` path.
  E-4 closed by RP-9 2026-09-03 at pin `594bdbe5`.
  `risk_tier: standard`. Branch `feat/rp-8-repin-f21-f22`.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
