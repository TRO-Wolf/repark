# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [frontier-worker-decision-plan-ledger.md](frontier-worker-decision-plan-ledger.md) — proposed frontier brief skills, worker qualification, and pilot decisions; documentation-only addendum to PR #423.
- [capability-roadmap-planning-ledger.md](capability-roadmap-planning-ledger.md) — capability identifiers, release assembly, and historical roadmap reconciliation; planning-only unit.
- [dfcore-1-ledger.md](dfcore-1-ledger.md) —
  **DFCORE-1 (2026-09-07), in flight:** leaf helpers out of `core.py` — Arrow cell
  conversion to `rows_export.py`, export-error mapping to new `export_errors.py`,
  mapInArrow schema checks to new `udf_schema.py`, grouped-UDF assembly to new
  `grouped_udf.py`. Move-only: `core.py` 6302 → 5954, `joins_columns.py` 1239 → 1238,
  export surfaces pinned identical, 6093 pre-existing collected IDs unchanged, 6 added. `risk_tier: standard`.
  Branch `refactor/dfcore-1`.
  pins: dfcore-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [dfcore-2-ledger.md](dfcore-2-ledger.md) — Unit ledger — DFCORE-2 · the four UDF projection rewrites out of `core.py`
- [dfcore-3-ledger.md](dfcore-3-ledger.md) — Unit ledger — DFCORE-3 · the statistics family out of `core.py`
- [dfcore-4a-ledger.md](dfcore-4a-ledger.md) — Unit ledger — DFCORE-4a · sampling out of `core.py`
- [dfcore-4b-ledger.md](dfcore-4b-ledger.md) — Unit ledger — DFCORE-4b · display out of `core.py`
- [dfcore-5-ledger.md](dfcore-5-ledger.md) — Charter ledger — DFCORE-5 · `approxQuantile` in one collect per frame
- [dfcore-6-ledger.md](dfcore-6-ledger.md) — Charter ledger — DFCORE-6 · eager previews fetch N+1, never `count()`
- [fnp-8-review-ledger.md](fnp-8-review-ledger.md) —
  **FNP-8-REVIEW (2026-09-07), in flight:** remediation round 1 for FNP-8 (PR #412,
  merged unreviewed) — the round-1 critic's F1–F7, each red-first against live
  PySpark 4.1.2, plus the no-regression held set. `risk_tier: standard`. Branch
  `review/fnp-8-review`.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
