# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [v3-7-merge-lineage-ledger.md](v3-7-merge-lineage-ledger.md) —
  **V3-7 (2026-09-02), completed:** carry `_row_id` through the RePark-owned MERGE
  writer; lift `V3-COW-1` MERGE where Spark-equal. Subquery-WHERE DML stays
  refused. `risk_tier: standard`. Branch `feat/v3-7-merge-lineage`.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
