# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3-2-create-v3-opt-in-ledger.md](v3-2-create-v3-opt-in-ledger.md) —
  **V3-2 (2026-08-24):** CREATE/CTAS `format-version = 3` behind
  `repark.sql.allowCreateFormatVersion3` (default false). Default create stays v2;
  ALTER stays refused; V3-LINEAGE-1 is not lifted.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
