# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-1-2-cow-oracle-ledger.md](v3e-1-2-cow-oracle-ledger.md) —
  **V3E-1 + V3E-2 (2026-08-24):** adopted v3 copy-on-write DML measurement and the v3
  maintenance-oracle decision. Ships in this PR.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
