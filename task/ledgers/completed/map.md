# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [rp-5-fork-repin-ledger.md](rp-5-fork-repin-ledger.md) —
  **RP-5 (2026-09-01), complete:** fork pin `00cdde0`. REF-1 FIXED; REF-3 BACKLOG;
  RDF-1 BACKLOG (MW-7 2,500-row pin still green; MW-8 partitioned runbook rewrote).
  `risk_tier: standard`. Branch `feat/rp-5-fork-repin`.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
