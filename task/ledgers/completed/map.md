# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [rp-1-fork-repin-ledger.md](rp-1-fork-repin-ledger.md) — **RP-1 (2026-08-23):** re-pin
  `iceberg*` to fork `main` `5e7b2e4` (F-0/F-1/F-2/F-8a; T6 name-directory freeze;
  Spark `position_deletes` rewrite). First row of the post-MW sequence.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
