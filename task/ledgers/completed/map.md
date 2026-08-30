# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [rp-3-fork-repin-ledger.md](rp-3-fork-repin-ledger.md) — **RP-3 (2026-08-28), on
  `feat/rp-3-fork-repin`:** one frozen fork repin at `d408da42` (F-17, F-14,
  F-7 U3, F-16, F-9, F-15, the public R114 DV API), the engine-side wiring of the fork's DV
  container closure — opt-in for callers, so the engine's own MOR path must make the call — and
  the eight-cell DV input-state matrix on all three doors. Eleven clauses, all PROVEN.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
