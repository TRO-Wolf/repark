# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [v3r-1-rulings-ledger.md](v3r-1-rulings-ledger.md) — **V3R-1 (2026-08-25):** the five owner
  rulings recorded where the gate reads them, and the one that is engine code built and pinned
  — copy-on-write DML on a format-v3 table refuses (registry `V3-COW-1`, two guard seats),
  `V3-GEO-1` DECLARED, shredded variant queued, OD-3b in, the upgrade row ruled "build behind
  the opt-in". Thirteen clauses, all PROVEN. The CCC pass found three S1 bypasses of the guard
  on novel inputs (short names under a default catalog, a padded merge-on-read spelling, a
  dotted quoted name) — remediated in cycle 2 with pins proven load-bearing by mutation;
  `CCC-CONVERGED`.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
