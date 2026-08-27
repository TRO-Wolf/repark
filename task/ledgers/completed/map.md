# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [production-file-size-ledger.md](production-file-size-ledger.md) — production/file-size
  refactor: split the 8,390-line session free-function surface into responsibility-named modules,
  preserve `_funcs` compatibility, and retire its Python source-size exception. The sequential
  Critic closed with all charter clauses proven.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
