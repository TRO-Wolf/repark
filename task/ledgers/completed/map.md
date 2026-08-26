# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [v3e-3-partitioned-eqdel-fixtures-ledger.md](../archive/2026-08/2026-08-25-v3e-3-partitioned-eqdel-fixtures-ledger.md) — V3E-3 — partitioned + equality-delete v3 fixtures
- [dl-5-contract-compaction-ledger.md](dl-5-contract-compaction-ledger.md) — **DL-5
  (2026-08-25):** compact the live STATUS remainder and the contributor contract;
  `engineering-method` loses restated project rules; `check_docs_compaction` ceilings
  extend to `AGENTS.md` and the method skill. Host-injection measurement and
  `.agents/roles/` are out of scope.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
