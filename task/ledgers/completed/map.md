# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [dl-3-archive-map-compaction-charter-ledger.md](dl-3-archive-map-compaction-charter-ledger.md) —
  **DL-3 (2026-08-23):** archive month maps become one line per ledger (owner ruling: the record
  is the ledger, the row is navigation); `_condense_row` in the lifecycle script + the 2026-08
  migration (~55 kB → ~15 kB) + the off-the-read-path note.

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
