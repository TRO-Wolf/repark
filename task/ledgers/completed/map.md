# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [mw-9-delete-granularity-ledger.md](mw-9-delete-granularity-ledger.md) —
  **MW-9 (2026-08-24):** honor `write.delete.granularity` (`file` / `partition`);
  Spark default `file`; close registry `MOR-2` for RePark-owned MERGE
  (fork SQL DELETE/UPDATE still partition-group).

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
