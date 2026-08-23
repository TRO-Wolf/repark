# map — task/ledgers/

## Purpose
The per-unit ledgers, filed by state: the directory is the status (chartered by
[staging/dl-1-ledger-lifecycle-charter-ledger.md](staging/dl-1-ledger-lifecycle-charter-ledger.md),
2026-08-23). Until DL-1's backfill lands, the ledgers of finished units still sit in
[../map.md](../map.md) under `task/`.

## Contents
- [staging/](staging/map.md) — ledgers of units in flight; born on the unit's branch.

`completed/` (frozen, awaiting archive) and `archive/yyyy-mm/` (immutable) arrive with DL-1's
script commit.

## I want to... → go to
| I want to... | go to |
|---|---|
| Open a unit's ledger | `staging/<unit>-ledger.md` on the unit's branch |
| Mark a unit finished | `scripts/ledger_lifecycle.py move` (DL-1) → `completed/` in the unit's last commit |
| Understand the three states and the script | the DL-1 charter above, §3 |

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../AGENTS.md](../../AGENTS.md) "Markdown document lifecycle"
