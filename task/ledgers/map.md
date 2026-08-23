# map — task/ledgers/

## Purpose
The per-unit ledgers, filed by state: the directory is the status (chartered by
[staging/dl-1-ledger-lifecycle-charter-ledger.md](staging/dl-1-ledger-lifecycle-charter-ledger.md),
2026-08-23; backfilled the same day — 122 ledgers archived by merge date, four open charters
left in `staging/`).

## Contents
- [staging/](staging/map.md) — ledgers of units in flight; born on the unit's branch.
- [completed/](completed/map.md) — finished, frozen; the agent's `move` in the unit's last commit.
- [archive/](archive/map.md) — immutable, one folder per month, named `yyyy-mm-dd-<unit>-ledger.md`
  by the merge date; the script's `archive` at pickup.

## I want to... → go to
| I want to... | go to |
|---|---|
| Open a unit's ledger | `staging/<unit>-ledger.md` on the unit's branch |
| Mark a unit finished | `python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`, in the unit's last commit |
| File what finished (pickup step 0) | `make ledger-archive` — zero tokens, staged, idempotent |
| Check the bins and every ledger link | `make check-ledgers` |
| Understand the three states and the script | the DL-1 charter above, §3; `scripts/ledger_lifecycle.py` docstring |

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../AGENTS.md](../../AGENTS.md) "Markdown document lifecycle"
