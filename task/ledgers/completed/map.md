# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.
A unit's pin binding is its `<unit>-pins.md` beside its ledger (one `C-NNN: <path>[::<name>], ...` line per clause, or `C-NNN: none (records)`), read by `scripts/check_ledger_grammar.py` rule B; it moves with its ledger and gets no row here.

## Contents


## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
