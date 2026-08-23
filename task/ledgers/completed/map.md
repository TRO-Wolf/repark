# map — task/ledgers/completed/

## Purpose
Ledgers of finished units, moved here by the unit's last commit
(`python3 scripts/ledger_lifecycle.py move task/ledgers/staging/<unit>-ledger.md completed`) and
frozen: `make check-ledgers` allows a link repair or a dated errata note at the top, nothing
else. The next pickup's `make ledger-archive` files everything here under
[../archive/](../archive/map.md) by the merge date.

## Contents
- [dl-2-ledger-grammar-charter-ledger.md](dl-2-ledger-grammar-charter-ledger.md) — **DL-2
  (2026-08-23):** the ledger grammar, checked by `scripts/check_ledger_grammar.py` — clause rows,
  `pins:` citations binding tests to clauses, the Critic's attestation form; XML measured and
  declined. Stacked on DL-1 (PR #221).

## Pointers
- Up: [../map.md](../map.md)
- Policy: [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle"
