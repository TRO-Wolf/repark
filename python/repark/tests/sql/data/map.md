# map — python/repark/tests/sql/data

## Purpose

Measured cell tables that a facade SQL pin reads as its clause.

## Contents

- [ident_dialect_1_cells.jsonl](ident_dialect_1_cells.jsonl) — the AUD-015 domain table:
  1144 cells (13 heads x 22 target spellings x a temporary view of the target's name or
  none x the memory and hadoop catalogs), each with Spark 4.1.2's answer and state change
  (hadoop catalog) and RePark's at 4fd5c6c1, then 28 delta cells (a relation after a comma or
  JOIN in a FROM clause, 14 statements x both catalogs) with RePark's answer at c3c2a76b. Read by
  [../test_ident_dialect_1.py](../test_ident_dialect_1.py) (IDENT-DIALECT-1).

SHA-256 of the cells file:

`ec4135c044ff7182f601eff4c260f468e4c32b99e3db7dec13262932cb35b767`

## Pointers

- Up: [../map.md](../map.md)
