# map — python/repark/tests/sql/data

## Purpose

Measured cell tables that a facade SQL pin reads as its clause.

## Contents

- [ident_dialect_1_cells.jsonl](ident_dialect_1_cells.jsonl) — the AUD-015 domain table:
  1144 cells (13 heads x 22 target spellings x a temporary view of the target's name or
  none x the memory and hadoop catalogs), each with Spark 4.1.2's answer and state change
  (hadoop catalog) and RePark's at 4fd5c6c1. Read by
  [../test_ident_dialect_1.py](../test_ident_dialect_1.py) (IDENT-DIALECT-1).

SHA-256 of the cells file:

`66570faf960f93bde5b2ff2e1521a9a5f7282f754c7b6be9a068f570f6d5033c`

## Pointers

- Up: [../map.md](../map.md)
