# map — python/repark/tests/sql/data

## Purpose

Measured cell tables that a facade SQL pin reads as its clause.

## Contents

- [ident_dialect_1_cells.jsonl](ident_dialect_1_cells.jsonl) — the AUD-015 domain table:
  1144 cells (13 heads x 22 target spellings x a temporary view of the target's name or
  none x the memory and hadoop catalogs), each with Spark 4.1.2's answer and state change
  (hadoop catalog) and RePark's at 4fd5c6c1; 28 delta cells (a relation after a comma or
  JOIN in a FROM clause, 14 statements x both catalogs) with RePark's answer at c3c2a76b;
  16 IDENT-DIALECT-1B pin-plan cells (the card's twelve, rows 1 to 6, and four from-first
  cells held at RePark's refusal); and 12 IDENT-DIALECT-1B delta cells (the `f1b_*` relations
  after a VALUES row list, a LATERAL subquery or a `values` column). Read by
  [../test_ident_dialect_1.py](../test_ident_dialect_1.py) (IDENT-DIALECT-1 and 1B).

SHA-256 of the cells file:

`0507e12cfeefc7f6514a32517d2a4a972a91b647838cd3f8427b6b68a8d73377`

- [ansi_subscript_field_cells.jsonl](ansi_subscript_field_cells.jsonl) — 93 cells measured
  2026-11-05 on Spark 4.1.2 with Iceberg 1.11.0 (hadoop catalog): array subscripts, `element_at`,
  `try_element_at`, lambdas, CASE and AND guards, duplicate and case-colliding struct fields, and
  DELETE, UPDATE, INSERT and MERGE statements that reach them, under ANSI, legacy and
  case-sensitive confs, each with Spark's answer or refusal text and its readback after DML.
  Read by [../test_ansi_subscript_field.py](../test_ansi_subscript_field.py).

## Pointers

- Up: [../map.md](../map.md)
