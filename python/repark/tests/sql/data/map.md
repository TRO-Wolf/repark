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

- [dq_identifiers_config_refuse_1_cells.jsonl](dq_identifiers_config_refuse_1_cells.jsonl) —
  Spark 4.1.2 (Iceberg 1.11.0) answers for `spark.sql.ansi.doubleQuotedIdentifiers`: the
  `SQLConf` entry, 21 `SET`/`RESET` arms, 3 `conf.set` arms and the six DML-reach statements
  with the config off (plus Spark's config-on answers) and under `spark.sql.ansi.enabled=false`
  with the config on. Read by
  [../test_dq_identifiers_config_refuse_1.py](../test_dq_identifiers_config_refuse_1.py).

SHA-256 of the AUD-015 cells file:

`0507e12cfeefc7f6514a32517d2a4a972a91b647838cd3f8427b6b68a8d73377`

## Pointers

- Up: [../map.md](../map.md)
