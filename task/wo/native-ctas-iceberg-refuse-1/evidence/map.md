# map — task/wo/native-ctas-iceberg-refuse-1/evidence

## Purpose

The records behind [card NATIVE-CTAS-ICEBERG-REFUSE-1](../../../roadmap/mid-term/native-ctas-iceberg-refuse-1-card-2026-11-12.md):
the inventory, step 0 on main and on Spark 4.1.2, and the head run.

## Contents

- [inventory.md](inventory.md) — every entry, statement form and target by which a CTAS reaches the
  native door's create, with file:line and the measured cell.
- [step0/](step0/map.md) — main `a3f8c7d9`: 380 native cells, 66 Spark-door cells, 66 Spark cells.
- [after/](after/map.md) — the head build: the same cells, and the facade pin red on main and
  green on head.

## Step 0 in one table (main, both Iceberg catalogs of the native session)

| Cells | Main | Spark 4.1.2 |
|---|---|---|
| CTAS, every spelling, `UNION` / `DISTINCT` / `INTERSECT` / `EXCEPT` / aggregate | table created, zero rows, success | the query's rows |
| CTAS, every spelling, literal / scan / `UNION ALL` / `VALUES` / join | `register_table does not support tables with data`, no table | the query's rows |
| `OR REPLACE` over an existing table (failing or succeeding query) | the table is dropped, the statement errors | failing: the table is kept; succeeding: replaced |
| `IF NOT EXISTS` / plain CTAS over an existing table | no-op / `already exists` | no-op / `already exists` |
| `CREATE TABLE (cols)` then `INSERT INTO … SELECT` (the workaround) | Spark's rows, every shape | Spark's rows |
| RePark `spark.sql` CTAS, every shape | Spark's rows | — |
| in-memory CTAS (bare, `datafusion.public.`) | Spark's rows, every shape | — |

## Cost

The refusal reads the plan the native guard already builds (`refuse_native_statement`); it adds no
parse and no plan. It walks that plan once and clones the session config only when it finds a
CTAS node, so no timing loop was run.

## Pointers

- Up: [order map](../map.md)
