# map — python/dbt-repark/src/dbt/include/repark/macros

## Purpose

Every macro here overrides one `dbt-spark` macro that RePark's SQL door refuses. The measured
refusals are `python/dbt-repark/tests/test_statement_surface.py`; the registry rows are
[../../../../../../../docs/spark-sql-iceberg-parity.md](../../../../../../../docs/spark-sql-iceberg-parity.md)
§2.5 and §7. These files carry **no Jinja or SQL comments** (owner ruling 2026-08-26); the reason
for each override is the table below.
pins: dbt-1-adapter/C-002, dbt-1-adapter/C-004

## Contents

- `adapters.sql`

  | Macro | Why it overrides `spark__` |
  |---|---|
  | `repark__generate_database_name` | `spark__generate_database_name` returns `None`, which drops the catalog part. RePark needs it. |
  | `repark__create_schema` | `create schema if not exists <ns>` refuses; RePark wants `create namespace if not exists <catalog>.<ns>`. |
  | `repark__drop_schema` | same naming reason. |
  | `repark__list_schemas` | `show databases` refuses without a catalog (registry `NS-1`); `show namespaces in <catalog>` is the served form. |
  | `repark__get_columns_in_relation` | routes to the adapter's Python method so no caller can fall through to `spark__`'s `describe extended`, which answers Arrow spellings (`DBT-DESC-1`). |
  | `repark__get_columns_in_relation_raw` | refuses loudly for the same reason, instead of returning a wrong-typed table. |
  | `repark__create_temporary_view` | RePark has no temporary views (`DBT-TEMPVIEW-1`). |
  | `repark__create_view_as` | RePark refuses `create or replace view` (`DBT-VIEW-1`). |
  | `repark__alter_column_comment` | `alter column … comment` is refused (`DBT-COLCOMMENT-1`). |
  | `repark__comment_clause` | `create table … comment` is refused on an Iceberg CTAS, and after `tblproperties` the parser blames `using` (`DBT-RELCOMMENT-1`). Refusing at compile time keeps that misleading message away from the user. |
  | `repark__location_clause`, `repark__options_clause`, `repark__clustered_cols` | `LOCATION`, `OPTIONS` and `CLUSTERED BY` are refused on an Iceberg CTAS (`DBT-CTASCLAUSE-1`). `partition_by` is served, so `spark__partition_cols` is **not** overridden. |

- `materializations.sql` — `view`, `incremental` and `snapshot` for `adapter='repark'`, each
  raising a compiler error that names its registry row. Materialization lookup walks the plugin
  dependency chain, so a `repark` materialization wins over the inherited `spark` one; without
  these three, dbt would silently run dbt-spark's versions and fail deep inside a merge.
  `table` is **not** overridden: dbt-spark's, with `old_relation.is_iceberg` true, skips the drop
  and issues the `create or replace table` RePark serves.

## Pointers

- Up: [../map.md](../map.md)
- The adapter: [../../../adapters/repark/map.md](../../../adapters/repark/map.md)
