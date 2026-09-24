{% materialization view, adapter='repark' -%}
  {{ exceptions.raise_compiler_error(
    "dbt-repark does not support materialized='view': the SQL door serves CREATE VIEW, but
     the adapter does not build views yet (divergence registry DBT-VIEW-1). Use
     materialized='table' with file_format='iceberg'."
  ) }}
{%- endmaterialization %}


{% materialization incremental, adapter='repark', supported_languages=['sql'] -%}
  {{ exceptions.raise_compiler_error(
    "dbt-repark does not support materialized='incremental':
     RePark does not run dbt incremental/snapshot materializations yet
     (divergence registry DBT-INCREMENTAL-1). Use materialized='table', which rebuilds with
     CREATE OR REPLACE TABLE in one Iceberg snapshot."
  ) }}
{%- endmaterialization %}


{% materialization snapshot, adapter='repark' -%}
  {{ exceptions.raise_compiler_error(
    "dbt-repark does not support snapshots:
     RePark does not run dbt incremental/snapshot materializations yet
     (divergence registry DBT-INCREMENTAL-1)."
  ) }}
{%- endmaterialization %}
