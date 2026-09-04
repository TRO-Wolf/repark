{% materialization view, adapter='repark' -%}
  {{ exceptions.raise_compiler_error(
    "dbt-repark does not support materialized='view': RePark refuses CREATE OR REPLACE VIEW
     (divergence registry DBT-VIEW-1). Use materialized='table' with
     file_format='iceberg'."
  ) }}
{%- endmaterialization %}


{% materialization incremental, adapter='repark', supported_languages=['sql'] -%}
  {{ exceptions.raise_compiler_error(
    "dbt-repark does not support materialized='incremental': every incremental strategy
     stages the new rows in a temporary view first, and RePark has no temporary views
     (divergence registry DBT-TEMPVIEW-1). Use materialized='table', which rebuilds with
     CREATE OR REPLACE TABLE in one Iceberg snapshot."
  ) }}
{%- endmaterialization %}


{% materialization snapshot, adapter='repark' -%}
  {{ exceptions.raise_compiler_error(
    "dbt-repark does not support snapshots: the snapshot materialization needs temporary
     views and MERGE against a staged relation, and RePark has no temporary views
     (divergence registry DBT-TEMPVIEW-1)."
  ) }}
{%- endmaterialization %}
