{% macro repark__generate_database_name(custom_database_name=none, node=none) -%}
  {%- if custom_database_name is none -%}
    {{ target.database }}
  {%- else -%}
    {{ custom_database_name | trim }}
  {%- endif -%}
{%- endmacro %}


{% macro repark__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create namespace if not exists {{ relation.database }}.{{ relation.schema }}
  {% endcall %}
{% endmacro %}


{% macro repark__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show namespaces in {{ database }}
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro repark__get_columns_in_relation(relation) -%}
  {{ return(adapter.get_columns_in_relation(relation)) }}
{% endmacro %}


{% macro repark__get_columns_in_relation_raw(relation) -%}
  {{ exceptions.raise_compiler_error(
    "dbt-repark does not read DESCRIBE EXTENDED text for column metadata (divergence registry
     DBT-DESC-1). Call adapter.get_columns_in_relation instead, which reads the facade schema."
  ) }}
{% endmacro %}


{% macro repark__create_temporary_view(relation, compiled_code) -%}
  {{ exceptions.raise_compiler_error(
    "dbt-repark does not stage temporary views:
     RePark does not run dbt incremental/snapshot materializations yet
     (divergence registry DBT-INCREMENTAL-1). Only materialized='table' is supported."
  ) }}
{%- endmacro %}


{% macro repark__create_view_as(relation, sql) -%}
  {{ exceptions.raise_compiler_error(
    "dbt-repark cannot build views: the SQL door serves CREATE VIEW, but the adapter does
     not build views yet (divergence registry DBT-VIEW-1). Set materialized='table' on "
     ~ relation.render() ~ "."
  ) }}
{% endmacro %}


{% macro repark__options_clause() -%}
  {%- if config.get('options') is not none -%}
    {{ exceptions.raise_compiler_error(
      "dbt-repark cannot set options: RePark refuses the OPTIONS clause on an Iceberg CTAS
       (divergence registry DBT-CTASCLAUSE-1). Use tblproperties instead."
    ) }}
  {%- endif %}
{%- endmacro %}


{% macro repark__clustered_cols(label, required=false) %}
  {%- if config.get('clustered_by') is not none -%}
    {{ exceptions.raise_compiler_error(
      "dbt-repark cannot set clustered_by: RePark refuses CLUSTERED BY ... INTO n BUCKETS on an
       Iceberg CTAS (divergence registry DBT-CTASCLAUSE-1). Use partition_by, which is served."
    ) }}
  {%- endif %}
{%- endmacro %}
