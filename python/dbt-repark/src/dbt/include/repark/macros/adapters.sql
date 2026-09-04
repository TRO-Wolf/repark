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
    "dbt-repark cannot use DESCRIBE EXTENDED for column metadata: it answers Arrow type
     spellings (Utf8, Int32, Date32) and no table-detail block (divergence registry
     DBT-DESC-1). Call adapter.get_columns_in_relation instead, which reads the facade schema."
  ) }}
{% endmacro %}


{% macro repark__create_temporary_view(relation, compiled_code) -%}
  {{ exceptions.raise_compiler_error(
    "dbt-repark has no temporary views: RePark refuses CREATE OR REPLACE TEMPORARY VIEW
     (divergence registry DBT-TEMPVIEW-1). Only materialized='table' is supported."
  ) }}
{%- endmacro %}


{% macro repark__create_view_as(relation, sql) -%}
  {{ exceptions.raise_compiler_error(
    "dbt-repark cannot build views: RePark refuses CREATE OR REPLACE VIEW (divergence
     registry DBT-VIEW-1). Set materialized='table' on " ~ relation.render() ~ "."
  ) }}
{% endmacro %}


{% macro repark__alter_column_comment(relation, column_dict) %}
  {{ exceptions.raise_compiler_error(
    "dbt-repark cannot persist column documentation: RePark refuses ALTER TABLE ... ALTER
     COLUMN ... COMMENT via SQL (divergence registry DBT-COLCOMMENT-1). Remove
     persist_docs.columns, or keep persist_docs.relation only."
  ) }}
{% endmacro %}


{% macro repark__comment_clause() %}
  {%- set raw_persist_docs = config.get('persist_docs', {}) -%}
  {%- if raw_persist_docs is mapping and raw_persist_docs.get('relation', false) -%}
    {{ exceptions.raise_compiler_error(
      "dbt-repark cannot persist a relation description: RePark refuses CREATE TABLE ...
       COMMENT on an Iceberg CTAS (divergence registry DBT-RELCOMMENT-1). Remove
       persist_docs.relation, or carry the description in tblproperties."
    ) }}
  {%- endif %}
{%- endmacro %}


{% macro repark__location_clause() %}
  {%- if config.get('location_root') is not none -%}
    {{ exceptions.raise_compiler_error(
      "dbt-repark cannot set location_root: RePark refuses CREATE TABLE ... LOCATION on an
       Iceberg CTAS and derives the table location from the namespace warehouse (divergence
       registry DBT-CTASCLAUSE-1)."
    ) }}
  {%- endif %}
{%- endmacro %}


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
