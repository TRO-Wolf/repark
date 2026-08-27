"""The :class:`ReparkSession` facade — the near-drop-in entry point.



Migrating a PySpark script is a one-line import change::



    from repark.spark import ReparkSession   # was: from pyspark.sql import SparkSession



    spark = ReparkSession.builder.appName("etl").getOrCreate()

    df = spark.sql("SELECT 1 AS a")

    df.show()



For source-compatible drop-in, ``SparkSession`` is kept as an alias of :class:`ReparkSession`, so

``from repark import SparkSession`` also works and the rest of an existing PySpark script stays

byte-identical. The builder mirrors PySpark's ``SparkSession.builder…getOrCreate()`` chain. Compute

runs in Rust behind the native ``repark._native.PyReparkSession`` — a thin, typed Python shell.

"""

from __future__ import annotations

import contextlib as contextlib

import contextvars as contextvars

import logging as logging

import re as re

import sys as _sys

import uuid as uuid

import warnings as warnings

from pathlib import Path as Path

from typing import TYPE_CHECKING as TYPE_CHECKING, Any as Any

from repark import _native as _native

from repark.spark._secrets import prop_key_is_secret as _prop_key_is_secret

from repark.spark._idents import is_plain_ident as _is_plain_ident

from repark.spark._idents import quote_ident as _quote_ident

from repark.spark._idents import quote_ident_if_needed as _quote_ident_if_needed

from repark.spark._idents import reject_path_escape_segment as _reject_path_escape_segment

from repark.spark.catalog import (
    DEFAULT_CATALOG_NAME as DEFAULT_CATALOG_NAME,
    DEFAULT_DATABASE_NAME as DEFAULT_DATABASE_NAME,
    Catalog as Catalog,
)

from repark.spark.dataframe import DataFrame as DataFrame

from repark.errors import (
    AnalysisException as AnalysisException,
    IllegalArgumentException as IllegalArgumentException,
    PySparkException as PySparkException,
    PySparkRuntimeError as PySparkRuntimeError,
    PySparkTypeError as PySparkTypeError,
    PySparkValueError as PySparkValueError,
)

from repark.spark.session.session_time_zone import (
    DEFAULT_SESSION_TIME_ZONE as DEFAULT_SESSION_TIME_ZONE,
    SESSION_TIME_ZONE_KEY as SESSION_TIME_ZONE_KEY,
)

from repark.spark._temp_views import scratch_view_name as scratch_view_name

from repark.spark.session.timestamp_type import (
    DEFAULT_TIMESTAMP_TYPE as DEFAULT_TIMESTAMP_TYPE,
    TIMESTAMP_TYPE_KEY as TIMESTAMP_TYPE_KEY,
)

from repark.spark.session import session_configuration as _session_configuration

from repark.spark.session import catalog_resolution as _catalog_resolution

from repark.spark.session import session_state as _session_state

from repark.spark.session import reader_support as _reader_support

from repark.spark.session import create_dataframe_values as _create_dataframe_values

from repark.spark.session import create_dataframe_schema as _create_dataframe_schema

from repark.spark.session import create_dataframe_rows as _create_dataframe_rows

from repark.spark.session import create_dataframe_inference as _create_dataframe_inference

from repark.spark.session import create_dataframe_arrow as _create_dataframe_arrow

from repark.spark.session import create_dataframe_tuples as _create_dataframe_tuples

from repark.spark.session import sql_udf_parsing as _sql_udf_parsing

from repark.spark.session import sql_udf_rewrite as _sql_udf_rewrite

from repark.spark.session import sql_udf_discovery as _sql_udf_discovery

from repark.spark.session import sql_udf_residual as _sql_udf_residual

from repark.spark.session import sql_udf_materialization as _sql_udf_materialization

from repark.spark.session import sql_relations as _sql_relations

_session_configuration.logger = logging.getLogger(__name__)

vars(_catalog_resolution).update(
    {
        "_DISPLAY_STYLE_KEY": (_session_configuration._DISPLAY_STYLE_KEY),
        "_parse_table_identifier_segments": (_sql_relations._parse_table_identifier_segments),
    }
)

vars(_create_dataframe_values).update(
    {
        "_validate_decimal_envelope": (_create_dataframe_inference._validate_decimal_envelope),
        "_parse_schema_ddl": (_create_dataframe_schema._parse_schema_ddl),
    }
)

vars(_create_dataframe_schema).update(
    {
        "_DECIMAL_PRECISION": (_create_dataframe_values._DECIMAL_PRECISION),
        "_DECIMAL_SCALE": (_create_dataframe_values._DECIMAL_SCALE),
        "_NUMPY_DATETIME64_DATE_UNITS": (_create_dataframe_values._NUMPY_DATETIME64_DATE_UNITS),
        "_TYPED_NULL_SQL": (_create_dataframe_values._TYPED_NULL_SQL),
        "_data_type_to_sql_type": (_create_dataframe_values._data_type_to_sql_type),
        "_normalize_create_dataframe_cell": (
            _create_dataframe_values._normalize_create_dataframe_cell
        ),
        "_numpy_datetime64_unit": (_create_dataframe_values._numpy_datetime64_unit),
    }
)

vars(_create_dataframe_rows).update(
    {
        "_INFER_NESTED_DICT_AS_STRUCT": (_create_dataframe_inference._INFER_NESTED_DICT_AS_STRUCT),
        "_LEGACY_FIRST_ELEMENT_COERCE": (_create_dataframe_inference._LEGACY_FIRST_ELEMENT_COERCE),
        "_apply_permutation": (_create_dataframe_schema._apply_permutation),
        "_column_null_sql_from_raw_tuples": (
            _create_dataframe_schema._column_null_sql_from_raw_tuples
        ),
        "_infer_null_sql_from_raw_cells": (_create_dataframe_schema._infer_null_sql_from_raw_cells),
        "_null_sql_for_pandas_dtype": (_create_dataframe_schema._null_sql_for_pandas_dtype),
        "_null_sql_for_polars_dtype": (_create_dataframe_schema._null_sql_for_polars_dtype),
        "_pandas_dtype_needs_object_null_witness": (
            _create_dataframe_schema._pandas_dtype_needs_object_null_witness
        ),
        "_schema_names_and_permutation": (_create_dataframe_schema._schema_names_and_permutation),
        "_arrow_table_from_tuples": (_create_dataframe_tuples._arrow_table_from_tuples),
        "_TYPED_NULL_SQL": (_create_dataframe_values._TYPED_NULL_SQL),
        "_is_pandas_dataframe": (_create_dataframe_values._is_pandas_dataframe),
        "_is_polars_dataframe": (_create_dataframe_values._is_polars_dataframe),
        "_normalize_create_dataframe_cell": (
            _create_dataframe_values._normalize_create_dataframe_cell
        ),
        "_parse_create_dataframe_schema": (_create_dataframe_values._parse_create_dataframe_schema),
        "_sql_literal": (_create_dataframe_values._sql_literal),
    }
)

vars(_create_dataframe_inference).update(
    {
        "_SPARK_SCALAR_MERGE_LABELS": (_create_dataframe_tuples._SPARK_SCALAR_MERGE_LABELS),
        "_python_scalar_merge_kind": (_create_dataframe_tuples._python_scalar_merge_kind),
        "_DECIMAL_MAX_ABS": (_create_dataframe_values._DECIMAL_MAX_ABS),
        "_DECIMAL_PRECISION": (_create_dataframe_values._DECIMAL_PRECISION),
        "_DECIMAL_SCALE": (_create_dataframe_values._DECIMAL_SCALE),
    }
)

vars(_create_dataframe_arrow).update(
    {
        "_sql_type_to_arrow": (_create_dataframe_inference._sql_type_to_arrow),
        "_validate_decimal_envelope": (_create_dataframe_inference._validate_decimal_envelope),
        "_refuse_duplicate_pandas_columns": (
            _create_dataframe_rows._refuse_duplicate_pandas_columns
        ),
        "_rows_from_pandas": (_create_dataframe_rows._rows_from_pandas),
        "_infer_null_sql_from_raw_cells": (_create_dataframe_schema._infer_null_sql_from_raw_cells),
        "_null_sql_for_pandas_dtype": (_create_dataframe_schema._null_sql_for_pandas_dtype),
        "_null_sql_for_polars_dtype": (_create_dataframe_schema._null_sql_for_polars_dtype),
        "_pandas_dtype_needs_object_null_witness": (
            _create_dataframe_schema._pandas_dtype_needs_object_null_witness
        ),
        "_schema_names_and_permutation": (_create_dataframe_schema._schema_names_and_permutation),
        "_arrow_table_from_tuples": (_create_dataframe_tuples._arrow_table_from_tuples),
        "_DECIMAL_PRECISION": (_create_dataframe_values._DECIMAL_PRECISION),
        "_DECIMAL_SCALE": (_create_dataframe_values._DECIMAL_SCALE),
        "_normalize_create_dataframe_cell": (
            _create_dataframe_values._normalize_create_dataframe_cell
        ),
    }
)

vars(_create_dataframe_tuples).update(
    {
        "_INFER_NESTED_DICT_AS_STRUCT": (_create_dataframe_inference._INFER_NESTED_DICT_AS_STRUCT),
        "_LEGACY_FIRST_ELEMENT_COERCE": (_create_dataframe_inference._LEGACY_FIRST_ELEMENT_COERCE),
        "_infer_arrow_type_from_python_sample": (
            _create_dataframe_inference._infer_arrow_type_from_python_sample
        ),
        "_infer_struct_arrow_from_dict_samples": (
            _create_dataframe_inference._infer_struct_arrow_from_dict_samples
        ),
        "_merge_inferred_arrow_types": (_create_dataframe_inference._merge_inferred_arrow_types),
        "_prepare_nested_cell": (_create_dataframe_inference._prepare_nested_cell),
        "_sql_type_to_arrow": (_create_dataframe_inference._sql_type_to_arrow),
        "_validate_decimal_envelope": (_create_dataframe_inference._validate_decimal_envelope),
        "_sql_literal": (_create_dataframe_values._sql_literal),
    }
)

vars(_sql_udf_rewrite).update(
    {
        "_sql_find_registry_udf_calls": (_sql_udf_discovery._sql_find_registry_udf_calls),
        "_sql_peel_select_trailing_clauses": (_sql_udf_discovery._sql_peel_select_trailing_clauses),
        "_sql_residual_has_subquery": (_sql_udf_discovery._sql_residual_has_subquery),
        "_sql_udf_arg_is_simple": (_sql_udf_discovery._sql_udf_arg_is_simple),
        "_sql_udf_call_match_key": (_sql_udf_discovery._sql_udf_call_match_key),
        "_sql_materialize_expr_udfs": (_sql_udf_materialization._sql_materialize_expr_udfs),
        "_sql_plan_order_by_aliases": (_sql_udf_materialization._sql_plan_order_by_aliases),
        "_split_sql_select_list": (_sql_udf_parsing._split_sql_select_list),
        "_sql_strip_comments_preserve_strings": (
            _sql_udf_parsing._sql_strip_comments_preserve_strings
        ),
        "_sql_top_level_keyword_index": (_sql_udf_parsing._sql_top_level_keyword_index),
        "_sql_where_residual_base_projections": (
            _sql_udf_residual._sql_where_residual_base_projections
        ),
    }
)

vars(_sql_udf_discovery).update(
    {
        "_find_matching_paren": (_sql_relations._find_matching_paren),
        "_sql_mask_strings_and_comments": (_sql_relations._sql_mask_strings_and_comments),
        "_split_sql_select_list": (_sql_udf_parsing._split_sql_select_list),
        "_sql_strip_comments_preserve_strings": (
            _sql_udf_parsing._sql_strip_comments_preserve_strings
        ),
        "_sql_top_level_keyword_index": (_sql_udf_parsing._sql_top_level_keyword_index),
    }
)

vars(_sql_udf_residual).update(
    {
        "_sql_mask_strings_and_comments": (_sql_relations._sql_mask_strings_and_comments),
    }
)

vars(_sql_udf_materialization).update(
    {
        "_sql_find_registry_udf_calls": (_sql_udf_discovery._sql_find_registry_udf_calls),
        "_sql_udf_arg_is_simple": (_sql_udf_discovery._sql_udf_arg_is_simple),
        "_split_sql_select_list": (_sql_udf_parsing._split_sql_select_list),
    }
)

from repark.spark.session.session_configuration import (
    _BATCH_SIZE_KEYS,
    _CONF_GET_UNSET,
    _DATAFUSION_CONF_KEY_RE,
    _DATAFUSION_CONF_PREFIX,
    _DATAFUSION_RUNTIME_MEMORY_LIMIT_KEY,
    _DEFAULT_DISPLAY_STYLE,
    _DISPLAY_STYLE_KEY,
    _DISPLAY_STYLE_VALUES,
    _MEMORY_LIMIT_KEYS,
    _MEMORY_LIMIT_KEY_LOWER,
    _SQLCONF_DEFAULTS,
    _SQLCONF_STATIC_KEYS,
    _TARGET_PARTITIONS_KEYS,
    _apply_builder_datafusion_conf,
    _builder_has_memory_limit_key,
    _format_datafusion_set_sql,
    _forward_datafusion_conf,
    _is_datafusion_conf_key,
    _looks_like_datafusion_conf_key,
    _refuse_dual_memory_pool_knobs,
    _refuse_runtime_memory_limit_gb,
    logger,
    normalize_display_style,
)

from repark.spark.session.catalog_resolution import (
    _AUTO_MEMORY_CATALOG_KEY,
    _alias_catalog_name,
    _auto_memory_catalog_wanted,
    _catalog_names_from_builder_config,
    _default_catalog_from_builder_config,
    _default_namespace_from_builder_config,
    _join_table_identifier_segments,
    _sync_display_style_into_builder_config,
    resolve_table_name,
)

from repark.spark.session.session_state import (
    _STOPPED_MESSAGE,
    _active_session,
    _config_value_error,
    _late_catalog_names,
    _master_warned,
    _reset_active_session_for_tests,
    _reset_dropin_warnings_for_tests,
    _to_str,
    _unbounded_batch_warned,
    _warn_master_once,
    _warn_unbounded_batch_once,
    _install_state_proxy,
)

_install_state_proxy(_sys.modules[__name__])

from repark.spark.session.reader_support import (
    _CSV_NATIVE_OPTION_KEYS,
    _CSV_UNSUPPORTED_PARSE_OPTIONS,
    _EXCEL_NATIVE_OPTION_KEYS,
    _I64_MAX,
    _I64_MIN,
    _ICEBERG_TIME_TRAVEL_OPTIONS,
    _JSON_NATIVE_OPTION_KEYS,
    _JSON_UNSUPPORTED_PARSE_OPTIONS,
    _UNSUPPORTED_SEMANTIC_READER_OPTIONS,
    _json_input_nonempty,
    _json_multiline_empty_schema_is_mismatch,
    _parse_jdbc_int_option,
    _promote_csv_string_types,
    _reader_path_to_str,
    _schema_fields,
)

from repark.spark.session.create_dataframe_values import (
    _ARRAY_TYPECODES_SUPPORTED,
    _DECIMAL_MAX_ABS,
    _DECIMAL_PRECISION,
    _DECIMAL_SCALE,
    _NUMPY_DATETIME64_DATE_UNITS,
    _TYPED_NULL_SQL,
    _array_typecodes_supported,
    _coerce_schema_names,
    _data_type_to_sql_type,
    _is_pandas_dataframe,
    _is_polars_dataframe,
    _normalize_create_dataframe_cell,
    _numpy_datetime64_unit,
    _parse_create_dataframe_schema,
    _sql_literal,
    _supported_array_typecodes,
)

from repark.spark.session.create_dataframe_schema import (
    _apply_permutation,
    _column_null_sql_from_raw_tuples,
    _datetime64_unit_from_dtype,
    _infer_null_sql_from_raw_cells,
    _null_sql_for_pandas_dtype,
    _null_sql_for_polars_dtype,
    _pandas_dtype_needs_object_null_witness,
    _parse_schema_ddl,
    _schema_names_and_permutation,
)

from repark.spark.session.create_dataframe_rows import (
    _bind_named_row,
    _create_dataframe_from_rows,
    _create_dataframe_from_rows_inner,
    _drop_cdf_temp_view,
    _empty_frame_sql,
    _empty_typed_arrow_frame,
    _materialize_arrow_as_memtable_frame,
    _materialize_values_as_memtable_frame,
    _refuse_duplicate_pandas_columns,
    _register_cdf_view_cleanup,
    _rows_from_mapping_list,
    _rows_from_pandas,
    _rows_from_polars,
    _spark_dict_key_union_order,
    _values_sql_with_typed_nulls,
)

from repark.spark.session.create_dataframe_inference import (
    _INFER_NESTED_DICT_AS_STRUCT,
    _LEGACY_FIRST_ELEMENT_COERCE,
    _arrow_type_is_nested,
    _arrow_type_merge_label,
    _infer_arrow_type_from_python_sample,
    _infer_struct_arrow_from_dict_samples,
    _merge_inferred_arrow_types,
    _merge_struct_arrow_types,
    _normalize_nested_sql_type_aliases,
    _prepare_nested_cell,
    _sql_type_to_arrow,
    _validate_decimal_envelope,
)

from repark.spark.session.create_dataframe_arrow import (
    _arrow_null_sql_to_type,
    _arrow_table_from_pandas,
    _arrow_table_from_polars,
    _localize_naive_timestamp_column,
    _normalize_frame_arrow_column,
    _validate_decimal_column_envelope,
)

from repark.spark.session.create_dataframe_tuples import (
    _SPARK_SCALAR_MERGE_KIND_ORDER,
    _SPARK_SCALAR_MERGE_LABELS,
    _arrow_table_from_tuples,
    _python_scalar_merge_kind,
    _refuse_incompatible_scalar_merge_kinds,
    _refuse_list_element_type_merge,
    _refuse_long_double_merge,
    _values_sql_with_explicit_casts,
)

from repark.spark.session.sql_udf_parsing import (
    _parse_simple_sql_udf_call,
    _split_sql_select_list,
    _sql_strip_comments_preserve_strings,
    _sql_top_level_keyword_index,
    _sql_udf_in_nested_subquery,
)

from repark.spark.session.sql_udf_rewrite import _try_rewrite_select_list_python_udfs

from repark.spark.session.sql_udf_discovery import (
    _sql_collect_registry_udf_hits,
    _sql_find_registry_udf_calls,
    _sql_peel_select_trailing_clauses,
    _sql_residual_has_subquery,
    _sql_udf_arg_is_simple,
    _sql_udf_call_match_key,
)

from repark.spark.session.sql_udf_residual import _sql_where_residual_base_projections

from repark.spark.session.sql_udf_materialization import (
    _sql_materialize_expr_udfs,
    _sql_plan_order_by_aliases,
    _sql_udf_clean_exception,
    _sql_udf_public_error_text,
)

from repark.spark.session.sql_relations import (
    _CREATE_TABLE_PREFIX_RE,
    _CREATE_TEMP_TABLE_SQL_RE,
    _CREATE_VIEW_SQL_RE,
    _DELETE_FROM_PREFIX_RE,
    _DROP_TABLE_SQL_RE,
    _FROM_JOIN_NON_TABLE,
    _INSERT_DIRECTORY_HEAD_RE,
    _INSERT_PREFIX_RE,
    _MERGE_INTO_SQL_RE,
    _RELATION_FOLLOW_KEYWORDS,
    _SELECT_OR_WITH_HEAD_RE,
    _UPDATE_PREFIX_RE,
    _collect_cte_names,
    _find_matching_paren,
    _match_from_or_join_keyword,
    _parse_table_identifier_segments,
    _scan_sql_table_identifier_end,
    _skip_sql_ws_and_comments,
    _split_leading_sql_trivia,
    _split_leading_table_ident,
    _split_sql_table_name_list,
    _sql_mask_strings_and_comments,
    _sql_table_ref,
    _update_rest_has_set_clause,
)
