"""DFCORE-1 export snapshot pin: package, core, and DataFrame identity frozen pre-slice.

The expected sets below were recorded on the pre-slice tree; the move-only
relocation must leave every one of them byte-identical.

DFCORE-2 (2026-09-07) declared deltas, extended before the production edit: the
four ``_select_with_*`` helpers leave the class, so ``EXPECTED_DATAFRAME_DIR``
loses exactly those four names; ``core`` and the package each gain exactly the
two new module names ``udf_projection`` and ``udf_window_projection``.

DFCORE-3 (2026-09-07) declared deltas, extended before the production edit: the
seven statistics bodies leave for ``statistics.py`` as private frame-first
module functions, but the public methods (``approxQuantile``, ``corr``,
``cov``, ``crosstab``, ``summary``, ``describe`` on ``DataFrame``;
``freqItems`` on ``DataFrameStatFunctions``) stay as one-line wrappers, so
``EXPECTED_DATAFRAME_DIR`` is unchanged; ``core`` and the package each gain
exactly the one new module name ``statistics``.

DFCORE-4a (2026-09-07) declared deltas, extended before the production edit: the
three sampling bodies (``sample``, ``randomSplit``, ``sampleBy``) leave for
``sampling.py`` as private frame-first module functions, but the public methods
stay as one-line wrappers; the ``_prepare_sample_args`` static method leaves the
class with no wrapper, so ``EXPECTED_DATAFRAME_DIR`` loses exactly that one
name; the module-level ``_coerce_sample_seed`` moves to ``sampling.py`` and is
re-imported by ``core`` so both surfaces keep it; ``core`` and the package each
gain exactly the one new module name ``sampling``.

DFCORE-4b (2026-09-07) declared deltas, extended before the production edit: the
ten display bodies (``show``, ``__repr__``, ``_repr_html_``,
``_preview_tail_rows`` plus the six private helpers) leave for ``display.py``
as private frame-first module functions; ``show``, ``__repr__``,
``_repr_html_``, and ``_preview_tail_rows`` (called on the instance by the
styled-show pins) stay as one-line wrappers, so ``EXPECTED_DATAFRAME_DIR``
loses exactly the six leavers (``_conf_lookup``, ``_eager_eval_enabled``,
``_eager_eval_limits``, ``_normalize_show_args``, ``_render_styled_show``,
``_resolve_display_style``); ``core`` and the package each gain exactly the one
new module name ``display``. The ownership test lives in the sibling
``test_dfcore_4b_exports.py``: this file is at the default ceiling and the
gate's sanctioned out is a split, not an exception row.

DF-EXPLAIN-1 (2026-09-08): the explain rendering support splits out and
``_explain_text`` joins the class (same ``(extended, mode)`` shape beside
``explain``), so ``EXPECTED_DATAFRAME_DIR`` gains exactly ``_explain_text``;
the package gains exactly the one new module name ``explain``, while ``core``
gains the two private imports in its frozen surface and no module binding.
DF-EAGER-1 step 2 (2026-09-09): eager/lazy/compute join the class; guard trio to eager.py.
"""

from __future__ import annotations

import typing
from types import ModuleType
from typing import Any

import repark.spark.dataframe as dataframe_package
import repark.spark.dataframe.core as dataframe_core
import repark.spark.dataframe.export_errors as export_errors
import repark.spark.dataframe.grouped_udf as grouped_udf
import repark.spark.dataframe.rows_export as rows_export
import repark.spark.dataframe.udf_projection as udf_projection
import repark.spark.dataframe.udf_schema as udf_schema
import repark.spark.dataframe.udf_window_projection as udf_window_projection
from repark.spark.dataframe import DataFrame

EXPECTED_PACKAGE_EXPORTS: list[str] = [
    "AnalysisException",
    "Any",
    "Callable",
    "Column",
    "DataFrame",
    "DataFrameNaFunctions",
    "DataFrameStatFunctions",
    "DataFrameWriter",
    "DataFrameWriterV2",
    "DataType",
    "GroupedData",
    "IllegalArgumentException",
    "Iterator",
    "PySparkAttributeError",
    "PySparkException",
    "PySparkNotImplementedError",
    "PySparkTypeError",
    "PySparkValueError",
    "Row",
    "StructField",
    "StructType",
    "TYPE_CHECKING",
    "UnsupportedOperationException",
    "_APPLY_IN_PANDAS_KEY_MISSING",
    "_CACHE_MAX_BYTES_KEY",
    "_CACHE_VIEW_PREFIX",
    "_EXPLAIN_SECTION_PLAN",
    "_EXPORT_MEMORY_ERROR_MARKERS",
    "_G2_RANGE_NUMERIC_DTYPES",
    "_PYARROW_DYNAMIC_SOURCE_NOISE",
    "_QCOL_SIDE_BOUNDARY_RE",
    "_QCOL_TOKEN_RE",
    "_SEMI_JOIN_HOWS",
    "_SQL_LITERAL_KEYWORDS",
    "_STOPPED_MESSAGE",
    "_UNTYPED_NULL_ELEMENT",
    "__all__",
    "__builtins__",
    "__cached__",
    "__doc__",
    "__file__",
    "__loader__",
    "__name__",
    "__package__",
    "__path__",
    "__spec__",
    "_apply_in_pandas_keys_equal",
    "_apply_in_pandas_row_key",
    "_apply_in_pandas_scalar_key_equal",
    "_apply_in_pandas_table_from_segments",
    "_apply_ordered_window_pandas_udf",
    "_arrow_cell_to_spark_python",
    "_arrow_debug_type_to_sql",
    "_arrow_map_pairs",
    "_arrow_pa_type_label",
    "_bound_generator_array",
    "_by_name_casefold_map",
    "_cache_conf_lookup",
    "_cell_text",
    "_coerce_map_in_arrow_schema",
    "_coerce_sample_seed",
    "_collapse_identity_projection_alias",
    "_column_may_reference_names",
    "_column_widths",
    "_column_window_spec",
    "_data_type_has_required_child",
    "_decode_qcol_field",
    "_display_type_labels_from_arrow",
    "_drop_mia_temp_views",
    "_emit_join_side_columns",
    "_export_engine_error",
    "_export_error_message",
    "_export_error_message_is_noise",
    "_format_duckdb_show",
    "_format_eager_eval_table",
    "_format_polars_show",
    "_format_show_table",
    "_format_show_vertical",
    "_g2_dtype_is_range_numeric",
    "_global_agg_sql_parts",
    "_is_compound_sql_expr",
    "_is_native_pure_global_aggregate",
    "_is_numeric_type_key",
    "_iter_apply_in_pandas_group_tables",
    "_list_field_element_debug",
    "_map_in_pandas_arrow_batches",
    "_merge_path_write_tree",
    "_normalize_parquet_write_compression",
    "_normalize_subset",
    "_normalize_write_compression",
    "_null_safe_equi_join_sql",
    "_output_field_would_persist_required",
    "_pandas_udf_window_frame_bounds",
    "_parse_count_distinct_simple_names",
    "_parse_list_element_sql_type",
    "_pivot_agg_output_suffix",
    "_pivot_aggregate_builder",
    "_pivot_aggregate_input",
    "_pivot_column_engine_type",
    "_pivot_count_one_is_row_count",
    "_pivot_is_count_distinct_name",
    "_pivot_is_typed_scalar_inner",
    "_pivot_max_values",
    "_pivot_native_shows_typed_literal",
    "_pivot_recover_agg_name",
    "_pivot_sort_discovered_values",
    "_pivot_value_column_name",
    "_quote_filter_ident_token",
    "_quote_filter_idents_in_fragment",
    "_quote_ident_sql",
    "_refuse_calendar_interval_python_value",
    "_register_cache_frame",
    "_reject_aggregate_in_with_column",
    "_reject_non_numeric_range_order",
    "_reject_partition_transform",
    "_render_explain_sections",
    "_reset_dropin_warnings_for_tests",
    "_reset_writer_v2_option_warnings_for_tests",
    "_resolve_cache_max_bytes",
    "_resolve_writer_table",
    "_rewrite_join_qcol_sql",
    "_rewrite_qcol_tokens_local",
    "_run_pandas_udf_arrow_batches",
    "_run_python_udf_arrow_batches",
    "_same_object_qcol_alternation_safe",
    "_spark_array_element_to_sql",
    "_sql_embed_expr_fragment",
    "_sql_ident_bare_name",
    "_sql_option_escape",
    "_sql_string_literal",
    "_strip_internal_tighten_metadata",
    "_style_type_label",
    "_table_to_cell_rows",
    "_uniform_window_key_from_map",
    "_validate_apply_in_pandas_result_columns",
    "_validate_map_in_arrow_batch",
    "_vertical_show_warned",
    "_warn_storage_level_cosmetic_once",
    "_warn_writer_v2_option_once",
    "_window_spec_structural_key",
    "_writer_v2_option_warned",
    "actions_export",
    "annotations",
    "contextlib",
    "core",
    "functools",
    "home_view_ref",
    "joins_columns",
    "logger",
    "logging",
    "overload",
    "plan_collapse",
    "re",
    "scratch_view_name",
    "sort_nulls_first_for",
    "udf_bridge",
    "uuid",
    "warnings",
    "writer_readwriter",
]

EXPECTED_CORE_EXPORTS: list[str] = [
    "AnalysisException",
    "Any",
    "Callable",
    "Column",
    "DataFrame",
    "DataFrameNaFunctions",
    "DataFrameStatFunctions",
    "DataFrameWriter",
    "DataFrameWriterV2",
    "DataType",
    "GroupedData",
    "IllegalArgumentException",
    "Iterator",
    "PySparkAttributeError",
    "PySparkException",
    "PySparkNotImplementedError",
    "PySparkTypeError",
    "PySparkValueError",
    "Row",
    "StructField",
    "StructType",
    "TYPE_CHECKING",
    "UnsupportedOperationException",
    "_APPLY_IN_PANDAS_KEY_MISSING",
    "_CACHE_MAX_BYTES_KEY",
    "_CACHE_VIEW_PREFIX",
    "_EXPLAIN_SECTION_PLAN",
    "_EXPORT_MEMORY_ERROR_MARKERS",
    "_G2_RANGE_NUMERIC_DTYPES",
    "_PYARROW_DYNAMIC_SOURCE_NOISE",
    "_QCOL_SIDE_BOUNDARY_RE",
    "_QCOL_TOKEN_RE",
    "_SEMI_JOIN_HOWS",
    "_SQL_LITERAL_KEYWORDS",
    "_STOPPED_MESSAGE",
    "_UNTYPED_NULL_ELEMENT",
    "__all__",
    "__annotations__",
    "__builtins__",
    "__cached__",
    "__doc__",
    "__file__",
    "__loader__",
    "__name__",
    "__package__",
    "__spec__",
    "_apply_in_pandas_keys_equal",
    "_apply_in_pandas_row_key",
    "_apply_in_pandas_scalar_key_equal",
    "_apply_in_pandas_table_from_segments",
    "_apply_ordered_window_pandas_udf",
    "_arrow_cell_to_spark_python",
    "_arrow_debug_type_to_sql",
    "_arrow_map_pairs",
    "_arrow_pa_type_label",
    "_bound_generator_array",
    "_by_name_casefold_map",
    "_cache_conf_lookup",
    "_cell_text",
    "_coerce_map_in_arrow_schema",
    "_coerce_sample_seed",
    "_collapse_identity_projection_alias",
    "_column_may_reference_names",
    "_column_widths",
    "_column_window_spec",
    "_data_type_has_required_child",
    "_decode_qcol_field",
    "_display_type_labels_from_arrow",
    "_drop_mia_temp_views",
    "_emit_join_side_columns",
    "_export_engine_error",
    "_export_error_message",
    "_export_error_message_is_noise",
    "_format_duckdb_show",
    "_format_eager_eval_table",
    "_format_polars_show",
    "_format_show_table",
    "_format_show_vertical",
    "_g2_dtype_is_range_numeric",
    "_global_agg_sql_parts",
    "_is_compound_sql_expr",
    "_is_native_pure_global_aggregate",
    "_is_numeric_type_key",
    "_iter_apply_in_pandas_group_tables",
    "_list_field_element_debug",
    "_map_in_pandas_arrow_batches",
    "_merge_path_write_tree",
    "_normalize_parquet_write_compression",
    "_normalize_subset",
    "_normalize_write_compression",
    "_null_safe_equi_join_sql",
    "_output_field_would_persist_required",
    "_pandas_udf_window_frame_bounds",
    "_parse_count_distinct_simple_names",
    "_parse_list_element_sql_type",
    "_pivot_agg_output_suffix",
    "_pivot_aggregate_builder",
    "_pivot_aggregate_input",
    "_pivot_column_engine_type",
    "_pivot_count_one_is_row_count",
    "_pivot_is_count_distinct_name",
    "_pivot_is_typed_scalar_inner",
    "_pivot_max_values",
    "_pivot_native_shows_typed_literal",
    "_pivot_recover_agg_name",
    "_pivot_sort_discovered_values",
    "_pivot_value_column_name",
    "_quote_filter_ident_token",
    "_quote_filter_idents_in_fragment",
    "_quote_ident_sql",
    "_refuse_calendar_interval_python_value",
    "_register_cache_frame",
    "_reject_aggregate_in_with_column",
    "_reject_non_numeric_range_order",
    "_reject_partition_transform",
    "_render_explain_sections",
    "_reset_dropin_warnings_for_tests",
    "_reset_writer_v2_option_warnings_for_tests",
    "_resolve_cache_max_bytes",
    "_resolve_writer_table",
    "_rewrite_join_qcol_sql",
    "_rewrite_qcol_tokens_local",
    "_run_pandas_udf_arrow_batches",
    "_run_python_udf_arrow_batches",
    "_same_object_qcol_alternation_safe",
    "_spark_array_element_to_sql",
    "_sql_embed_expr_fragment",
    "_sql_ident_bare_name",
    "_sql_option_escape",
    "_sql_string_literal",
    "_strip_internal_tighten_metadata",
    "_style_type_label",
    "_table_to_cell_rows",
    "_uniform_window_key_from_map",
    "_validate_apply_in_pandas_result_columns",
    "_validate_map_in_arrow_batch",
    "_vertical_show_warned",
    "_warn_storage_level_cosmetic_once",
    "_warn_writer_v2_option_once",
    "_window_spec_structural_key",
    "_writer_v2_option_warned",
    "annotations",
    "contextlib",
    "functools",
    "home_view_ref",
    "logger",
    "logging",
    "overload",
    "re",
    "scratch_view_name",
    "sort_nulls_first_for",
    "uuid",
    "warnings",
]

EXPECTED_DATAFRAME_SLOTS: tuple[str, ...] = (
    "__weakref__",
    "_alive_token",
    "_cache_view",
    "_checkpoint_lazy",
    "_collapse_base",
    "_display_names",
    "_eager_shape",
    "_engine_names",
    "_ingest_report",
    "_inner",
    "_layer_defined",
    "_layer_map",
    "_layer_window_key",
    "_lineage_inner",
    "_map_bridge",
    "_mia_action_views",
    "_mia_cleanup_registered",
    "_mia_plan_ready",
    "_mia_temp_views",
    "_origin_map",
    "_origin_not_emitted",
    "_persist_requested",
    "_plan_id",
    "_session",
    "_source_view_name",
    "_storage_level",
    "_tighten_derived",
)

EXPECTED_DATAFRAME_ALIASES: list[tuple[str, str]] = [
    ("col_regex", "colRegex"),
    ("compute", "eager"),
    ("createGlobalTempView", "create_global_temp_view"),
    ("createOrReplaceGlobalTempView", "create_global_temp_view"),
    ("createOrReplaceTempView", "create_or_replace_temp_view"),
    ("createTempView", "create_temp_view"),
    ("cross_join", "crossJoin"),
    ("declareSorted", "declare_sorted"),
    ("dropDuplicates", "drop_duplicates"),
    ("dynamic_flatten", "dynamicFlatten"),
    ("except_all", "exceptAll"),
    ("groupBy", "group_by"),
    ("groupby", "group_by"),
    ("groupingSets", "grouping_sets"),
    ("intersect_all", "intersectAll"),
    ("is_empty", "isEmpty"),
    ("map_in_arrow", "mapInArrow"),
    ("map_in_pandas", "mapInPandas"),
    ("melt", "unpivot"),
    ("merge_into", "mergeInto"),
    ("orderBy", "order_by"),
    ("print_schema", "printSchema"),
    ("random_split", "randomSplit"),
    ("same_semantics", "sameSemantics"),
    ("select_expr", "selectExpr"),
    ("sort", "order_by"),
    ("sortWithinPartitions", "sort_within_partitions"),
    ("toArrowBatches", "to_arrow_batches"),
    ("toPandas", "to_pandas"),
    ("to_df", "toDF"),
    ("to_local_iterator", "toLocalIterator"),
    ("unionAll", "union"),
    ("unionByName", "union_by_name"),
    ("where", "filter"),
    ("withColumn", "with_column"),
    ("withColumnRenamed", "with_column_renamed"),
    ("withColumns", "with_columns"),
    ("withColumnsRenamed", "with_columns_renamed"),
    ("write_to", "writeTo"),
]

EXPECTED_DATAFRAME_DIR: list[str] = [
    "__arrow_c_stream__",
    "__class__",
    "__delattr__",
    "__dir__",
    "__doc__",
    "__eq__",
    "__format__",
    "__ge__",
    "__getattr__",
    "__getattribute__",
    "__getitem__",
    "__getstate__",
    "__gt__",
    "__hash__",
    "__init__",
    "__init_subclass__",
    "__le__",
    "__lt__",
    "__module__",
    "__ne__",
    "__new__",
    "__reduce__",
    "__reduce_ex__",
    "__repr__",
    "__setattr__",
    "__sizeof__",
    "__slots__",
    "__str__",
    "__subclasshook__",
    "__weakref__",
    "_action_inner",
    "_alive_token",
    "_analyzed_arrow_schema",
    "_apply_export_display_names",
    "_array_element_sql_type",
    "_arrow_type_may_hold_calendar_interval",
    "_arrow_type_needs_spark_python_convert",
    "_ascending_remark_flags",
    "_bind_engine_display_column",
    "_bind_schema_column",
    "_cache_view",
    "_checkpoint_lazy",
    "_collapse_base",
    "_column_of",
    "_consume_map_in_arrow_batches",
    "_cross_join_enabled",
    "_display_names",
    "_display_overlay_names",
    "_eager_shape",
    "_engine_field_for_display",
    "_engine_names",
    "_ensure_alive",
    "_ensure_mia_view_cleanup",
    "_execute_map_in_arrow_bridge",
    "_execute_map_in_arrow_bridge_ipc",
    "_explain_text",
    "_grouping_col_sql",
    "_grouping_sets_grouped",
    "_identity_child",
    "_ingest_report",
    "_inner",
    "_iter_bound_columns",
    "_iter_map_in_arrow_output",
    "_iter_rows_from_arrow_table",
    "_iter_rows_from_record_batch",
    "_iter_rows_streaming",
    "_join_on_condition_h1",
    "_layer_defined",
    "_layer_map",
    "_layer_window_key",
    "_lineage_inner",
    "_map_bridge",
    "_materialize_cache_if_needed",
    "_materialize_map_bridge_once",
    "_mia_action_views",
    "_mia_cleanup_registered",
    "_mia_plan_ready",
    "_mia_temp_views",
    "_name_of",
    "_native_for_registration",
    "_origin_map",
    "_origin_not_emitted",
    "_origin_plan_ids",
    "_persist_requested",
    "_plan",
    "_plan_id",
    "_prepare_for_plan",
    "_preview_tail_rows",
    "_quote_filter_sql_identifiers",
    "_raise_if_origin_not_emitted",
    "_raise_unemitted_qcol_tokens",
    "_rebind_origin_column",
    "_rebind_stable_name_column",
    "_refuse_tightened_iceberg_create",
    "_register_arrow_stream_as_inner",
    "_register_ipc_bytes_as_inner",
    "_remember_unemitted_right_origins",
    "_repr_html_",
    "_require_non_negative_limit",
    "_resolve_getitem_column_name",
    "_rows_from_arrow_table",
    "_select_global_aggregate_sql",
    "_select_via_qcol_sql",
    "_select_with_generator",
    "_session",
    "_sort_specs",
    "_source_view_name",
    "_spawn",
    "_spawn_preserving_identity",
    "_sql_binary_set_op",
    "_storage_level",
    "_tighten_derived",
    "_track_mia_view",
    "_try_merge_adjacent_window_layer",
    "agg",
    "alias",
    "approxQuantile",
    "cache",
    "coalesce",
    "colRegex",
    "col_regex",
    "collect",
    "columns",
    "compute",
    "corr",
    "count",
    "cov",
    "createGlobalTempView",
    "createOrReplaceGlobalTempView",
    "createOrReplaceTempView",
    "createTempView",
    "create_global_temp_view",
    "create_or_replace_temp_view",
    "create_temp_view",
    "crossJoin",
    "cross_join",
    "crosstab",
    "cube",
    "declareSorted",
    "declare_sorted",
    "describe",
    "describe_ingest",
    "distinct",
    "drop",
    "dropDuplicates",
    "drop_duplicates",
    "dropna",
    "dtypes",
    "dynamicFlatten",
    "dynamic_flatten",
    "eager",
    "exceptAll",
    "except_all",
    "explain",
    "fillna",
    "filter",
    "first",
    "groupBy",
    "group_by",
    "groupby",
    "groupingSets",
    "grouping_sets",
    "head",
    "hint",
    "intersect",
    "intersectAll",
    "intersect_all",
    "isEmpty",
    "isStreaming",
    "is_cached",
    "is_empty",
    "is_streaming",
    "join",
    "lazy",
    "limit",
    "localCheckpoint",
    "mapInArrow",
    "mapInPandas",
    "map_in_arrow",
    "map_in_pandas",
    "melt",
    "mergeInto",
    "merge_into",
    "na",
    "offset",
    "orderBy",
    "order_by",
    "persist",
    "pl",
    "printSchema",
    "print_schema",
    "randomSplit",
    "random_split",
    "repartition",
    "repartitionById",
    "repartitionByRange",
    "replace",
    "rollup",
    "sameSemantics",
    "same_semantics",
    "sample",
    "sampleBy",
    "schema",
    "select",
    "selectExpr",
    "select_expr",
    "show",
    "sort",
    "sortWithinPartitions",
    "sort_within_partitions",
    "stat",
    "storageLevel",
    "storage_level",
    "subtract",
    "summary",
    "tail",
    "take",
    "toArrow",
    "toArrowBatches",
    "toDF",
    "toJSON",
    "toLocalIterator",
    "toPandas",
    "to_arrow",
    "to_arrow_batches",
    "to_df",
    "to_local_iterator",
    "to_numpy",
    "to_pandas",
    "to_polars",
    "transform",
    "union",
    "unionAll",
    "unionByName",
    "union_by_name",
    "unpersist",
    "unpivot",
    "where",
    "withColumn",
    "withColumnRenamed",
    "withColumns",
    "withColumnsRenamed",
    "with_column",
    "with_column_renamed",
    "with_columns",
    "with_columns_renamed",
    "write",
    "writeTo",
    "write_to",
]

EXPECTED_OVERLOADED_METHODS: dict[str, int] = {"head": 2}

EXPECTED_NEW_PACKAGE_SUBMODULES: set[str] = {
    "display",
    "eager",
    "explain",
    "export_errors",
    "grouped_udf",
    "polars_cells",
    "rows_export",
    "sampling",
    "statistics",
    "udf_projection",
    "udf_schema",
    "udf_window_projection",
}

EXPECTED_NEW_CORE_SUBMODULES: set[str] = {
    "display",
    "sampling",
    "statistics",
    "udf_projection",
    "udf_window_projection",
}


def export_surface_names(module: Any) -> list[str]:
    """Return the sorted exported names of a module, minus dunders and submodules.

    Dunder attributes are interpreter state, and submodule attributes are bound by the
    import system when a re-export import executes; neither is a re-exported name. The
    package init itself copies only non-dunder names from core.
    """
    names: list[str] = []
    for name in dir(module):
        if name.startswith("__"):
            continue
        if isinstance(getattr(module, name, None), ModuleType):
            continue
        names.append(name)
    return sorted(names)


def test_package_export_set_unchanged() -> None:
    """Assert the package export surface still equals the pre-slice snapshot.

    Dunders are interpreter state (warning registries, import caches) and vary with
    test order, so the delta asserts cover non-dunder names only. The only accepted
    gain is the ten new submodule attributes (DFCORE-1's four plus DFCORE-2's
    ``udf_projection`` and ``udf_window_projection`` plus DFCORE-3's
    ``statistics`` plus DFCORE-4a's ``sampling`` plus DFCORE-4b's ``display`` plus
    DF-EXPLAIN-1's ``explain``),
    bound by the import system when core imports the new homes.
    """
    expected_surface = [
        name
        for name in EXPECTED_PACKAGE_EXPORTS
        if not name.startswith("__")
        and not isinstance(getattr(dataframe_package, name, None), ModuleType)
    ]
    assert export_surface_names(dataframe_package) == expected_surface
    gained = {
        name
        for name in set(dir(dataframe_package)) - set(EXPECTED_PACKAGE_EXPORTS)
        if not name.startswith("__")
    }
    assert gained == EXPECTED_NEW_PACKAGE_SUBMODULES
    lost = {
        name
        for name in set(EXPECTED_PACKAGE_EXPORTS) - set(dir(dataframe_package))
        if not name.startswith("__")
    }
    assert lost == set()


def test_core_export_set_unchanged() -> None:
    """Assert the core export surface still equals the pre-slice snapshot.

    Dunders are interpreter state (warning registries, import caches) and vary with
    test order, so the delta asserts cover non-dunder names only. ``__annotations__``
    is separately asserted absent: the moved constants carried core's last annotated
    module-level assignments with them. The only accepted gain is the five new
    module bindings (DFCORE-2's two plus DFCORE-3's ``statistics`` plus DFCORE-4a's
    ``sampling`` plus DFCORE-4b's ``display``): ``select``, the statistics, the
    sampling, and the display wrappers delegate to the moved helpers through them.
    DF-EXPLAIN-1 (2026-09-08) binds no module on ``core``: its two private names
    import from ``explain.py`` into the frozen ``core``/package surfaces, and the
    class gains exactly ``_explain_text``.
    """
    expected_surface = [
        name
        for name in EXPECTED_CORE_EXPORTS
        if not name.startswith("__")
        and not isinstance(getattr(dataframe_core, name, None), ModuleType)
    ]
    assert export_surface_names(dataframe_core) == expected_surface
    gained = {
        name
        for name in set(dir(dataframe_core)) - set(EXPECTED_CORE_EXPORTS)
        if not name.startswith("__")
    }
    assert gained == EXPECTED_NEW_CORE_SUBMODULES
    lost = {
        name
        for name in set(EXPECTED_CORE_EXPORTS) - set(dir(dataframe_core))
        if not name.startswith("__")
    }
    assert lost == set()
    assert "__annotations__" not in dir(dataframe_core)


def test_dataframe_identity_unchanged() -> None:
    """Assert module, slots, and slot-only storage still match the snapshot."""
    assert DataFrame.__module__ == "repark.spark.dataframe.core"
    assert DataFrame.__slots__ == EXPECTED_DATAFRAME_SLOTS
    assert DataFrame.__slots__ == dataframe_core.DataFrame.__slots__
    instance = DataFrame.__new__(DataFrame)
    assert not hasattr(instance, "__dict__")
    assert "__dict__" not in dir(instance)


def test_dataframe_aliases_unchanged() -> None:
    """Assert every alias still binds the same underlying method."""
    seen: set[str] = set()
    for alias, target in EXPECTED_DATAFRAME_ALIASES:
        assert getattr(DataFrame, alias) is getattr(DataFrame, target)
        seen.add(alias)
    found = sorted(
        name
        for name, value in vars(DataFrame).items()
        if not name.startswith("__")
        and isinstance(value, type(DataFrame.filter))
        and value.__name__ != name
    )
    assert found == sorted(seen)


def test_dataframe_overloads_unchanged() -> None:
    """Assert the overloaded methods and their variant counts still match."""
    overloaded: dict[str, int] = {}
    for name, value in vars(DataFrame).items():
        if isinstance(value, (staticmethod, classmethod)):
            value = value.__func__
        if not isinstance(value, type(DataFrame.filter)):
            continue
        variants = typing.get_overloads(value)
        if variants:
            overloaded[name] = len(variants)
    assert overloaded == EXPECTED_OVERLOADED_METHODS


def test_dataframe_dir_unchanged() -> None:
    """Assert the non-dunder DataFrame attribute set still equals the snapshot.

    Class dunders are interpreter and stdlib-cache state: ``copyreg`` memoizes
    ``__slotnames__`` on the first pickle or copy of a frame, so the raw set varies
    with test order. Module, slots, and instance storage stay pinned by the identity
    test.
    """
    expected_names = [name for name in EXPECTED_DATAFRAME_DIR if not name.startswith("__")]
    current_names = sorted(name for name in dir(DataFrame) if not name.startswith("__"))
    assert current_names == expected_names


MOVED_HELPERS: dict[ModuleType, tuple[str, ...]] = {
    rows_export: (
        "_arrow_cell_to_spark_python",
        "_arrow_map_pairs",
        "_refuse_calendar_interval_python_value",
    ),
    export_errors: (
        "_EXPORT_MEMORY_ERROR_MARKERS",
        "_PYARROW_DYNAMIC_SOURCE_NOISE",
        "_export_engine_error",
        "_export_error_message",
        "_export_error_message_is_noise",
    ),
    udf_schema: ("_coerce_map_in_arrow_schema", "_validate_map_in_arrow_batch"),
    grouped_udf: (
        "_APPLY_IN_PANDAS_KEY_MISSING",
        "_apply_in_pandas_keys_equal",
        "_apply_in_pandas_row_key",
        "_apply_in_pandas_scalar_key_equal",
        "_apply_in_pandas_table_from_segments",
        "_iter_apply_in_pandas_group_tables",
        "_validate_apply_in_pandas_result_columns",
    ),
}


def test_moved_helpers_are_reexported_by_identity() -> None:
    """Every moved helper on core and on the package is the leaf module's own object."""
    for home, names in MOVED_HELPERS.items():
        for name in names:
            assert getattr(dataframe_core, name) is getattr(home, name), name
            assert getattr(dataframe_package, name) is getattr(home, name), name


MOVED_SELECT_HELPERS: dict[ModuleType, tuple[str, ...]] = {
    udf_projection: ("_select_with_pandas_udfs", "_select_with_python_udfs"),
    udf_window_projection: (
        "_select_with_window_pandas_udfs",
        "_select_with_ordered_window_pandas_udfs",
    ),
}


def test_moved_select_helpers_live_in_new_homes() -> None:
    """DFCORE-2: each moved select helper is its new home's own module function.

    The helpers take the frame as their first argument; the class keeps no copy.
    """
    import inspect

    for home, names in MOVED_SELECT_HELPERS.items():
        for name in names:
            helper = getattr(home, name)
            assert inspect.isfunction(helper), name
            assert helper.__module__ == home.__name__, name
            assert next(iter(inspect.signature(helper).parameters)) == "frame", name
            assert name not in vars(DataFrame), name


MOVED_STATISTICS_HELPERS: tuple[str, ...] = (
    "_approx_quantile",
    "_corr",
    "_cov",
    "_crosstab",
    "_describe",
    "_freq_items",
    "_summary",
)


def test_moved_statistics_helpers_live_in_new_home() -> None:
    """DFCORE-3: each moved statistics body is ``statistics``' own module function.

    The helpers take the frame as their first argument. Unlike DFCORE-2, the
    public methods stay on the class as one-line wrappers, so the class dir is
    unchanged; the leaf import stays function-local so the pin file never binds
    the moved home at collection time.
    """
    import inspect

    import repark.spark.dataframe.statistics as statistics

    for name in MOVED_STATISTICS_HELPERS:
        helper = getattr(statistics, name)
        assert inspect.isfunction(helper), name
        assert helper.__module__ == statistics.__name__, name
        assert next(iter(inspect.signature(helper).parameters)) == "frame", name
    assert "statistics" in dir(dataframe_core)
    assert "statistics" in dir(dataframe_package)


MOVED_SAMPLING_HELPERS: tuple[str, ...] = (
    "_sample",
    "_random_split",
    "_sample_by",
)


def test_moved_sampling_helpers_live_in_new_home() -> None:
    """DFCORE-4a: each moved sampling body is ``sampling``'s own module function.

    The three method bodies take the frame as their first argument. The two
    receiverless helpers move verbatim: ``_prepare_sample_args`` keeps its
    parameter list (it leaves the class with no wrapper) and
    ``_coerce_sample_seed`` is re-imported by ``core`` so both surfaces keep it
    by identity. The leaf import stays function-local so the pin file never
    binds the moved home at collection time.
    """
    import inspect

    import repark.spark.dataframe.sampling as sampling

    for name in MOVED_SAMPLING_HELPERS:
        helper = getattr(sampling, name)
        assert inspect.isfunction(helper), name
        assert helper.__module__ == sampling.__name__, name
        assert next(iter(inspect.signature(helper).parameters)) == "frame", name
    normalizer = sampling._prepare_sample_args
    assert inspect.isfunction(normalizer)
    assert normalizer.__module__ == sampling.__name__
    assert list(inspect.signature(normalizer).parameters) == [
        "withReplacement",
        "fraction",
        "seed",
    ]
    assert "_prepare_sample_args" not in vars(DataFrame)
    assert dataframe_core._coerce_sample_seed is sampling._coerce_sample_seed
    assert dataframe_package._coerce_sample_seed is sampling._coerce_sample_seed
    assert "sampling" in dir(dataframe_core)
    assert "sampling" in dir(dataframe_package)
