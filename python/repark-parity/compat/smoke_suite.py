"""C2 / R-PYSPARK-COMPAT — always-green smoke pins for the Apache-suite harness.

Pins:
1. **Meta** — redirect seam installs repark into the pyspark namespace; no JVM gateway.
2. **Meta** — a known-FAIL Apache case stays classified (FAIL-MISSING), not a crash.
3. **Census subset** — every Apache test that PASSed at the X1 tip (functions+column
   growth) plus the still-green pins from other modules (types / dataframe / stretch).

Requires: installed ``pyspark`` (record extra / dev env) and the runtime-fetched
Apache test cache at ``~/.cache/repark-pyspark-tests/<tag>/`` (populated on first
``ensure_spark_tests()`` call). Skips cleanly when pyspark is absent.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

# Harness lives under python/repark-parity/compat (not the repark_parity wheel package).
_REPO_ROOT = Path(__file__).resolve().parents[3]
_COMPAT_ROOT = _REPO_ROOT / "python" / "repark-parity"
for _path in (_COMPAT_ROOT, _REPO_ROOT / "python" / "repark" / "src"):
    path_str = str(_path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from compat.bootstrap import (  # noqa: E402
    assert_no_jvm,
    install_redirect,
    is_redirect_installed,
    redirect_log,
)
from compat.classify import CENSUS_CLASSES  # noqa: E402
from compat.fetch import ensure_spark_tests  # noqa: E402
from compat.runner import run_module_inprocess  # noqa: E402

# r20 morning mega (2026-08-03): pin every Apache test verified PASS at the mega
# tip across all three census cohorts (classic five + C3 expand + C4 expand2).
# Regenerated from the census PASS union each morning mega (sole-owner).
# min(25, N) is dead (G9).
_PINNED_PASSING_APACHE_TESTS: tuple[str, ...] = (
    "pyspark.sql.tests.test_catalog.CatalogTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_collect_time",
    "pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_to_local_iterator",
    "pyspark.sql.tests.test_collection.DataFrameCollectionTests.test_to_local_iterator_prefetch",
    "pyspark.sql.tests.test_column.ColumnTests.test_access_column",
    "pyspark.sql.tests.test_column.ColumnTests.test_alias_negative",
    "pyspark.sql.tests.test_column.ColumnTests.test_and_in_expression",
    "pyspark.sql.tests.test_column.ColumnTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_column.ColumnTests.test_cast_negative",
    "pyspark.sql.tests.test_column.ColumnTests.test_cast_str_representation",
    "pyspark.sql.tests.test_column.ColumnTests.test_column_accessor",
    "pyspark.sql.tests.test_column.ColumnTests.test_column_name_encoding",
    "pyspark.sql.tests.test_column.ColumnTests.test_column_name_with_non_ascii",
    "pyspark.sql.tests.test_column.ColumnTests.test_column_operators",
    "pyspark.sql.tests.test_column.ColumnTests.test_column_select",
    "pyspark.sql.tests.test_column.ColumnTests.test_eqnullsafe_classmethod_usage",
    "pyspark.sql.tests.test_column.ColumnTests.test_isinstance_dataframe",
    "pyspark.sql.tests.test_column.ColumnTests.test_lit_time_representation",
    "pyspark.sql.tests.test_column.ColumnTests.test_transform",
    "pyspark.sql.tests.test_conf.ConfTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_conf.ConfTests.test_conf",
    "pyspark.sql.tests.test_conf.ConfTests.test_conf_with_python_objects",
    "pyspark.sql.tests.test_conf.ConfTests.test_get_all",
    "pyspark.sql.tests.test_creation.DataFrameCreationTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_creation.DataFrameCreationTests.test_create_dataframe_from_array_of_long",
    "pyspark.sql.tests.test_creation.DataFrameCreationTests.test_create_dataframe_from_pandas_with_dst",
    "pyspark.sql.tests.test_creation.DataFrameCreationTests.test_create_str_from_dict",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_cache_dataframe",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_colregex",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_column_iterator",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_count_star",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_df_show",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_drop",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_drop_col_from_different_dataframe",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_drop_duplicates",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_drop_duplicates_with_ambiguous_reference",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_drop_empty_column",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_drop_join",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_invalid_join_method",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_isinstance_dataframe",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_join_without_on",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_local_checkpoint_dataframe_with_storage_level",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_ordering_of_with_columns_renamed",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_print_schema",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_range",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_require_cross",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_same_semantics_error",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_sample",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_sample_with_random_seed",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_self_join",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_self_join_II",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_self_join_III",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_self_join_IV",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_table",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_toDF_with_string",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_union_classmethod_usage",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_where",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_with_column_with_existing_name",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_with_columns",
    "pyspark.sql.tests.test_dataframe.DataFrameTests.test_with_columns_renamed",
    "pyspark.sql.tests.test_errors.ErrorsTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_add_months_function",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_approxQuantile",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_avro_type_check",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_bool_ndarray",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_bucket",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_collect_functions",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_corr",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_cov",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_crosstab",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_date_add_function",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_date_sub_function",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_datetime_functions",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_dayname",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_dayofweek",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_empty_ndarray",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_from_csv",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_from_xml",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_greatest",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_hour",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_inverse_trig_functions",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_json_tuple_empty_fields",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_lit_list",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_lit_time",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_math_functions",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_median",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_minute",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_monthname",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_ndarray_input",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_reciprocal_trig_functions",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_regexp_replace",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_sampleby",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_schema_of_csv",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_schema_of_json",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_schema_of_xml",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_second",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_str_ndarray",
    "pyspark.sql.tests.test_functions.FunctionsTests.test_when",
    "pyspark.sql.tests.test_group.GroupTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_readwriter.ReadwriterTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_readwriter.ReadwriterTests.test_cached_table",
    "pyspark.sql.tests.test_readwriter.ReadwriterTests.test_insert_into",
    "pyspark.sql.tests.test_readwriter.ReadwriterTests.test_save",
    "pyspark.sql.tests.test_readwriter.ReadwriterV2Tests.test_assert_classic_mode",
    "pyspark.sql.tests.test_readwriter.ReadwriterV2Tests.test_create_without_provider",
    "pyspark.sql.tests.test_repartition.DataFrameRepartitionTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_repartition.DataFrameRepartitionTests.test_repartition",
    "pyspark.sql.tests.test_repartition.DataFrameRepartitionTests.test_repartition_by_id_error_invalid_num_partitions",
    "pyspark.sql.tests.test_repartition.DataFrameRepartitionTests.test_repartition_by_id_error_non_int_type",
    "pyspark.sql.tests.test_repartition.DataFrameRepartitionTests.test_repartition_by_id_out_of_range",
    "pyspark.sql.tests.test_serde.SerdeTests.test_BinaryType_serialization",
    "pyspark.sql.tests.test_serde.SerdeTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_serde.SerdeTests.test_bytes_as_binary_type",
    "pyspark.sql.tests.test_serde.SerdeTests.test_datetime_at_epoch",
    "pyspark.sql.tests.test_serde.SerdeTests.test_decimal",
    "pyspark.sql.tests.test_serde.SerdeTests.test_filter_with_datetime",
    "pyspark.sql.tests.test_serde.SerdeTests.test_filter_with_datetime_timezone",
    "pyspark.sql.tests.test_serde.SerdeTests.test_ntz_from_internal",
    "pyspark.sql.tests.test_serde.SerdeTests.test_select_null_literal",
    "pyspark.sql.tests.test_session.SparkSessionBuilderTests.test_create_spark_context_with_initial_session_options_bool",
    "pyspark.sql.tests.test_session.SparkSessionProfileTests.test_clear_memory_type",
    "pyspark.sql.tests.test_session.SparkSessionProfileTests.test_clear_no_type",
    "pyspark.sql.tests.test_session.SparkSessionProfileTests.test_clear_perf_type",
    "pyspark.sql.tests.test_session.SparkSessionProfileTests.test_dump_memory_type",
    "pyspark.sql.tests.test_session.SparkSessionProfileTests.test_dump_no_type",
    "pyspark.sql.tests.test_session.SparkSessionProfileTests.test_dump_perf_type",
    "pyspark.sql.tests.test_session.SparkSessionProfileTests.test_show_memory_type",
    "pyspark.sql.tests.test_session.SparkSessionProfileTests.test_show_no_type",
    "pyspark.sql.tests.test_session.SparkSessionProfileTests.test_show_perf_type",
    "pyspark.sql.tests.test_session.SparkSessionTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_session.SparkSessionTests1.test_assert_classic_mode",
    "pyspark.sql.tests.test_session.SparkSessionTests3.test_config_option_propagated_to_existing_session",
    "pyspark.sql.tests.test_session.SparkSessionTests3.test_create_new_session_if_old_session_stopped",
    "pyspark.sql.tests.test_session.SparkSessionTests3.test_create_new_session_with_statement",
    "pyspark.sql.tests.test_session.SparkSessionTests3.test_global_default_session",
    "pyspark.sql.tests.test_session.SparkSessionTests3.test_new_session",
    "pyspark.sql.tests.test_session.SparkSessionTests4.test_assert_classic_mode",
    "pyspark.sql.tests.test_session.SparkSessionTests4.test_get_active_session_after_create_dataframe",
    "pyspark.sql.tests.test_sql.SQLTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_sql.SQLTests.test_lit_time",
    "pyspark.sql.tests.test_sql.SQLTests.test_simple",
    "pyspark.sql.tests.test_stat.DataFrameStatTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_stat.DataFrameStatTests.test_dropna",
    "pyspark.sql.tests.test_stat.DataFrameStatTests.test_fillna",
    "pyspark.sql.tests.test_stat.DataFrameStatTests.test_melt_groupby",
    "pyspark.sql.tests.test_subquery.SubqueryTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_types.DataTypeTests.test_char_type",
    "pyspark.sql.tests.test_types.DataTypeTests.test_data_type_eq",
    "pyspark.sql.tests.test_types.DataTypeTests.test_datetype_equal_zero",
    "pyspark.sql.tests.test_types.DataTypeTests.test_decimal_type",
    "pyspark.sql.tests.test_types.DataTypeTests.test_empty_row",
    "pyspark.sql.tests.test_types.DataTypeTests.test_invalid_create_row",
    "pyspark.sql.tests.test_types.DataTypeTests.test_row_repr_with_empty_row",
    "pyspark.sql.tests.test_types.DataTypeTests.test_row_without_column_name",
    "pyspark.sql.tests.test_types.DataTypeTests.test_struct_field_type_name",
    "pyspark.sql.tests.test_types.DataTypeTests.test_timestamp_microsecond",
    "pyspark.sql.tests.test_types.DataTypeTests.test_varchar_type",
    "pyspark.sql.tests.test_types.DataTypeVerificationTests.test_row_without_field_sorting",
    "pyspark.sql.tests.test_types.DataTypeVerificationTests.test_struct_field_from_json",
    "pyspark.sql.tests.test_types.DataTypeVerificationTests.test_verify_type_exception_msg",
    "pyspark.sql.tests.test_types.DataTypeVerificationTests.test_verify_type_ok_nullable",
    "pyspark.sql.tests.test_types.TypesTests.test_array_type_from_json",
    "pyspark.sql.tests.test_types.TypesTests.test_array_types",
    "pyspark.sql.tests.test_types.TypesTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_types.TypesTests.test_cal_interval_in_collect",
    "pyspark.sql.tests.test_types.TypesTests.test_calendar_interval_type_constructor",
    "pyspark.sql.tests.test_types.TypesTests.test_convert_list_to_str",
    "pyspark.sql.tests.test_types.TypesTests.test_create_dataframe_from_dict_respects_schema",
    "pyspark.sql.tests.test_types.TypesTests.test_daytime_interval_type_constructor",
    "pyspark.sql.tests.test_types.TypesTests.test_from_ddl",
    "pyspark.sql.tests.test_types.TypesTests.test_geography_json_serde",
    "pyspark.sql.tests.test_types.TypesTests.test_geometry_json_serde",
    "pyspark.sql.tests.test_types.TypesTests.test_infer_array_element_type_empty",
    "pyspark.sql.tests.test_types.TypesTests.test_infer_map_pair_type_empty",
    "pyspark.sql.tests.test_types.TypesTests.test_infer_map_pair_type_with_nested_maps",
    "pyspark.sql.tests.test_types.TypesTests.test_infer_nested_array_element_type_with_struct",
    "pyspark.sql.tests.test_types.TypesTests.test_infer_schema_not_enough_names",
    "pyspark.sql.tests.test_types.TypesTests.test_map_type_from_json",
    "pyspark.sql.tests.test_types.TypesTests.test_merge_type",
    "pyspark.sql.tests.test_types.TypesTests.test_metadata_null",
    "pyspark.sql.tests.test_types.TypesTests.test_parse_datatype_json_string",
    "pyspark.sql.tests.test_types.TypesTests.test_repr",
    "pyspark.sql.tests.test_types.TypesTests.test_string_type_simple_string",
    "pyspark.sql.tests.test_types.TypesTests.test_struct_type",
    "pyspark.sql.tests.test_types.TypesTests.test_to_ddl",
    "pyspark.sql.tests.test_types.TypesTests.test_tree_string",
    "pyspark.sql.tests.test_types.TypesTests.test_tree_string_for_builtin_types",
    "pyspark.sql.tests.test_types.TypesTests.test_yearmonth_interval_type_constructor",
    "pyspark.sql.tests.test_udf.UDFTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_udf.UDFTests.test_nested_array",
    "pyspark.sql.tests.test_udf.UDFTests.test_single_udf_with_repeated_argument",
    "pyspark.sql.tests.test_udf.UDFTests.test_timeout_util_with_udf",
    "pyspark.sql.tests.test_udf.UDFTests.test_udf",
    "pyspark.sql.tests.test_udf.UDFTests.test_udf2",
    "pyspark.sql.tests.test_udf.UDFTests.test_udf_empty_frame",
    "pyspark.sql.tests.test_udf.UDFTests.test_udf_should_not_accept_noncallable_object",
    "pyspark.sql.tests.test_udf.UDFTests.test_udf_with_256_args",
    "pyspark.sql.tests.test_udf.UDFTests.test_udf_with_array_type",
    "pyspark.sql.tests.test_udf.UDFTests.test_udf_with_collated_string_types",
    "pyspark.sql.tests.test_udf.UDFTests.test_udf_with_column_vector",
    "pyspark.sql.tests.test_udf.UDFTests.test_udf_with_decorator",
    "pyspark.sql.tests.test_udf.UDFTests.test_udf_with_rand",
    "pyspark.sql.tests.test_utils.UtilsTests.test_assert_classic_mode",
    "pyspark.sql.tests.test_utils.UtilsTests.test_assert_equal_nulldf",
    "pyspark.sql.tests.test_utils.UtilsTests.test_assert_schema_equal_with_decimal_types",
    "pyspark.sql.tests.test_utils.UtilsTests.test_assert_unequal_null_actual",
    "pyspark.sql.tests.test_utils.UtilsTests.test_assert_unequal_null_expected",
    "pyspark.sql.tests.test_utils.UtilsTests.test_capture_analysis_exception",
    "pyspark.sql.tests.test_utils.UtilsTests.test_capture_parse_exception",
    "pyspark.sql.tests.test_utils.UtilsTests.test_schema_array_unequal",
    "pyspark.sql.tests.test_utils.UtilsTests.test_schema_ignore_nullable_array_equal",
    "pyspark.sql.tests.test_utils.UtilsTests.test_schema_ignore_nullable_struct_equal",
    "pyspark.sql.tests.test_utils.UtilsTests.test_schema_more_nested_struct_unequal",
    "pyspark.sql.tests.test_utils.UtilsTests.test_schema_struct_unequal",
    "pyspark.sql.tests.test_utils.UtilsTests.test_schema_unsupported_type",
)

# Known-FAIL meta pin: nested dotted field resolve residual (df["r.a"] / df["r.b"]).
# F2 landed mixed-type lit(list) + global regexp_replace (test_lit_list → PASS); wall moved
# off lit cast. r16 lesson: pin BOTH status AND cause-string when the wall moves.
# Pin list / exact-count assert remain morning-mega owned — not edited here.
_KNOWN_FAIL_TEST_ID = "pyspark.sql.tests.test_column.ColumnTests.test_field_accessor"
_KNOWN_FAIL_STATUS = "FAIL-VALUE"

# Exact pin count = the r22 morning-mega census PASS union (classic five 142 +
# C3 expand 40 + C4 expand2 87 = 269; sole-owner regeneration, 2026-08-03) MINUS
# 55 pandas-version-sensitive rows: Apache's pyspark.testing helpers import
# pandas.core.common._builtin_table, removed in pandas 3 — those rows PASS under
# the census venv (pandas 2.x, the record env the harness requires) but classify
# HARNESS under the uv.lock env (pandas 3.0.3) that parity-live runs. Pins must be
# always-green in the locked env; census reports record the pandas-2 PASS honestly.
# The excluded set is regenerated each morning by running the suite in the locked
# env and dropping the _builtin_table-class failures — never hand-curated.
# Dropping a pin silently re-opens charter G9.
assert len(_PINNED_PASSING_APACHE_TESTS) == 218, (
    f"smoke pin count must stay 218 always-green Apache PASSes "
    f"(got {len(_PINNED_PASSING_APACHE_TESTS)})"
)


@pytest.fixture(scope="module")
def _compat_provenance() -> object:
    """Fetch/reuse Apache tests cache once per module; install redirect."""
    provenance = ensure_spark_tests()
    install_redirect(spark_tests_root=provenance.cache_dir)
    assert_no_jvm()
    return provenance


def test_meta_redirect_installs_repark_session(_compat_provenance: object) -> None:
    """Meta-pin: after bootstrap, pyspark.sql.SparkSession is repark's class."""
    assert is_redirect_installed()
    import pyspark.sql as pyspark_sql
    import pyspark.sql.column as pyspark_column
    import pyspark.sql.dataframe as pyspark_dataframe

    from repark.spark.column import Column as ReparkColumn
    from repark.spark.dataframe import DataFrame as ReparkDataFrame
    from repark.spark.session import ReparkSession
    from repark.spark.session import SparkSession as ReparkSparkSession

    assert pyspark_sql.SparkSession is ReparkSparkSession
    assert issubclass(pyspark_sql.SparkSession, ReparkSession) or (
        pyspark_sql.SparkSession is ReparkSession
    )
    assert pyspark_sql.Column is ReparkColumn
    assert pyspark_column.Column is ReparkColumn
    assert pyspark_sql.DataFrame is ReparkDataFrame
    assert pyspark_dataframe.DataFrame is ReparkDataFrame
    log = redirect_log()
    assert any("SparkSession" in line for line in log)
    assert any("inject-package" in line for line in log)
    assert any("pyspark.sql.column.Column" in line for line in log)


def test_meta_no_jvm_gateway(_compat_provenance: object) -> None:
    """Meta-pin: redirect must not start a JVM / active SparkContext gateway."""
    assert_no_jvm()
    # Building a session through the patched factory path stays JVM-free.
    from repark.spark.session import ReparkSession, _reset_active_session_for_tests

    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("compat-smoke-no-jvm").getOrCreate()
    try:
        assert_no_jvm()
        # repark's minimal SparkContext has no _jvm.
        spark_context = session.sparkContext
        assert not hasattr(spark_context, "_jvm") or getattr(spark_context, "_jvm", None) is None
        with pytest.raises(AttributeError, match=r"out of scope|no attribute"):
            _ = spark_context.parallelize([1, 2, 3])  # type: ignore[attr-defined]
    finally:
        session.stop()
        _reset_active_session_for_tests()
    assert_no_jvm()


def test_meta_known_fail_stays_classified(_compat_provenance: object) -> None:
    """Meta-pin: a known engine gap classifies as FAIL-* (no harness crash)."""
    census = run_module_inprocess(
        "test_column",
        provenance=_compat_provenance,  # type: ignore[arg-type]
        test_filter="test_field_accessor",
    )
    assert census.rows, "expected at least the test_field_accessor row"
    row = next((item for item in census.rows if item.test_id == _KNOWN_FAIL_TEST_ID), None)
    assert row is not None, f"missing {_KNOWN_FAIL_TEST_ID} in {[r.test_id for r in census.rows]}"
    assert row.status in CENSUS_CLASSES
    assert row.status == _KNOWN_FAIL_STATUS
    # F2: lit_list mixed cast landed; wall is dotted nested-name resolve on DataFrame
    # (still FAIL-VALUE — Column.__getitem__ map path is green; df["r.a"] is not).
    cause_lower = row.cause.lower()
    assert "r.a" in cause_lower or "cannot be resolved" in cause_lower, (
        f"known-fail cause must still mention nested field resolve (got {row.cause!r})"
    )
    assert row.status != "HARNESS"
    assert row.status != "MODULE-TIMEOUT"


def _module_short(test_id: str) -> str:
    """``pyspark.sql.tests.test_dataframe.DataFrameTests.test_x`` → ``test_dataframe``."""
    parts = test_id.split(".")
    # pyspark.sql.tests.<module>.<Class>.<method>
    assert parts[0:3] == ["pyspark", "sql", "tests"], test_id
    return parts[3]


def _method_name(test_id: str) -> str:
    return test_id.rsplit(".", 1)[-1]


@pytest.mark.parametrize("test_id", _PINNED_PASSING_APACHE_TESTS)
def test_apache_pinned_pass(test_id: str, _compat_provenance: object) -> None:
    """Each census-PASS Apache test remains PASS under the harness (CI tier)."""
    module_short = _module_short(test_id)
    method = _method_name(test_id)
    census = run_module_inprocess(
        module_short,
        provenance=_compat_provenance,  # type: ignore[arg-type]
        test_filter=method,
    )
    # Filter can match multiple tests sharing a method suffix; select exact id.
    matches = [row for row in census.rows if row.test_id == test_id]
    if not matches:
        # Some method names collide across classes; accept unique method match in module.
        matches = [row for row in census.rows if row.test_id.endswith(f".{method}")]
    assert matches, (
        f"pinned test {test_id!r} not collected; got {[row.test_id for row in census.rows][:10]}"
    )
    # When filter matches several (e.g. test_assert_classic_mode on multiple classes),
    # require the exact id among them to PASS.
    exact = next((row for row in matches if row.test_id == test_id), matches[0])
    assert exact.status == "PASS", (
        f"{exact.test_id} classified {exact.status}: {exact.cause} @ {exact.divergent_frame}"
    )
