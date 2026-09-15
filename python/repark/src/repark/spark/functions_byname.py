"""By-name routine calls resolved at call time."""

from __future__ import annotations

from typing import Any

from repark.errors import AnalysisException, PySparkTypeError
from repark.spark.column import Column

BYNAME_NAMES: tuple[str, ...] = ("call_function", "call_udf")

BYNAME_NON_ROUTINE_NAMES: frozenset[str] = frozenset(
    {
        "approxCountDistinct",
        "asc",
        "asc_nulls_first",
        "asc_nulls_last",
        "arrow_udf",
        "arrow_udtf",
        "broadcast",
        "bucket",
        "call_function",
        "call_udf",
        "col",
        "column",
        "desc",
        "desc_nulls_first",
        "desc_nulls_last",
        "expr",
        "lit",
        "pandas_udf",
        "timestamp_add",
        "timestamp_diff",
        "toDegrees",
        "toRadians",
        "udf",
        "udtf",
        "when",
    }
)

FACADE_ONLY_ROUTINE_NAMES: tuple[str, ...] = (
    "add_months",
    "aggregate",
    "approx_count_distinct",
    "approx_percentile",
    "array_agg",
    "array_size",
    "avg",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bitmap_and_agg",
    "bitmap_construct_agg",
    "bitmap_or_agg",
    "bitwiseNOT",
    "bitwise_not",
    "bool_and",
    "bool_or",
    "char",
    "coalesce",
    "collect_list",
    "collect_set",
    "concat",
    "corr",
    "count",
    "countDistinct",
    "count_distinct",
    "count_if",
    "covar_pop",
    "covar_samp",
    "cume_dist",
    "curdate",
    "currentDate",
    "currentTimestamp",
    "current_catalog",
    "current_database",
    "current_schema",
    "current_timestamp",
    "current_timezone",
    "current_user",
    "date_add",
    "date_format",
    "date_from_unix_date",
    "date_sub",
    "date_trunc",
    "dateadd",
    "day",
    "dayname",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "days",
    "dense_rank",
    "e",
    "endswith",
    "equal_null",
    "every",
    "exists",
    "explode",
    "explode_outer",
    "extract",
    "filter",
    "first",
    "first_value",
    "forall",
    "format_number",
    "from_csv",
    "from_xml",
    "get",
    "grouping",
    "hash",
    "hll_sketch_agg",
    "hll_sketch_estimate",
    "hll_union",
    "hll_union_agg",
    "hours",
    "ifnull",
    "input_file_block_length",
    "input_file_block_start",
    "input_file_name",
    "is_variant_null",
    "isnotnull",
    "isnull",
    "java_method",
    "json_tuple",
    "kll_merge_agg_bigint",
    "kll_merge_agg_double",
    "kll_merge_agg_float",
    "kll_sketch_agg_bigint",
    "kll_sketch_agg_double",
    "kll_sketch_agg_float",
    "kll_sketch_get_n_bigint",
    "kll_sketch_get_n_double",
    "kll_sketch_get_n_float",
    "kll_sketch_get_quantile_bigint",
    "kll_sketch_get_quantile_double",
    "kll_sketch_get_quantile_float",
    "kll_sketch_get_rank_bigint",
    "kll_sketch_get_rank_double",
    "kll_sketch_get_rank_float",
    "kll_sketch_merge_bigint",
    "kll_sketch_merge_double",
    "kll_sketch_merge_float",
    "kll_sketch_to_string_bigint",
    "kll_sketch_to_string_double",
    "kll_sketch_to_string_float",
    "kurtosis",
    "lag",
    "last",
    "last_day",
    "last_value",
    "lcase",
    "lead",
    "left",
    "listagg",
    "log2",
    "map_contains_key",
    "map_filter",
    "map_zip_with",
    "max",
    "mean",
    "median",
    "min",
    "mode",
    "monotonically_increasing_id",
    "month",
    "monthname",
    "months",
    "named_struct",
    "negate",
    "negative",
    "now",
    "nth_value",
    "ntile",
    "nullifzero",
    "nvl",
    "nvl2",
    "parse_json",
    "percent_rank",
    "percentile_approx",
    "pi",
    "pmod",
    "posexplode",
    "posexplode_outer",
    "positive",
    "printf",
    "quarter",
    "quote",
    "raise_error",
    "rank",
    "reduce",
    "reflect",
    "regexp",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "replace",
    "right",
    "row_number",
    "schema_of_csv",
    "schema_of_variant",
    "schema_of_variant_agg",
    "schema_of_xml",
    "sentences",
    "session_user",
    "shiftLeft",
    "shiftRight",
    "shiftRightUnsigned",
    "skewness",
    "some",
    "spark_partition_id",
    "st_asbinary",
    "st_geogfromwkb",
    "st_geomfromwkb",
    "st_setsrid",
    "st_srid",
    "stack",
    "startswith",
    "std",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "string_agg",
    "struct",
    "sum",
    "theta_difference",
    "theta_intersection",
    "theta_intersection_agg",
    "theta_sketch_agg",
    "theta_sketch_estimate",
    "theta_union",
    "theta_union_agg",
    "to_csv",
    "to_variant_object",
    "to_xml",
    "transform",
    "transform_keys",
    "transform_values",
    "trunc",
    "try_avg",
    "try_parse_json",
    "try_reflect",
    "try_sum",
    "try_to_timestamp",
    "try_variant_get",
    "ucase",
    "unix_millis",
    "unix_seconds",
    "unwrap_udt",
    "user",
    "uuid",
    "var_pop",
    "var_samp",
    "variance",
    "variant_get",
    "version",
    "weekday",
    "weekofyear",
    "xpath",
    "xpath_boolean",
    "xpath_double",
    "xpath_float",
    "xpath_int",
    "xpath_long",
    "xpath_number",
    "xpath_short",
    "xpath_string",
    "year",
    "years",
    "zeroifnull",
    "zip_with",
)

_FACADE_LOWER_NAMES: dict[str, str] = {name.lower(): name for name in FACADE_ONLY_ROUTINE_NAMES}

_ENGINE_UNKNOWN_MARKER = "unsupported function"

_SQL_FUNCTIONS_DOC = "https://spark.apache.org/docs/latest/sql-ref-functions.html"


def _arity_error(text: str) -> AnalysisException | None:
    """Map engine arity text to WRONG_NUM_ARGS, else None."""
    prefix, found, rest = text.partition("call_scalar(")
    if found == "" or prefix != "":
        return None
    name, found, rest = rest.partition(") expects ")
    if found == "":
        return None
    spec, found, got = rest.partition(", got ")
    if found == "" or not got.isdigit():
        return None
    count, gap, unit = spec.partition(" ")
    if gap and unit in ("arg", "args") and count.isdigit():
        required = f"requires {count} parameters"
    else:
        required = f"accepts {spec}"
    return AnalysisException(
        f"[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{name}` {required} but the actual "
        f"number is {got}. Please, refer to '{_SQL_FUNCTIONS_DOC}' for a fix. SQLSTATE: 42605"
    )


def _resolve_routine(name: str, label: str, cols: tuple[Any, ...]) -> Any:
    """Resolve a routine name to a Column, else raise."""
    if not isinstance(name, str) or name == "":
        raise PySparkTypeError(f"call routine {label} must be a non-empty str")
    if "." in name:
        namespace = ".".join(f"`{part}`" for part in name.split(".")[:-1])
        raise AnalysisException(
            "[REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part "
            f"namespace, but got {namespace}. SQLSTATE: 42K05"
        )
    folded = name.lower()
    from repark.spark.session import ReparkSession

    session = ReparkSession.getActiveSession()
    if session is not None:
        registry = session._udf_registry()
        if registry:
            entry = registry.get(name)
            if entry is None:
                entry = registry.get(folded)
            if isinstance(entry, dict) and "udf" in entry:
                return entry["udf"](*cols)
            for key, candidate_entry in registry.items():
                if (
                    key.lower() == folded
                    and isinstance(candidate_entry, dict)
                    and "udf" in candidate_entry
                ):
                    return candidate_entry["udf"](*cols)
    if folded.startswith("_"):
        raise AnalysisException(
            f"[UNRESOLVED_ROUTINE] Cannot resolve routine `{name}` on search path "
            "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. "
            "SQLSTATE: 42883"
        )
    from repark.spark import functions as facade
    from repark.spark.functions import _scalar

    try:
        return _scalar(folded, *cols)
    except ValueError as error:
        if _ENGINE_UNKNOWN_MARKER not in str(error):
            arity = _arity_error(str(error))
            if arity is not None:
                raise arity from error
            raise
    exact = _FACADE_LOWER_NAMES.get(folded)
    candidate = facade.__dict__.get(exact) if exact is not None else None
    if candidate is not None and callable(candidate) and not isinstance(candidate, type):
        return candidate(*cols)
    raise AnalysisException(
        f"[UNRESOLVED_ROUTINE] Cannot resolve routine `{name}` on search path "
        "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883"
    )


def call_function(funcName: str, *cols: Column | str) -> Any:  # noqa: N803 — PySpark arg name
    """Call a builtin or session-registered function by name. pins: fnp-misc-1/C-002"""
    return _resolve_routine(funcName, "funcName", cols)


def call_udf(udfName: str, *cols: Column | str) -> Any:  # noqa: N803 — PySpark arg name
    """Call a session-registered UDF or builtin by name. pins: fnp-misc-1/C-002"""
    return _resolve_routine(udfName, "udfName", cols)


def install_into(namespace: dict[str, Any], exported: list[str]) -> None:
    """Copy the by-name surface onto the canonical functions module."""
    for name in BYNAME_NAMES:
        namespace[name] = globals()[name]
        if name not in exported:
            exported.append(name)
