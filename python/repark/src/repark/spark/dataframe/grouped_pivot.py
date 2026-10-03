"""Pivot value helpers for the grouped facade."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from repark.errors import AnalysisException

if TYPE_CHECKING:
    from repark.spark.column import Column
    from repark.spark.dataframe.core import DataFrame


def _pivot_max_values(frame: DataFrame) -> int:
    """Return the configured pivot cardinality limit."""
    token = getattr(frame, "_alive_token", {}) or {}
    conf = token.get("builder_config") or {}
    raw = "10000"
    for key, value in conf.items():
        if str(key).casefold() == "spark.sql.pivotmaxvalues" and value is not None:
            raw = str(value)
            break
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return 10000


def _pivot_value_column_name(value: Any) -> str:
    """Return Spark's pivot output name, including NULL and boolean spellings."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _pivot_column_engine_type(frame: DataFrame, pivot_col: str) -> str | None:
    """Return the native engine type used to cast explicit pivot values."""
    frame._ensure_alive()
    target = pivot_col.casefold()
    for name, type_key, _ in frame._inner.logical_schema_fields():
        if name == pivot_col or name.casefold() == target:
            return type_key
    return None


def _pivot_recover_agg_name(aggregate: Column) -> str:
    """Recover an aggregate name after optional ``.alias(...)``.

    Explicit aliases clear ``_agg_name``. The pre-alias name remains in ``spark_display``.
    """
    if aggregate._agg_name:
        return aggregate._agg_name
    if not aggregate._is_aggregate:
        return ""
    display = aggregate.spark_display_part()
    # ``sum(x) AS total`` / ``count AS c`` — take the left of the last top-level AS.
    match = re.fullmatch(r"(?is)(.+?)\s+AS\s+(.+)", display.strip())
    if match is not None:
        return match.group(1).strip()
    return display.strip()


def _pivot_agg_output_suffix(aggregate: Column) -> str:
    """Return the output suffix for a pivot aggregate."""
    if aggregate._agg_name is None and aggregate._stable_name and aggregate._projection_name:
        return aggregate._projection_name
    recovered = _pivot_recover_agg_name(aggregate)
    return recovered or aggregate.spark_display_part()


def _pivot_is_count_distinct_name(name: str) -> bool:
    """Return whether a recovered name is a count-distinct expression."""
    return name.casefold().startswith("count(distinct ")


def _pivot_aggregate_builder(aggregate: Column) -> Callable[..., Column]:
    """Return the aggregate builder matching a pivot expression."""
    from repark.spark import functions as F  # noqa: N812

    name = _pivot_recover_agg_name(aggregate).casefold()
    # Distinct counts need a separate refusal from conditional-count rebuilding.
    if _pivot_is_count_distinct_name(name):
        raise AnalysisException(
            "pivot does not support countDistinct yet "
            f"(got {aggregate._agg_name!r}); use count/sum/avg/min/max/first/last"
        )
    if name.startswith("sum("):
        return F.sum
    if name.startswith("avg(") or name.startswith("mean("):
        return F.avg
    if name.startswith("min("):
        return F.min
    if name.startswith("max("):
        return F.max
    # GroupedData.count() uses the bare aggregate name ``count``.
    if name == "count" or name.startswith("count("):
        return F.count
    # Conditional pivot rows inject NULLs, so first and last must ignore them.
    # Alias recovery sees DataFusion first_value and last_value names.
    if name.startswith("first(") or name.startswith("first_value("):
        return lambda column: F.first(column, ignorenulls=True)
    if name.startswith("last(") or name.startswith("last_value("):
        return lambda column: F.last(column, ignorenulls=True)
    raise AnalysisException(
        f"pivot does not support aggregate {aggregate._agg_name!r} yet "
        f"(supported: sum/avg/mean/min/max/count/first/last)"
    )


def _pivot_sort_discovered_values(values: list[Any]) -> list[Any]:
    """Sort discovered pivot values with NULL values last."""
    nulls = [value for value in values if value is None]
    non_nulls = [value for value in values if value is not None]
    try:
        non_nulls.sort()
    except TypeError:
        non_nulls.sort(key=lambda value: (type(value).__name__, str(value)))
    return non_nulls + nulls


def _pivot_is_typed_scalar_inner(inner: str) -> bool:
    """Return whether ``inner`` is a typed scalar display rather than a compound expression."""
    return (
        re.fullmatch(
            r"(?is)(?:Int(?:8|16|32|64)|UInt(?:8|16|32|64)|Float(?:16|32|64)|"
            r"Utf8|LargeUtf8|Boolean|Decimal\d*(?:\(\d+,\s*\d+\))?|Date(?:32|64)|"
            r"Null)\(.*\)",
            inner.strip(),
        )
        is not None
    )


def _pivot_native_shows_typed_literal(aggregate: Column) -> bool:
    """Return whether native display distinguishes a typed literal from a named column."""
    if aggregate._agg_name is None:
        return False
    display = aggregate._inner.display_name()
    return (
        re.search(
            r"(?i)\((?:Int(?:8|16|32|64)|UInt(?:8|16|32|64)|Float(?:16|32|64)|"
            r"Utf8|LargeUtf8|Boolean|Decimal\d*|Date(?:32|64)|Null)\(",
            display,
        )
        is not None
    )


def _pivot_count_one_is_row_count(aggregate: Column) -> bool:
    """Return whether recovered ``count(1)`` denotes a measure column."""
    if aggregate._agg_name is not None:
        return aggregate._inner.display_name().casefold() != "count(1)"
    return False


def _pivot_aggregate_input(
    aggregate: Column,
    frame: DataFrame | None = None,
) -> Column:
    """Extract and rebind a simple aggregate input, refusing compound expressions."""
    from repark.spark import functions as F  # noqa: N812

    name = _pivot_recover_agg_name(aggregate)
    name_cf = name.casefold()
    # Require a word break after DISTINCT to avoid matching column names.
    if _pivot_is_count_distinct_name(name_cf):
        raise AnalysisException(
            "pivot does not support countDistinct yet "
            f"(got {name!r}); use count/sum/avg/min/max/first/last"
        )
    # Bare count and count(*) count every row under the pivot condition.
    # Keep count(1) separate because it can name a measure column.
    if name_cf in {"count", "count(*)"}:
        return F.lit(1)
    match = re.fullmatch(
        r"(?i)(sum|avg|mean|min|max|count|first_value|last_value|first|last)\((.+)\)",
        name,
    )
    if match is None:
        raise AnalysisException(
            "pivot requires aggregates with recoverable input names "
            f"(got {name!r}); use F.sum('col') form before pivot.agg"
        )
    kind = match.group(1).casefold()
    inner = match.group(2).strip()
    recovered_typed = _pivot_is_typed_scalar_inner(inner)
    native_typed = _pivot_native_shows_typed_literal(aggregate)
    if kind == "count":
        # Typed literals are non-null row-count inputs. Compound expressions are not.
        if inner == "*" or recovered_typed or native_typed:
            return F.lit(1)
        if inner == "1" and _pivot_count_one_is_row_count(aggregate):
            return F.lit(1)
    elif recovered_typed or native_typed:
        # Non-count literals must not bind digit-named measure columns.
        raise AnalysisException(
            "pivot requires simple column-name aggregate inputs "
            f"(got {name!r}); compound expressions, literals, and CAST are not supported yet"
        )
    if (inner.startswith('"') and inner.endswith('"')) or (
        inner.startswith("`") and inner.endswith("`")
    ):
        inner = inner[1:-1]
    # Simple identifiers only. Digit-leading names are valid Spark columns.
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*|[0-9][A-Za-z0-9_]*", inner):
        raise AnalysisException(
            "pivot requires simple column-name aggregate inputs "
            f"(got {name!r}); compound expressions, literals, and CAST are not supported yet"
        )
    if frame is not None:
        try:
            return frame._bind_schema_column(inner)
        except AnalysisException as bind_error:
            # Unresolvable names must fail here, not surface a later schema error.
            raise AnalysisException(
                "pivot requires simple column-name aggregate inputs "
                f"(got {name!r}); compound expressions, literals, and CAST are not supported yet"
            ) from bind_error
    return F.col(inner)
