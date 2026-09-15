"""``GroupedData.cogroup`` and ``PandasCogroupedOps`` — cogrouped pandas/Arrow map UDFs.

Bound onto ``GroupedData`` in :mod:`repark.spark.dataframe.joins_columns`.
"""

from __future__ import annotations

import contextlib
import functools
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any

import repark.spark.dataframe.grouped_arrow as grouped_arrow
import repark.spark.dataframe.grouped_udf as grouped_udf
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkException,
    PySparkTypeError,
)
from repark.spark.dataframe.udf_schema import _coerce_map_in_arrow_schema

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame
    from repark.spark.dataframe.joins_columns import GroupedData


def _iter_cogrouped_pairs(
    input_batches: Iterator[Any],
    *,
    left_key_names: list[str],
    right_frame: Any,
    right_key_names: list[str],
) -> Iterator[tuple[tuple[Any, ...], list[Any] | None, list[Any] | None]]:
    """Merge-walk two sorted group streams; one group's segments buffered per side."""
    import pyarrow as pa

    right_reader = pa.RecordBatchReader.from_stream(right_frame)
    try:
        left_groups = grouped_udf._iter_apply_in_pandas_keyed_groups(input_batches, left_key_names)
        right_groups = grouped_udf._iter_apply_in_pandas_keyed_groups(
            iter(right_reader), right_key_names
        )
        left_next = next(left_groups, None)
        right_next = next(right_groups, None)
        while left_next is not None and right_next is not None:
            left_key, left_segments = left_next
            right_key, right_segments = right_next
            order = grouped_udf._apply_in_pandas_keys_compare(left_key, right_key)
            if order == 0:
                yield (left_key, left_segments, right_segments)
                left_next = next(left_groups, None)
                right_next = next(right_groups, None)
            elif order < 0:
                yield (left_key, left_segments, None)
                left_next = next(left_groups, None)
            else:
                yield (right_key, None, right_segments)
                right_next = next(right_groups, None)
        while left_next is not None:
            left_key, left_segments = left_next
            yield (left_key, left_segments, None)
            left_next = next(left_groups, None)
        while right_next is not None:
            right_key, right_segments = right_next
            yield (right_key, None, right_segments)
            right_next = next(right_groups, None)
    finally:
        close = getattr(right_reader, "close", None)
        if callable(close):
            with contextlib.suppress(Exception):
                close()


def _cogrouped_pandas_batches(
    input_batches: Iterator[Any],
    *,
    user_func: Callable[..., Any],
    keyed: bool,
    left_key_names: list[str],
    right_frame: Any,
    right_key_names: list[str],
    left_arrow_schema: Any,
    right_arrow_schema: Any,
    expected_names: list[str],
    expected_arrow: Any,
) -> Iterator[Any]:
    """Run a cogrouped applyInPandas callback over the merged group streams."""
    import pandas as pd
    import pyarrow as pa

    empty_left = pa.Table.from_batches([], schema=left_arrow_schema)
    empty_right = pa.Table.from_batches([], schema=right_arrow_schema)
    pairs = _iter_cogrouped_pairs(
        input_batches,
        left_key_names=left_key_names,
        right_frame=right_frame,
        right_key_names=right_key_names,
    )
    for key, left_segments, right_segments in pairs:
        if left_segments is None:
            left_pdf = empty_left.to_pandas()
        else:
            left_pdf = grouped_udf._apply_in_pandas_table_from_segments(left_segments).to_pandas()
        if right_segments is None:
            right_pdf = empty_right.to_pandas()
        else:
            right_pdf = grouped_udf._apply_in_pandas_table_from_segments(right_segments).to_pandas()
        if keyed:
            out_pdf = grouped_arrow._call_grouped_user_func(
                user_func, "applyInPandas", key, left_pdf, right_pdf
            )
        else:
            out_pdf = grouped_arrow._call_grouped_user_func(
                user_func, "applyInPandas", left_pdf, right_pdf
            )
        if out_pdf is None:
            raise PySparkException(
                "applyInPandas user function must return a pandas.DataFrame (got None)"
            )
        if not isinstance(out_pdf, pd.DataFrame):
            raise PySparkException(
                "applyInPandas user function must return a pandas.DataFrame; "
                f"got {type(out_pdf).__name__}"
            )
        grouped_udf._validate_apply_in_pandas_result_columns(out_pdf, expected_names)
        yield from grouped_arrow._pandas_result_to_arrow_batches(
            out_pdf, expected_arrow, api_name="applyInPandas"
        )


def _cogrouped_arrow_batches(
    input_batches: Iterator[Any],
    *,
    user_func: Callable[..., Any],
    keyed: bool,
    left_key_names: list[str],
    right_frame: Any,
    right_key_names: list[str],
    left_arrow_schema: Any,
    right_arrow_schema: Any,
    expected_arrow: Any,
) -> Iterator[Any]:
    """Run a cogrouped applyInArrow callback over the merged group streams."""
    import pyarrow as pa

    empty_left = pa.Table.from_batches([], schema=left_arrow_schema)
    empty_right = pa.Table.from_batches([], schema=right_arrow_schema)
    pairs = _iter_cogrouped_pairs(
        input_batches,
        left_key_names=left_key_names,
        right_frame=right_frame,
        right_key_names=right_key_names,
    )
    for _key, left_segments, right_segments in pairs:
        if left_segments is None:
            left_table = empty_left
        else:
            left_table = grouped_udf._apply_in_pandas_table_from_segments(left_segments)
        if right_segments is None:
            right_table = empty_right
        else:
            right_table = grouped_udf._apply_in_pandas_table_from_segments(right_segments)
        if keyed:
            if left_segments is not None:
                key_scalars = grouped_arrow._apply_in_arrow_group_key(left_segments, left_key_names)
            else:
                key_scalars = grouped_arrow._apply_in_arrow_group_key(
                    right_segments, right_key_names
                )
            result = grouped_arrow._call_grouped_user_func(
                user_func, "applyInArrow", key_scalars, left_table, right_table
            )
        else:
            result = grouped_arrow._call_grouped_user_func(
                user_func, "applyInArrow", left_table, right_table
            )
        grouped_arrow._verify_arrow_table_result(result, expected_arrow)
        if result.num_columns == 0 and result.num_rows == 0:
            continue
        if list(result.schema.names) != list(expected_arrow.names):
            result = result.select(list(expected_arrow.names))
        yield from result.to_batches()


class PandasCogroupedOps:
    """Two ``GroupedData`` groupings cogrouped for cogrouped map UDFs (PySpark name)."""

    __slots__ = ("_gd1", "_gd2")

    def __init__(self, gd1: GroupedData, gd2: GroupedData) -> None:
        """Bind the left and right grouped operands."""
        self._gd1 = gd1
        self._gd2 = gd2

    def _prepared(self) -> tuple[Any, Any, list[str], list[str], Any, Any]:
        """Validate the cogroup and sort both sides for the merge walk."""
        left = self._gd1
        right = self._gd2
        if len(left._group_columns) != len(right._group_columns):
            raise IllegalArgumentException(
                "requirement failed: Cogroup keys must have same size: "
                f"{len(left._group_columns)} != {len(right._group_columns)}"
            )
        for grouped, other_side in ((left, "right"), (right, "left")):
            if grouped._sql_group_clause is not None:
                raise AnalysisException(
                    "cogroup after cube/rollup/grouping sets is not supported; "
                    f"cogroup the {other_side} side with groupBy(...)"
                )
            if grouped._pivot_col is not None:
                raise AnalysisException(
                    "cogroup after pivot is not supported; "
                    f"cogroup the {other_side} side with groupBy(...)"
                )
        left_frame = left._dataframe
        right_frame = right._dataframe
        left_frame._ensure_alive()
        right_frame._ensure_alive()
        left_key_names = left._apply_in_pandas_group_key_names()
        right_key_names = right._apply_in_pandas_group_key_names()
        left_sorted = left_frame.order_by(*left_key_names) if left_key_names else left_frame
        right_sorted = right_frame.order_by(*right_key_names) if right_key_names else right_frame
        left_arrow_schema = left_sorted._analyzed_arrow_schema()
        right_arrow_schema = right_sorted._analyzed_arrow_schema()
        return (
            left_sorted,
            right_sorted,
            left_key_names,
            right_key_names,
            left_arrow_schema,
            right_arrow_schema,
        )

    def applyInPandas(  # noqa: N802 — PySpark method name
        self,
        func: Callable[..., Any],
        schema: Any,
    ) -> DataFrame:
        """Apply a cogrouped pandas UDF to each key's frame pair through mapInArrow.

        Groups present on only one side call ``func`` with an empty frame of that
        side's schema. Memory is bounded by the largest pair of groups.
        """
        if not callable(func):
            raise PySparkTypeError(
                f"[NOT_CALLABLE] Argument `func` should be a callable, got {type(func).__name__}.",
                errorClass="NOT_CALLABLE",
                messageParameters={
                    "arg_name": "func",
                    "arg_type": type(func).__name__,
                },
            )
        (
            left_sorted,
            right_sorted,
            left_key_names,
            right_key_names,
            left_arrow_schema,
            right_arrow_schema,
        ) = self._prepared()
        _declared, expected_arrow = _coerce_map_in_arrow_schema(schema)
        return left_sorted.mapInArrow(
            functools.partial(
                _cogrouped_pandas_batches,
                user_func=func,
                keyed=grouped_arrow._grouped_map_positional_arity(func) == 3,
                left_key_names=left_key_names,
                right_frame=right_sorted,
                right_key_names=right_key_names,
                left_arrow_schema=left_arrow_schema,
                right_arrow_schema=right_arrow_schema,
                expected_names=list(expected_arrow.names),
                expected_arrow=expected_arrow,
            ),
            schema,
        )

    def applyInArrow(  # noqa: N802 — PySpark method name
        self,
        func: Callable[..., Any],
        schema: Any,
    ) -> DataFrame:
        """Apply a cogrouped Arrow UDF to each key's table pair through mapInArrow.

        Groups present on only one side call ``func`` with an empty table of that
        side's schema. Memory is bounded by the largest pair of groups.
        """
        if not callable(func):
            raise PySparkTypeError(
                f"[NOT_CALLABLE] Argument `func` should be a callable, got {type(func).__name__}.",
                errorClass="NOT_CALLABLE",
                messageParameters={
                    "arg_name": "func",
                    "arg_type": type(func).__name__,
                },
            )
        (
            left_sorted,
            right_sorted,
            left_key_names,
            right_key_names,
            left_arrow_schema,
            right_arrow_schema,
        ) = self._prepared()
        _declared, expected_arrow = _coerce_map_in_arrow_schema(schema)
        return left_sorted.mapInArrow(
            functools.partial(
                _cogrouped_arrow_batches,
                user_func=func,
                keyed=grouped_arrow._grouped_map_positional_arity(func) == 3,
                left_key_names=left_key_names,
                right_frame=right_sorted,
                right_key_names=right_key_names,
                left_arrow_schema=left_arrow_schema,
                right_arrow_schema=right_arrow_schema,
                expected_arrow=expected_arrow,
            ),
            schema,
        )


def cogroup(grouped: GroupedData, other: Any) -> PandasCogroupedOps:
    """``GroupedData.cogroup`` — pair two groupings for cogrouped map UDFs."""
    from repark.spark.dataframe.joins_columns import GroupedData

    if not isinstance(other, GroupedData):
        raise PySparkTypeError(
            f"[NOT_EXPECTED_TYPE] Argument `other` should be a GroupedData, got "
            f"{type(other).__name__}.",
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "other",
                "expected_type": "GroupedData",
                "arg_type": type(other).__name__,
            },
        )
    return PandasCogroupedOps(grouped, other)
