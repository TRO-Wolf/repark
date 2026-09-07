"""Windowed GROUPED_AGG select-projection rewrites through deferred Arrow bridges."""

from __future__ import annotations

import contextlib
import functools
from typing import TYPE_CHECKING, Any

from repark.errors import (
    AnalysisException,
    PySparkTypeError,
    UnsupportedOperationException,
)
from repark.spark._temp_views import scratch_view_name
from repark.spark.column import Column
from repark.spark.dataframe.joins_columns import GroupedData
from repark.spark.dataframe.plan_collapse import (
    _null_safe_equi_join_sql,
    _pandas_udf_window_frame_bounds,
)
from repark.spark.dataframe.udf_bridge import _apply_ordered_window_pandas_udf

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame


def _select_with_window_pandas_udfs(frame: DataFrame, items: list[Any]) -> DataFrame:
    """Lower windowed ``GROUPED_AGG`` expressions through a deferred Arrow bridge.

    Partition-only windows use grouped aggregation. Ordered windows use rows-frame slices.
    Mixed partition-key sets and mixed windowed/non-windowed UDFs are refused.
    """
    from repark.spark.functions import PandasUDFColumn, PandasUDFType

    frame._ensure_alive()

    windowed: list[PandasUDFColumn] = []
    plain_items: list[Any] = []
    for item in items:
        if isinstance(item, PandasUDFColumn):
            function_type = int(getattr(item, "_function_type", PandasUDFType.SCALAR))
            if (
                function_type == PandasUDFType.GROUPED_AGG
                and getattr(item, "_window_spec", None) is not None
            ):
                windowed.append(item)
            elif function_type in {PandasUDFType.SCALAR, PandasUDFType.SCALAR_ITER}:
                raise UnsupportedOperationException(
                    "mixing scalar/SCALAR_ITER pandas_udf with windowed GROUPED_AGG in "
                    "one select is not supported in repark v1; materialize window columns "
                    "first, then apply scalar pandas_udf"
                )
            elif function_type == PandasUDFType.GROUPED_AGG:
                raise AnalysisException(
                    "GROUPED_AGG pandas_udf in select requires .over(Window.partitionBy(...)) "
                    "or use groupBy(...).agg(...)"
                )
            else:
                raise UnsupportedOperationException(
                    f"pandas_udf functionType={function_type!r} is not supported in "
                    "windowed select (GROUPED_MAP is an M7-class seed)"
                )
        else:
            plain_items.append(item)

    if not windowed:
        raise AnalysisException(
            "internal: _select_with_window_pandas_udfs with no windowed markers"
        )

    partition_key_lists: list[list[str]] = []
    order_key_lists: list[list[str]] = []
    frame_bounds_list: list[tuple[int | None, int | None]] = []
    for marker in windowed:
        spec = marker._window_spec
        keys: list[str] = []
        for column in list(getattr(spec, "_partition_columns", []) or []):
            if column._stable_name and column._projection_name is not None:
                keys.append(column._projection_name)
            else:
                raise AnalysisException(
                    "windowed pandas_udf partitionBy requires simple column-name keys "
                    f"(got non-NamedExpression {column.spark_display_part()!r}); "
                    "project the expression first"
                )
        if not keys:
            raise UnsupportedOperationException(
                "windowed pandas_udf requires Window.partitionBy(...) with at least one key"
            )
        partition_key_lists.append(keys)
        order_keys: list[str] = []
        for column in list(getattr(spec, "_order_columns", []) or []):
            if column._stable_name and column._projection_name is not None:
                order_keys.append(column._projection_name)
            else:
                raise AnalysisException(
                    "windowed pandas_udf orderBy requires simple column-name keys "
                    f"(got non-NamedExpression {column.spark_display_part()!r}); "
                    "project the expression first"
                )
        order_key_lists.append(order_keys)
        frame_bounds_list.append(_pandas_udf_window_frame_bounds(spec))
    first_keys = partition_key_lists[0]
    for keys in partition_key_lists[1:]:
        if keys != first_keys:
            raise UnsupportedOperationException(
                "windowed pandas_udf markers in one select must share the same "
                f"partitionBy keys (got {first_keys!r} vs {keys!r})"
            )
    key_names = list(first_keys)
    has_order = any(bool(order_keys) for order_keys in order_key_lists)
    if has_order and not all(bool(order_keys) for order_keys in order_key_lists):
        raise UnsupportedOperationException(
            "windowed pandas_udf markers in one select must all use orderBy or all omit it"
        )
    if has_order:
        first_order = order_key_lists[0]
        first_frame = frame_bounds_list[0]
        for order_keys, frame_bounds in zip(
            order_key_lists[1:], frame_bounds_list[1:], strict=True
        ):
            if order_keys != first_order or frame_bounds != first_frame:
                raise UnsupportedOperationException(
                    "windowed pandas_udf markers in one select must share the same "
                    "orderBy keys and rows frame bounds"
                )
        return _select_with_ordered_window_pandas_udfs(
            frame,
            items=items,
            windowed=windowed,
            plain_items=plain_items,
            key_names=key_names,
            order_names=list(first_order),
            frame_start=first_frame[0],
            frame_end=first_frame[1],
        )

    bare_markers: list[PandasUDFColumn] = []
    window_out_names: list[str] = []
    for marker in windowed:
        bare = PandasUDFColumn(
            marker._user_func,
            marker._return_type_sql,
            marker._inputs,
            marker._function_name,
            alias_name=marker._alias_name,
            function_type=marker._function_type,
            window_spec=None,
        )
        bare_markers.append(bare)
        window_out_names.append(bare.output_name())

    grouped = GroupedData(frame, [frame._bind_schema_column(name) for name in key_names])
    agg_frame = grouped._agg_via_pandas_udfs(tuple(bare_markers))

    if plain_items:
        source_projections: list[Any] = list(plain_items)
        plain_names: set[str] = set()
        for item in plain_items:
            if isinstance(item, str):
                plain_names.add(item)
            elif isinstance(item, Column) and item._projection_name is not None:
                plain_names.add(item._projection_name)
        for key_name in key_names:
            if key_name not in plain_names:
                source_projections.append(frame._bind_schema_column(key_name))
        left = frame.select(*source_projections)
    else:
        left = frame

    session = frame._session
    left_view = scratch_view_name(session, "__repark_win_l_")
    agg_view = scratch_view_name(session, "__repark_win_a_")
    out_view = scratch_view_name(session, "__repark_win_o_")
    try:
        left._prepare_for_plan()
        agg_frame._prepare_for_plan()
        session.materialize_as_temp_view(left_view, left._inner)
        session.materialize_as_temp_view(agg_view, agg_frame._inner)
        left_clean = frame._spawn(session.sql(f"SELECT * FROM {left_view}"))
        agg_clean = frame._spawn(session.sql(f"SELECT * FROM {agg_view}"))

        if plain_items:
            final_names: list[str] = []
            window_iter = iter(window_out_names)
            for item in items:
                if isinstance(item, PandasUDFColumn):
                    name = next(window_iter)
                elif isinstance(item, str):
                    name = item
                elif isinstance(item, Column):
                    name = (
                        item._projection_name
                        if item._projection_name is not None
                        else item.spark_display_part()
                    )
                else:
                    raise PySparkTypeError(
                        "select item type "
                        f"{type(item).__name__} unsupported with windowed pandas_udf"
                    )
                final_names = [prior for prior in final_names if prior != name]
                final_names.append(name)
        else:
            final_names = list(window_out_names)

        join_sql = _null_safe_equi_join_sql(
            left_view,
            agg_view,
            list(key_names),
            final_names,
            left_column_names=list(left_clean.columns),
            right_column_names=list(agg_clean.columns),
            prefer_right_names=set(window_out_names),
        )
        joined = frame._spawn(session.sql(join_sql))
        joined._prepare_for_plan()
        session.materialize_as_temp_view(out_view, joined._inner)
        return frame._spawn(session.sql(f"SELECT * FROM {out_view}"))
    finally:
        with contextlib.suppress(Exception):
            session.drop_temp_view(left_view)
        with contextlib.suppress(Exception):
            session.drop_temp_view(agg_view)


def _select_with_ordered_window_pandas_udfs(
    frame: DataFrame,
    *,
    items: list[Any],
    windowed: list[Any],
    plain_items: list[Any],
    key_names: list[str],
    order_names: list[str],
    frame_start: int | None,
    frame_end: int | None,
) -> DataFrame:
    """Apply a grouped aggregate over each ordered rows-frame partition.

    Frame offsets are relative to the current row (Spark ``rowsBetween``): ``None``
    means unbounded on that side; ``0`` is current row; negative is preceding.
    Default bounds are ``(None, 0)``: UNBOUNDED PRECEDING to CURRENT ROW. User UDF
    code runs over each frame slice.
    """
    from repark.spark.functions import (
        PandasUDFColumn,
        _normalize_pandas_udf_return_type_sql,
        _pandas_udf_arrow_type_for_return,
    )
    from repark.spark.types import DataType, StructField, StructType

    window_out_names: list[str] = []
    udf_specs: list[dict[str, Any]] = []
    for marker in windowed:
        out_name = marker.output_name()
        window_out_names.append(out_name)
        input_names: list[str] = []
        for column in marker._inputs:
            if column._stable_name and column._projection_name is not None:
                input_names.append(column._projection_name)
            else:
                raise AnalysisException(
                    "windowed pandas_udf inputs require simple column-name args "
                    f"(got {column.spark_display_part()!r}); project first"
                )
        udf_specs.append(
            {
                "user_func": marker._user_func,
                "function_name": marker._function_name,
                "return_type_sql": marker._return_type_sql,
                "out_name": out_name,
                "input_names": input_names,
            }
        )

    needed: list[str] = []
    for name in [*key_names, *order_names]:
        if name not in needed:
            needed.append(name)
    for spec in udf_specs:
        for name in spec["input_names"]:
            if name not in needed:
                needed.append(name)
    source_cols = list(frame.columns)
    for name in source_cols:
        if name not in needed:
            needed.append(name)
    projected = frame.select(*[frame._bind_schema_column(name) for name in needed])

    struct_fields: list[StructField] = []
    for field in projected.schema.fields:
        struct_fields.append(StructField(field.name, field.dataType, field.nullable))
    for spec in udf_specs:
        validated_sql = _normalize_pandas_udf_return_type_sql(spec["return_type_sql"])
        spec["return_type_sql"] = validated_sql
        data_type = DataType.fromDDL(validated_sql)
        _pandas_udf_arrow_type_for_return(data_type)
        struct_fields = [field for field in struct_fields if field.name != spec["out_name"]]
        struct_fields.append(StructField(spec["out_name"], data_type, True))
    result_schema = StructType(struct_fields)

    group_cols = [projected._bind_schema_column(name) for name in key_names]
    grouped = GroupedData(projected, group_cols)
    result = grouped.applyInPandas(
        functools.partial(
            _apply_ordered_window_pandas_udf,
            specs=udf_specs,
            order_cols=list(order_names),
            start_bound=frame_start,
            end_bound=frame_end,
            struct_fields=struct_fields,
        ),
        result_schema,
    )

    if plain_items:
        final_names: list[str] = []
        window_iter = iter(window_out_names)
        for item in items:
            if isinstance(item, PandasUDFColumn):
                name = next(window_iter)
            elif isinstance(item, str):
                name = item
            elif isinstance(item, Column):
                name = (
                    item._projection_name
                    if item._projection_name is not None
                    else item.spark_display_part()
                )
            else:
                raise PySparkTypeError(
                    f"select item type {type(item).__name__} unsupported with windowed pandas_udf"
                )
            final_names = [prior for prior in final_names if prior != name]
            final_names.append(name)
        return result.select(*final_names)
    return result
