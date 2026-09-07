"""Scalar UDF select-projection rewrites through deferred mapInArrow bridges."""

from __future__ import annotations

import functools
import uuid
from typing import TYPE_CHECKING, Any

import repark.spark.dataframe.udf_window_projection as udf_window_projection
from repark.errors import (
    AnalysisException,
    PySparkTypeError,
    UnsupportedOperationException,
)
from repark.spark.dataframe.plan_collapse import _reject_partition_transform
from repark.spark.dataframe.udf_bridge import (
    _run_pandas_udf_arrow_batches,
    _run_python_udf_arrow_batches,
)
from repark.spark.types import DataType, StructField, StructType

if TYPE_CHECKING:
    from repark.spark.column import Column
    from repark.spark.dataframe.core import DataFrame


def _select_with_pandas_udfs(frame: DataFrame, items: list[Any]) -> DataFrame:
    """Rewrite scalar pandas UDF projections through one deferred ``mapInArrow`` bridge.

    Scalar UDFs run per batch; iterator UDFs receive batch iterators. Grouped aggregates require
    ``groupBy.agg`` or a window. The output schema is declared before action. Requires pandas
    at action time.
    """
    import pyarrow as pa

    from repark.spark.functions import PandasUDFColumn, PandasUDFType
    from repark.spark.types import _arrow_type_to_repark

    frame._ensure_alive()

    if any(
        isinstance(item, PandasUDFColumn)
        and int(getattr(item, "_function_type", PandasUDFType.SCALAR)) == PandasUDFType.GROUPED_AGG
        and getattr(item, "_window_spec", None) is not None
        for item in items
    ):
        return udf_window_projection._select_with_window_pandas_udfs(frame, items)

    intermediate_columns: list[Column] = []
    output_slots: list[dict[str, Any]] = []
    seen_out_names: dict[str, int] = {}

    for item in items:
        if isinstance(item, PandasUDFColumn):
            function_type = int(getattr(item, "_function_type", PandasUDFType.SCALAR))
            if function_type == PandasUDFType.GROUPED_AGG:
                raise AnalysisException(
                    "GROUPED_AGG pandas_udf cannot be used in select/withColumn without "
                    ".over(Window.partitionBy(...)); use groupBy(...).agg(pandas_udf(...)) "
                    "for non-window form, or attach an unbounded partition window via .over"
                )
            if function_type not in {
                PandasUDFType.SCALAR,
                PandasUDFType.SCALAR_ITER,
            }:
                raise UnsupportedOperationException(
                    f"pandas_udf functionType={function_type!r} is not supported in "
                    "select/withColumn (supported: SCALAR, SCALAR_ITER). "
                    "GROUPED_MAP / window pandas_udf are M6-class seeds."
                )
            input_inter_names: list[str] = []
            for input_column in item._inputs:
                _reject_partition_transform(input_column)
                if getattr(input_column, "_generator", None) is not None:
                    raise AnalysisException(
                        "pandas_udf input cannot be explode/posexplode generator; "
                        "unnest first, then apply the UDF on the expanded column "
                        f"(got generator on input to {item._function_name!r})"
                    )
                if bool(getattr(input_column, "_is_aggregate", False)):
                    raise AnalysisException(
                        "pandas_udf input cannot be an aggregate expression; "
                        "aggregate first or apply the UDF before aggregation "
                        f"(got aggregate on input to {item._function_name!r})"
                    )
                temp_name = f"__repark_pudf_in_{uuid.uuid4().hex}"
                if input_column._stable_name and not input_column._is_aggregate:
                    bound = frame._rebind_stable_name_column(input_column)
                else:
                    bound = input_column
                intermediate_columns.append(bound.alias(temp_name).for_select())
                input_inter_names.append(temp_name)
            out_name = item.output_name()
            seen_out_names[out_name] = seen_out_names.get(out_name, 0) + 1
            output_slots.append(
                {
                    "kind": "pudf",
                    "user_func": item._user_func,
                    "function_name": item._function_name,
                    "function_type": function_type,
                    "return_type_sql": item._return_type_sql,
                    "input_inter_names": input_inter_names,
                    "out_name": out_name,
                }
            )
            continue

        column = frame._column_of(item).for_select()
        if getattr(column, "_generator", None) is not None:
            raise AnalysisException(
                "pandas_udf cannot mix with explode/posexplode generators in one select; "
                "project generators and pandas_udf in separate steps"
            )
        if bool(column._is_aggregate):
            raise AnalysisException(
                "pandas_udf cannot mix with aggregate expressions in one select; "
                "aggregate first or apply the UDF before aggregation"
            )
        out_name = (
            column._projection_name
            if column._projection_name is not None
            else column.spark_display_part()
        )
        seen_out_names[out_name] = seen_out_names.get(out_name, 0) + 1
        intermediate_columns.append(column)
        output_slots.append(
            {
                "kind": "pass",
                "inter_name": out_name,
                "out_name": out_name,
            }
        )

    duplicates = [name for name, count in seen_out_names.items() if count > 1]
    if duplicates:
        raise AnalysisException(
            f"select would produce duplicate column names {duplicates}; repark requires "
            "unique projection names (DataFusion). Use .alias(...) to disambiguate. "
            "(Live PySpark allows duplicate names — disclosed Group H divergence.)"
        )
    if not intermediate_columns:
        raise PySparkTypeError("pandas_udf select produced no intermediate columns")

    intermediate = frame._spawn(
        frame._plan().select([column._inner for column in intermediate_columns])
    )

    inter_arrow_schema = intermediate._analyzed_arrow_schema()
    inter_arrow_by_name = {field.name: field for field in inter_arrow_schema}

    from repark.spark.functions import (
        _normalize_pandas_udf_return_type_sql,
        _pandas_udf_arrow_type_for_return,
    )

    struct_fields: list[StructField] = []
    expected_arrow_fields: list[Any] = []
    for slot in output_slots:
        out_name = slot["out_name"]
        if slot["kind"] == "pass":
            inter_field = inter_arrow_by_name.get(slot["inter_name"])
            if inter_field is None:
                raise AnalysisException(
                    "pandas_udf intermediate projection missing pass-through column "
                    f"{slot['inter_name']!r}"
                )
            data_type: DataType = _arrow_type_to_repark(inter_field.type)
            expected_arrow_fields.append(pa.field(out_name, inter_field.type, nullable=True))
        else:
            validated_sql = _normalize_pandas_udf_return_type_sql(slot["return_type_sql"])
            slot["return_type_sql"] = validated_sql
            data_type = DataType.fromDDL(validated_sql)
            expected_arrow_fields.append(
                pa.field(
                    out_name,
                    _pandas_udf_arrow_type_for_return(data_type),
                    nullable=True,
                )
            )
        struct_fields.append(StructField(out_name, data_type, True))
    result_schema = StructType(struct_fields)
    expected_arrow = pa.schema(expected_arrow_fields)

    needs_scalar_iter = any(
        slot["kind"] == "pudf" and slot.get("function_type") == PandasUDFType.SCALAR_ITER
        for slot in output_slots
    )

    result = intermediate.mapInArrow(
        functools.partial(
            _run_pandas_udf_arrow_batches,
            slots=output_slots,
            expected_arrow=expected_arrow,
            needs_scalar_iter=needs_scalar_iter,
        ),
        result_schema,
    )
    if result._map_bridge is not None:
        result._map_bridge["arrow_schema"] = expected_arrow
        result._map_bridge["schema"] = result_schema
    return result


def _select_with_python_udfs(frame: DataFrame, items: list[Any]) -> DataFrame:
    """Rewrite classic scalar UDF projections through a deferred ``mapInArrow`` bridge.

    The facade invokes the user function once per row and declares the output schema before
    action. This path requires PyArrow but not pandas.
    """
    import pyarrow as pa

    from repark.spark.functions import (
        PandasUDFColumn,
        PythonUDFColumn,
        _normalize_python_udf_return_type_sql,
        _python_udf_arrow_type_for_return,
    )
    from repark.spark.types import _arrow_type_to_repark

    frame._ensure_alive()

    intermediate_columns: list[Column] = []
    output_slots: list[dict[str, Any]] = []
    seen_out_names: dict[str, int] = {}

    for item in items:
        if isinstance(item, PandasUDFColumn):
            raise UnsupportedOperationException(
                "cannot mix classic udf and pandas_udf in one select/withColumn; "
                "project them in separate steps"
            )
        if isinstance(item, PythonUDFColumn):
            input_inter_names: list[str] = []
            for input_column in item._inputs:
                _reject_partition_transform(input_column)
                if getattr(input_column, "_generator", None) is not None:
                    raise AnalysisException(
                        "udf input cannot be explode/posexplode generator; "
                        "unnest first, then apply the UDF on the expanded column "
                        f"(got generator on input to {item._function_name!r})"
                    )
                if bool(getattr(input_column, "_is_aggregate", False)):
                    raise AnalysisException(
                        "udf input cannot be an aggregate expression; "
                        "aggregate first or apply the UDF before aggregation "
                        f"(got aggregate on input to {item._function_name!r})"
                    )
                temp_name = f"__repark_udf_in_{uuid.uuid4().hex}"
                if input_column._stable_name and not input_column._is_aggregate:
                    bound = frame._rebind_stable_name_column(input_column)
                else:
                    bound = input_column
                intermediate_columns.append(bound.alias(temp_name).for_select())
                input_inter_names.append(temp_name)
            out_name = item.output_name()
            seen_out_names[out_name] = seen_out_names.get(out_name, 0) + 1
            output_slots.append(
                {
                    "kind": "udf",
                    "user_func": item._user_func,
                    "function_name": item._function_name,
                    "return_type_sql": item._return_type_sql,
                    "input_inter_names": input_inter_names,
                    "out_name": out_name,
                }
            )
            continue

        column = frame._column_of(item).for_select()
        if getattr(column, "_generator", None) is not None:
            raise AnalysisException(
                "udf cannot mix with explode/posexplode generators in one select; "
                "project generators and udf in separate steps"
            )
        if bool(column._is_aggregate):
            raise AnalysisException(
                "udf cannot mix with aggregate expressions in one select; "
                "aggregate first or apply the UDF before aggregation"
            )
        out_name = (
            column._projection_name
            if column._projection_name is not None
            else column.spark_display_part()
        )
        seen_out_names[out_name] = seen_out_names.get(out_name, 0) + 1
        intermediate_columns.append(column)
        output_slots.append(
            {
                "kind": "pass",
                "inter_name": out_name,
                "out_name": out_name,
            }
        )

    duplicates = [name for name, count in seen_out_names.items() if count > 1]
    if duplicates:
        raise AnalysisException(
            f"select would produce duplicate column names {duplicates}; repark requires "
            "unique projection names (DataFusion). Use .alias(...) to disambiguate. "
            "(Live PySpark allows duplicate names — disclosed Group H divergence.)"
        )
    if not intermediate_columns:
        raise PySparkTypeError("udf select produced no intermediate columns")

    intermediate = frame._spawn(
        frame._plan().select([column._inner for column in intermediate_columns])
    )
    inter_arrow_schema = intermediate._analyzed_arrow_schema()
    inter_arrow_by_name = {field.name: field for field in inter_arrow_schema}

    struct_fields: list[StructField] = []
    expected_arrow_fields: list[Any] = []
    for slot in output_slots:
        out_name = slot["out_name"]
        if slot["kind"] == "pass":
            inter_field = inter_arrow_by_name.get(slot["inter_name"])
            if inter_field is None:
                raise AnalysisException(
                    "udf intermediate projection missing pass-through column "
                    f"{slot['inter_name']!r}"
                )
            data_type: DataType = _arrow_type_to_repark(inter_field.type)
            expected_arrow_fields.append(pa.field(out_name, inter_field.type, nullable=True))
        else:
            validated_sql = _normalize_python_udf_return_type_sql(slot["return_type_sql"])
            slot["return_type_sql"] = validated_sql
            data_type = DataType.fromDDL(validated_sql)
            expected_arrow_fields.append(
                pa.field(
                    out_name,
                    _python_udf_arrow_type_for_return(data_type),
                    nullable=True,
                )
            )
        struct_fields.append(StructField(out_name, data_type, True))
    result_schema = StructType(struct_fields)
    expected_arrow = pa.schema(expected_arrow_fields)
    result = intermediate.mapInArrow(
        functools.partial(
            _run_python_udf_arrow_batches,
            slots=output_slots,
            expected_arrow=expected_arrow,
        ),
        result_schema,
    )
    if result._map_bridge is not None:
        result._map_bridge["arrow_schema"] = expected_arrow
        result._map_bridge["schema"] = result_schema
    return result
