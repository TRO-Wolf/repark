"""Grouped map UDF bridges: ``apply``, ``applyInArrow``, and the state-API refusals.

Bound onto ``GroupedData`` in :mod:`repark.spark.dataframe.joins_columns`.
"""

from __future__ import annotations

import functools
import inspect
import traceback
import typing
import warnings
from collections.abc import Callable, Iterator
from types import MethodType
from typing import TYPE_CHECKING, Any, NoReturn

import repark.spark.dataframe.grouped_udf as grouped_udf
from repark.errors import (
    AnalysisException,
    PySparkException,
    PySparkNotImplementedError,
    PySparkRuntimeError,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark._integral import (
    _attached_error_class,
    _attached_message_parameters,
    _attached_sql_state,
)
from repark.spark.dataframe.udf_schema import _coerce_map_in_arrow_schema

if TYPE_CHECKING:
    from repark.spark.dataframe.joins_columns import GroupedData


def _raise_with_spark_class(
    error: BaseException,
    error_class: str,
    message_parameters: dict[str, str],
) -> NoReturn:
    """Raise a native exception carrying Spark's errorClass and messageParameters."""
    error._spark_error_class = error_class
    error._spark_message_parameters = message_parameters
    error._spark_sql_state = None
    error.getErrorClass = MethodType(_attached_error_class, error)
    error.getCondition = MethodType(_attached_error_class, error)
    error.getMessageParameters = MethodType(_attached_message_parameters, error)
    error.getSqlState = MethodType(_attached_sql_state, error)
    raise error


def _call_grouped_user_func(user_func: Callable[..., Any], api_name: str, *args: Any) -> Any:
    """Invoke a grouped map callback, wrapping non-PySpark failures."""
    try:
        return user_func(*args)
    except PySparkException:
        raise
    except Exception as error:
        detail = traceback.format_exc()
        raise PySparkException(
            f"{api_name} user function raised {type(error).__name__}: {error}\n{detail}"
        ) from error


def _pandas_result_to_arrow_batches(
    out_pdf: Any,
    expected_arrow: Any,
    *,
    api_name: str,
) -> Iterator[Any]:
    """Convert a validated pandas result into Arrow batches for the bridge."""
    import pyarrow as pa

    if len(out_pdf) == 0 and len(out_pdf.columns) == 0:
        yield pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in expected_arrow],
            schema=expected_arrow,
        )
        return
    try:
        out_table = pa.Table.from_pandas(out_pdf, schema=expected_arrow, preserve_index=False)
    except (
        pa.ArrowInvalid,
        pa.ArrowTypeError,
        pa.ArrowNotImplementedError,
        ValueError,
        TypeError,
        KeyError,
    ) as error:
        error_text = str(error)
        if (
            "Conversion failed" in error_text
            or "not in range" in error_text
            or "Could not convert" in error_text
        ):
            raise PySparkException(
                f"{api_name} failed converting pandas output to declared schema: {error}"
            ) from error
        try:
            out_table = pa.Table.from_pandas(out_pdf, preserve_index=False)
        except Exception:
            raise PySparkException(
                f"{api_name} failed converting pandas output to Arrow: {error}"
            ) from error
    output_batches = out_table.to_batches()
    if not output_batches:
        yield pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in out_table.schema],
            schema=out_table.schema,
        )
    else:
        yield from output_batches


def _apply_in_pandas_arrow_batches(
    input_batches: Iterator[Any],
    *,
    user_func: Callable[[Any], Any],
    key_names: list[str],
    expected_names: list[str],
    expected_arrow: Any,
) -> Iterator[Any]:
    """Run an applyInPandas callback for each streamed Arrow group."""
    import pandas as pd

    for group_table in grouped_udf._iter_apply_in_pandas_group_tables(input_batches, key_names):
        pdf = group_table.to_pandas()
        out_pdf = _call_grouped_user_func(user_func, "applyInPandas", pdf)
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
        yield from _pandas_result_to_arrow_batches(
            out_pdf, expected_arrow, api_name="applyInPandas"
        )


def grouped_apply(grouped: GroupedData, udf: Any) -> Any:
    """``GroupedData.apply``: GROUPED_MAP pandas UDFs delegate to applyInPandas.

    pins: grouped-surface-1/C-001
    """
    from repark.spark.column import Column
    from repark.spark.functions import PandasUDFType

    eval_type = getattr(udf, "evalType", getattr(udf, "_function_type", None))
    user_func = getattr(udf, "func", getattr(udf, "_user_func", None))
    if isinstance(udf, Column) or user_func is None or eval_type != PandasUDFType.GROUPED_MAP:
        raise PySparkTypeError(
            "[INVALID_UDF_EVAL_TYPE] Eval type for UDF must be SQL_GROUPED_MAP_PANDAS_UDF.",
            errorClass="INVALID_UDF_EVAL_TYPE",
            messageParameters={"eval_type": "SQL_GROUPED_MAP_PANDAS_UDF"},
        )
    warnings.warn(
        "It is preferred to use 'applyInPandas' over this API. This API will be "
        "deprecated in the future releases. See SPARK-28264 for more details.",
        UserWarning,
        stacklevel=2,
    )
    return_type = getattr(udf, "_return_type_sql", getattr(udf, "returnType", None))
    return grouped.applyInPandas(user_func, return_type)


def _grouped_map_positional_arity(func: Callable[..., Any]) -> int:
    """Count positional parameters the way Spark does (``len(argspec.args)``)."""
    try:
        spec = inspect.getfullargspec(func)
    except TypeError:
        return 1
    return len(spec.args)


def _is_iterator_of_record_batches(annotation: Any) -> bool:
    """True for ``Iterator[pa.RecordBatch]`` per Spark's check_iterator_annotation."""
    import pyarrow as pa

    name = getattr(annotation, "_name", getattr(annotation, "__name__", None))
    if name != "Iterator":
        return False
    args = getattr(annotation, "__args__", None) or ()
    return all(arg == pa.RecordBatch for arg in args)


def _infer_apply_in_arrow_is_iter(func: Callable[..., Any]) -> bool:
    """Mirror ``infer_group_arrow_eval_type_from_func`` reduced to the iter flag."""
    spec = inspect.getfullargspec(func)
    if not spec.args or not spec.annotations:
        return False
    try:
        type_hints = typing.get_type_hints(func)
    except Exception:
        type_hints = {}
    signature = inspect.signature(func)
    annotations: dict[str, Any] = {}
    for parameter in signature.parameters.values():
        if parameter.annotation is not parameter.empty:
            annotations[parameter.name] = type_hints.get(parameter.name, parameter.annotation)
    hinted = [annotations[name] for name in signature.parameters if name in annotations]
    if len(hinted) != len(signature.parameters):
        raise PySparkValueError(
            errorClass="TYPE_HINT_SHOULD_BE_SPECIFIED",
            messageParameters={
                "target": "all parameters",
                "sig": str(signature),
            },
        )
    return_hint = type_hints.get("return", signature.return_annotation)
    if return_hint is signature.empty:
        raise PySparkValueError(
            errorClass="TYPE_HINT_SHOULD_BE_SPECIFIED",
            messageParameters={
                "target": "the return type",
                "sig": str(signature),
            },
        )
    if len(hinted) == 1 and _is_iterator_of_record_batches(hinted[0]):
        return _is_iterator_of_record_batches(return_hint)
    if len(hinted) == 2 and _is_iterator_of_record_batches(hinted[1]):
        return _is_iterator_of_record_batches(return_hint)
    return False


def _apply_in_arrow_group_key(segments: list[Any], key_names: list[str]) -> tuple[Any, ...]:
    """Tuple of ``pyarrow.Scalar`` key cells read from the group's first segment."""
    if not key_names:
        return ()
    return tuple(segments[0].column(name)[0] for name in key_names)


def _verify_arrow_result_schema(result: Any, expected_arrow: Any) -> None:
    """Spark's ``verify_arrow_result``: empty-accept, name set, then per-name types."""
    if result.num_columns == 0 and result.num_rows == 0:
        return
    actual = dict(zip(result.schema.names, result.schema.types, strict=False))
    expected_names = list(expected_arrow.names)
    missing = sorted(name for name in expected_names if name not in actual)
    extra = sorted(name for name in actual if name not in set(expected_names))
    if missing or extra:
        missing_text = f" Missing: {', '.join(missing)}." if missing else ""
        extra_text = f" Unexpected: {', '.join(extra)}." if extra else ""
        raise PySparkRuntimeError(
            "[RESULT_COLUMN_NAMES_MISMATCH] Column names of the returned data do not "
            f"match specified schema.{missing_text}{extra_text}",
            errorClass="RESULT_COLUMN_NAMES_MISMATCH",
            messageParameters={"missing": missing_text, "extra": extra_text},
        )
    mismatches = [
        (name, expected_arrow.field(name).type, actual[name])
        for name in sorted(expected_names)
        if actual[name] != expected_arrow.field(name).type
    ]
    if mismatches:
        detail = ", ".join(
            f"column '{name}' (expected {expected}, actual {actual_type})"
            for name, expected, actual_type in mismatches
        )
        raise PySparkRuntimeError(
            "[RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not "
            f"match specified schema. Mismatch: {detail}.",
            errorClass="RESULT_COLUMN_TYPES_MISMATCH",
            messageParameters={"mismatch": detail},
        )


def _verify_arrow_table_result(result: Any, expected_arrow: Any) -> None:
    """Spark's ``verify_arrow_table``: pa.Table check then result schema check."""
    import pyarrow as pa

    if not isinstance(result, pa.Table):
        raise PySparkTypeError(
            "[UDF_RETURN_TYPE] Return type of the user-defined function should be "
            f"pyarrow.Table, but is {type(result).__name__}.",
            errorClass="UDF_RETURN_TYPE",
            messageParameters={
                "expected": "pyarrow.Table",
                "actual": type(result).__name__,
            },
        )
    _verify_arrow_result_schema(result, expected_arrow)


def _verify_arrow_batch_result(batch: Any, expected_arrow: Any) -> None:
    """Spark's ``verify_arrow_batch`` for iterator-form yielded batches."""
    import pyarrow as pa

    if not isinstance(batch, pa.RecordBatch):
        raise PySparkTypeError(
            "[UDF_RETURN_TYPE] Return type of the user-defined function should be "
            f"pyarrow.RecordBatch, but is {type(batch).__name__}.",
            errorClass="UDF_RETURN_TYPE",
            messageParameters={
                "expected": "pyarrow.RecordBatch",
                "actual": type(batch).__name__,
            },
        )
    _verify_arrow_result_schema(batch, expected_arrow)


def _iter_apply_in_arrow_results(result: Any, expected_arrow: Any) -> Iterator[Any]:
    """Verify and reorder each yielded batch from an iterator-form callback."""
    try:
        for batch in result:
            _verify_arrow_batch_result(batch, expected_arrow)
            if batch.num_columns == 0 and batch.num_rows == 0:
                continue
            if list(batch.schema.names) != list(expected_arrow.names):
                batch = batch.select(list(expected_arrow.names))
            yield batch
    except PySparkException:
        raise
    except Exception as error:
        detail = traceback.format_exc()
        raise PySparkException(
            f"applyInArrow user function raised {type(error).__name__}: {error}\n{detail}"
        ) from error


def _apply_in_arrow_group_batches(
    input_batches: Iterator[Any],
    *,
    user_func: Callable[..., Any],
    key_names: list[str],
    keyed: bool,
    is_iter: bool,
    expected_arrow: Any,
) -> Iterator[Any]:
    """Run an applyInArrow callback per group over a sorted batch stream."""
    for _key, segments in grouped_udf._iter_apply_in_pandas_keyed_groups(input_batches, key_names):
        if is_iter:
            if keyed:
                result = _call_grouped_user_func(
                    user_func,
                    "applyInArrow",
                    _apply_in_arrow_group_key(segments, key_names),
                    iter(segments),
                )
            else:
                result = _call_grouped_user_func(user_func, "applyInArrow", iter(segments))
            yield from _iter_apply_in_arrow_results(result, expected_arrow)
            continue
        table = grouped_udf._apply_in_pandas_table_from_segments(segments)
        if keyed:
            result = _call_grouped_user_func(
                user_func,
                "applyInArrow",
                _apply_in_arrow_group_key(segments, key_names),
                table,
            )
        else:
            result = _call_grouped_user_func(user_func, "applyInArrow", table)
        _verify_arrow_table_result(result, expected_arrow)
        if result.num_columns == 0 and result.num_rows == 0:
            continue
        if list(result.schema.names) != list(expected_arrow.names):
            result = result.select(list(expected_arrow.names))
        yield from result.to_batches()


def apply_in_arrow(
    grouped: GroupedData,
    func: Callable[..., Any],
    schema: Any,
) -> Any:
    """``GroupedData.applyInArrow``: Arrow table or iterator callback per group.

    pins: grouped-surface-1/C-002, grouped-surface-1/C-003
    """
    if grouped._sql_group_clause is not None:
        raise AnalysisException(
            "applyInArrow after cube/rollup/grouping sets is not supported; "
            "use groupBy(...).applyInArrow(...) instead"
        )
    if grouped._pivot_col is not None:
        raise AnalysisException(
            "applyInArrow after pivot is not supported; "
            "use groupBy(...).applyInArrow(...) without pivot"
        )
    if not callable(func):
        raise PySparkTypeError(
            f"[NOT_CALLABLE] Argument `func` should be a callable, got {type(func).__name__}.",
            errorClass="NOT_CALLABLE",
            messageParameters={
                "arg_name": "func",
                "arg_type": type(func).__name__,
            },
        )
    try:
        is_iter = _infer_apply_in_arrow_is_iter(func)
    except Exception:
        warnings.warn("Cannot infer the eval type from type hints. ", UserWarning, stacklevel=2)
        is_iter = False
    key_names = grouped._apply_in_pandas_group_key_names()
    _declared, expected_arrow = _coerce_map_in_arrow_schema(schema)
    frame = grouped._dataframe
    frame._ensure_alive()
    sorted_parent = frame.order_by(*key_names) if key_names else frame
    return sorted_parent.mapInArrow(
        functools.partial(
            _apply_in_arrow_group_batches,
            user_func=func,
            key_names=key_names,
            keyed=_grouped_map_positional_arity(func) == 2,
            is_iter=is_iter,
            expected_arrow=expected_arrow,
        ),
        schema,
    )


def apply_in_pandas_with_state(
    grouped: GroupedData,
    func: Any,
    outputStructType: Any,  # noqa: N803 — PySpark kwarg name
    stateStructType: Any,  # noqa: N803 — PySpark kwarg name
    outputMode: Any,  # noqa: N803 — PySpark kwarg name
    timeoutConf: Any,  # noqa: N803 — PySpark kwarg name
) -> NoReturn:
    """Batch frames always refuse Spark's stateful grouped pandas UDF.

    pins: grouped-surface-1/C-006
    """
    _raise_with_spark_class(
        UnsupportedOperationException(
            "applyInPandasWithState is unsupported in batch query. Use applyInPandas instead."
        ),
        "_LEGACY_ERROR_TEMP_3176",
        {},
    )


def transform_with_state(
    grouped: GroupedData,
    statefulProcessor: Any,  # noqa: N803 — PySpark kwarg name
    outputStructType: Any,  # noqa: N803 — PySpark kwarg name
    outputMode: Any,  # noqa: N803 — PySpark kwarg name
    timeMode: Any,  # noqa: N803 — PySpark kwarg name
    initialState: Any = None,  # noqa: N803 — PySpark kwarg name
    eventTimeColumnName: str = "",  # noqa: N803 — PySpark kwarg name
) -> NoReturn:
    """Declared refusal: transformWithState needs streaming state stores.

    pins: grouped-surface-1/C-006
    """
    raise PySparkNotImplementedError(
        "[NOT_IMPLEMENTED] transformWithState is not implemented.",
        errorClass="NOT_IMPLEMENTED",
        messageParameters={"feature": "transformWithState"},
    )


def transform_with_state_in_pandas(
    grouped: GroupedData,
    statefulProcessor: Any,  # noqa: N803 — PySpark kwarg name
    outputStructType: Any,  # noqa: N803 — PySpark kwarg name
    outputMode: Any,  # noqa: N803 — PySpark kwarg name
    timeMode: Any,  # noqa: N803 — PySpark kwarg name
    initialState: Any = None,  # noqa: N803 — PySpark kwarg name
    eventTimeColumnName: str = "",  # noqa: N803 — PySpark kwarg name
) -> NoReturn:
    """Declared refusal: transformWithStateInPandas needs streaming state stores.

    pins: grouped-surface-1/C-006
    """
    raise PySparkNotImplementedError(
        "[NOT_IMPLEMENTED] transformWithStateInPandas is not implemented.",
        errorClass="NOT_IMPLEMENTED",
        messageParameters={"feature": "transformWithStateInPandas"},
    )
