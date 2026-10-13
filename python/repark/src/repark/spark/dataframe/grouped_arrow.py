"""Grouped map UDF bridges: ``apply``, ``applyInArrow``, and the state-API refusals.

Bound onto ``GroupedData`` in :mod:`repark.spark.dataframe.joins_columns`.
"""

from __future__ import annotations

import functools
import inspect
import typing
import warnings
from collections.abc import Callable, Iterator
from types import MethodType
from typing import TYPE_CHECKING, Any, NoReturn

import repark.spark.dataframe.grouped_udf as grouped_udf
import repark.spark.dataframe.udf_bridge as udf_bridge
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
from repark.spark._secrets import scrub_exception, scrub_user_failure
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
    failure = passthrough = None
    try:
        return user_func(*args)
    except PySparkException as error:
        passthrough = scrub_exception(error)
        if passthrough is error:
            raise
    except Exception as error:
        detail, failure = scrub_user_failure(error)
    if passthrough is not None:
        raise passthrough
    raise PySparkException(
        f"{api_name} user function raised {type(failure).__name__}: {failure}\n{detail}"
    ) from failure


_SCALAR_POSITION = "timestamp"
_LIST_POSITION = "list"
_MAP_POSITION = "map"
_STRUCT_POSITION = "struct"
_SCALAR_PLAN: tuple[Any, ...] = (_SCALAR_POSITION,)
_NO_TICKS: frozenset[int] = frozenset()
_NOT_A_TIME_TICK = -(2**63)
_FACADE_CALLBACKS = (
    grouped_udf._grouped_agg_pandas,
    udf_bridge._apply_ordered_window_pandas_udf,
)


def _is_facade_callback(user_func: Any) -> bool:
    """True for the facade's own pandas UDF callbacks, which keep main's conversion."""
    return getattr(user_func, "func", None) in _FACADE_CALLBACKS


def _is_list_type(arrow_type: Any) -> bool:
    """True for every Arrow list layout."""
    import pyarrow as pa

    types = pa.types
    return (
        types.is_list(arrow_type)
        or types.is_large_list(arrow_type)
        or types.is_fixed_size_list(arrow_type)
        or types.is_list_view(arrow_type)
        or types.is_large_list_view(arrow_type)
    )


def _struct_scan_plan(
    named_types: list[tuple[str, Any]],
    integers: bool,
) -> tuple[Any, ...] | None:
    """Compile the fields of a declared struct that the result scan visits."""
    fields = []
    for index, (name, arrow_type) in enumerate(named_types):
        plan = _timestamp_scan_plan(arrow_type, integers)
        if plan is not None:
            fields.append((index, name, name.encode(), plan))
    return (_STRUCT_POSITION, tuple(fields)) if fields else None


def _timestamp_scan_plan(arrow_type: Any, integers: bool) -> tuple[Any, ...] | None:
    """Compile the positions of a declared type where a nanosecond tick can be stored coarser."""
    import pyarrow as pa

    types = pa.types
    if types.is_timestamp(arrow_type):
        return _SCALAR_PLAN if integers and arrow_type.unit == "us" else None
    if types.is_struct(arrow_type):
        return _struct_scan_plan([(field.name, field.type) for field in arrow_type], integers)
    if types.is_map(arrow_type):
        entries = _struct_scan_plan(
            [
                (arrow_type.key_field.name, arrow_type.key_type),
                (arrow_type.item_field.name, arrow_type.item_type),
            ],
            integers,
        )
        return None if entries is None else (_MAP_POSITION, entries, False)
    if _is_list_type(arrow_type):
        value_type = arrow_type.value_type
        timestamps = types.is_timestamp(value_type) and value_type.unit == "us"
        elements = _timestamp_scan_plan(value_type, integers)
        if elements is None and not timestamps:
            return None
        return (_LIST_POSITION, elements, timestamps)
    return None


def _timestamp_scan_plans(expected_arrow: Any) -> tuple[tuple[Any, Any, Any], ...]:
    """Compile the array plan and the integer plan of every declared field holding a timestamp."""
    plans = []
    for field in expected_arrow:
        integer_plan = _timestamp_scan_plan(field.type, True)
        if integer_plan is not None:
            plans.append((field, _timestamp_scan_plan(field.type, False), integer_plan))
    return tuple(plans)


def _nanosecond_ticks_differ_from_floor(values: Any) -> bool:
    """True when a raw nanosecond tick differs from its microsecond floor, NaT left out."""
    import numpy as np

    ticks = np.asarray(values).astype("int64")
    return bool(((ticks != ticks // 1000) & (ticks != _NOT_A_TIME_TICK)).any())


def _read_sequence(value: Any) -> list[Any] | None:
    """Return the elements of a cell container, or None when it cannot be read."""
    if isinstance(value, (list, tuple)):
        return list(value)
    if value is None or isinstance(value, (str, bytes, bytearray)):
        return None
    try:
        return list(value)
    except Exception:
        return None


def _struct_field_values(
    cells: list[Any],
    index: int,
    name: str,
    encoded_name: bytes,
) -> list[Any]:
    """Return the values of one struct field over some struct cells, read as pyarrow reads them."""
    values = []
    for cell in cells:
        if isinstance(cell, dict):
            value = dict.get(cell, name)
            if value is None:
                value = dict.get(cell, encoded_name)
        elif isinstance(cell, tuple):
            value = cell[index] if index < len(cell) else None
        else:
            items = _read_sequence(cell)
            pair = items[index] if items is not None and index < len(items) else None
            value = pair[1] if isinstance(pair, tuple) and len(pair) == 2 else None
        if value is not None:
            values.append(value)
    return values


def _list_cells_store_wrong(
    plan: tuple[Any, ...],
    cells: Any,
    handed_ticks: frozenset[int],
) -> bool:
    """True when a list or map cell stores a nanosecond tick as a coarser timestamp."""
    import numpy as np

    position, element_plan, timestamps = plan
    array_type = np.ndarray
    nanosecond_dtype = np.dtype("datetime64[ns]")
    cleared_dtype = None
    elements: list[Any] = []
    for cell in cells:
        if isinstance(cell, array_type):
            dtype = cell.dtype
            if dtype is cleared_dtype:
                continue
            kind = dtype.kind
            if kind == "O":
                if element_plan is not None:
                    elements.extend(cell.tolist())
            elif dtype != nanosecond_dtype:
                cleared_dtype = dtype
            elif timestamps and _nanosecond_ticks_differ_from_floor(cell):
                return True
        elif element_plan is not None:
            if isinstance(cell, (list, tuple)):
                elements.extend(cell)
            elif position == _MAP_POSITION and isinstance(cell, dict):
                elements.extend(dict.items(cell))
            else:
                elements.extend(_read_sequence(cell) or ())
    return bool(elements) and _cells_store_wrong(element_plan, elements, handed_ticks)


def _cells_store_wrong(plan: tuple[Any, ...], cells: Any, handed_ticks: frozenset[int]) -> bool:
    """True when a value at one declared position stores a nanosecond tick as a coarser one."""
    position = plan[0]
    if position == _SCALAR_POSITION:
        return any(type(cell) is int and cell in handed_ticks for cell in cells)
    if position == _STRUCT_POSITION:
        return any(
            _cells_store_wrong(
                field_plan,
                _struct_field_values(cells, index, name, encoded_name),
                handed_ticks,
            )
            for index, name, encoded_name, field_plan in plan[1]
        )
    return _list_cells_store_wrong(plan, cells, handed_ticks)


def _column_returns_handed_tick(
    plan: tuple[Any, ...],
    series: Any,
    handed_ticks: frozenset[int],
) -> bool:
    """True when a result column returns a handed-out nanosecond integer under a timestamp."""
    import pandas as pd

    if series.dtype == object:
        return _cells_store_wrong(plan, series.to_numpy().tolist(), handed_ticks)
    return (
        plan[0] == _SCALAR_POSITION
        and pd.api.types.is_integer_dtype(series.dtype)
        and bool(series.isin(handed_ticks).any())
    )


def _type_hands_integers(arrow_type: Any, in_struct_or_map: bool) -> bool:
    """True when ``to_pandas`` hands a nanosecond timestamp of this type out as an integer."""
    import pyarrow as pa

    types = pa.types
    if types.is_timestamp(arrow_type):
        return in_struct_or_map and arrow_type.unit == "ns"
    if types.is_struct(arrow_type):
        return any(_type_hands_integers(field.type, True) for field in arrow_type)
    if types.is_map(arrow_type):
        return _type_hands_integers(arrow_type.key_type, True) or _type_hands_integers(
            arrow_type.item_type, True
        )
    if _is_list_type(arrow_type):
        return _type_hands_integers(arrow_type.value_type, False)
    return False


def _collect_timestamp_leaves(
    array: Any,
    in_struct_or_map: bool,
    handed_only: bool,
    leaves: list[Any],
) -> None:
    """Append the timestamp leaves of an array, or only those pandas receives as integers."""
    import pyarrow as pa

    types = pa.types
    arrow_type = array.type
    if types.is_timestamp(arrow_type):
        if not handed_only or (in_struct_or_map and arrow_type.unit == "ns"):
            leaves.append(array)
    elif types.is_struct(arrow_type):
        for child in array.flatten():
            _collect_timestamp_leaves(child, True, handed_only, leaves)
    elif types.is_map(arrow_type):
        offsets = array.offsets
        start = offsets[0].as_py()
        length = offsets[len(offsets) - 1].as_py() - start
        _collect_timestamp_leaves(array.keys.slice(start, length), True, handed_only, leaves)
        _collect_timestamp_leaves(array.items.slice(start, length), True, handed_only, leaves)
    elif _is_list_type(arrow_type):
        _collect_timestamp_leaves(array.flatten(), False, handed_only, leaves)


def _timestamp_leaf_ticks(chunks: Any, handed_only: bool) -> Any:
    """Return the int64 ticks of the timestamp leaves of some Arrow arrays, chunked."""
    import pyarrow as pa

    leaves: list[Any] = []
    for chunk in chunks:
        _collect_timestamp_leaves(chunk, False, handed_only, leaves)
    return pa.chunked_array([leaf.cast(pa.int64()) for leaf in leaves], type=pa.int64())


def _handed_integer_ticks(group_tables: tuple[Any, ...]) -> Any:
    """Return the nanosecond ticks the group frames hand to pandas as integers, chunked."""
    import pyarrow as pa

    types = pa.types
    chunks = [
        chunk
        for table in group_tables
        for index, arrow_type in enumerate(table.schema.types)
        if types.is_nested(arrow_type) and _type_hands_integers(arrow_type, False)
        for chunk in table.column(index).chunks
    ]
    return _timestamp_leaf_ticks(chunks, True)


def _unambiguous_handed_ticks(handed: Any) -> frozenset[int]:
    """Return the handed-out ticks that are no microsecond floor of a handed-out tick."""
    import numpy as np
    import pyarrow.compute as pc

    ticks = pc.drop_null(handed).to_numpy()
    return frozenset(np.setdiff1d(ticks, ticks // 1000).tolist())


def _nanosecond_array_refusal(
    out_pdf: Any,
    scan_plans: tuple[tuple[Any, Any, Any], ...],
    api_name: str,
) -> str | None:
    """Return the refusal text when a result cell is a nanosecond array under a coarser list."""
    for field, array_plan, _integer_plan in scan_plans:
        if array_plan is None or field.name not in out_pdf.columns:
            continue
        series = out_pdf[field.name]
        if (
            series.ndim == 1
            and series.dtype == object
            and _cells_store_wrong(array_plan, series.to_numpy().tolist(), _NO_TICKS)
        ):
            return (
                f"{api_name} refuses column '{field.name}' of declared type {field.type}: "
                "the pandas result holds nanosecond datetimes (datetime64[ns]) inside a list, "
                "which would store wrong values. Cast the elements to microseconds in the "
                'function (for example .astype("datetime64[us]")) before returning.'
            )
    return None


def _handed_integer_refusal(
    out_pdf: Any,
    out_table: Any,
    scan_plans: tuple[tuple[Any, Any, Any], ...],
    api_name: str,
    group_tables: tuple[Any, ...],
) -> str | None:
    """Return the refusal text when a result stores a handed-out nanosecond integer."""
    import pyarrow.compute as pc

    if not scan_plans:
        return None
    handed = _handed_integer_ticks(group_tables)
    if handed.null_count == len(handed):
        return None
    for field, _array_plan, integer_plan in scan_plans:
        stored = _timestamp_leaf_ticks(out_table.column(field.name).chunks, False)
        if not pc.any(pc.is_in(stored, value_set=handed, skip_nulls=True)).as_py():
            continue
        if _column_returns_handed_tick(
            integer_plan, out_pdf[field.name], _unambiguous_handed_ticks(handed)
        ):
            return (
                f"{api_name} refuses column '{field.name}' of declared type {field.type}: "
                "the pandas result holds an integer that the group frame carried for a "
                "nanosecond timestamp (timestamp[ns]) inside a struct or map, which would "
                "store wrong values. Convert it to microseconds in the function (for example "
                "value // 1000) before returning."
            )
    return None


def _pandas_result_to_arrow_batches(
    out_pdf: Any,
    expected_arrow: Any,
    *,
    api_name: str,
    scan_plans: tuple[tuple[Any, Any, Any], ...],
    group_tables: tuple[Any, ...],
) -> Iterator[Any]:
    """Convert a validated pandas result into Arrow batches for the bridge."""
    import pyarrow as pa

    if len(out_pdf) == 0 and len(out_pdf.columns) == 0:
        yield pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in expected_arrow],
            schema=expected_arrow,
        )
        return
    refusal = _nanosecond_array_refusal(out_pdf, scan_plans, api_name)
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
    else:
        refusal = refusal or _handed_integer_refusal(
            out_pdf, out_table, scan_plans, api_name, group_tables
        )
        if refusal is not None:
            raise PySparkTypeError(refusal)
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

    scan_plans = () if _is_facade_callback(user_func) else _timestamp_scan_plans(expected_arrow)
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
            out_pdf,
            expected_arrow,
            api_name="applyInPandas",
            scan_plans=scan_plans,
            group_tables=(group_table,),
        )


def grouped_apply(grouped: GroupedData, udf: Any) -> Any:
    """``GroupedData.apply``: GROUPED_MAP pandas UDFs delegate to applyInPandas.

    pins: grouped-surface-1/C-001
    """
    from repark.spark.column import Column
    from repark.spark.functions import PandasUDFType

    grouped._dataframe._refuse_self_join_refs(grouped._raw_key_columns())
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
    failure = passthrough = None
    try:
        for batch in result:
            _verify_arrow_batch_result(batch, expected_arrow)
            if batch.num_columns == 0 and batch.num_rows == 0:
                continue
            if list(batch.schema.names) != list(expected_arrow.names):
                batch = batch.select(list(expected_arrow.names))
            yield batch
    except PySparkException as error:
        passthrough = scrub_exception(error)
        if passthrough is error:
            raise
    except Exception as error:
        detail, failure = scrub_user_failure(error)
    if passthrough is not None:
        raise passthrough
    if failure is not None:
        raise PySparkException(
            f"applyInArrow user function raised {type(failure).__name__}: {failure}\n{detail}"
        ) from failure


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
    grouped._dataframe._refuse_self_join_refs(grouped._raw_key_columns())
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
