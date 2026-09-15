"""Arrow user-defined functions and table functions (FNP-MISC-1)."""

from __future__ import annotations

import inspect
from collections import deque
from collections.abc import Callable, Iterator
from typing import Any

from repark.errors import PySparkException, PySparkRuntimeError, PySparkTypeError
from repark.spark.functions_udf import (
    PandasUDFType,
    _build_pandas_udf,
    _is_pandas_udf_datatype_like,
    _is_pandas_udf_function_type,
    _normalize_pandas_udf_function_type,
    _normalize_pandas_udf_return_type_sql,
    _pandas_udf_arrow_type_for_return,
)

ARROW_EXPORTS: tuple[str, ...] = ("arrow_udf", "arrow_udtf")

_PACKAGE_NOT_INSTALLED_MESSAGE = (
    "[PACKAGE_NOT_INSTALLED] PyArrow >= 15.0.0 must be installed; however, it was not found."
)

_LENGTH_MISMATCH_TEMPLATE = (
    "[SCHEMA_MISMATCH_FOR_PANDAS_UDF] Result vector from arrow_udf was not the "
    "required length: expected {expected}, got {got}."
)

_ARROW_YIELD_MESSAGE = (
    "[UDTF_ARROW_TYPE_CONVERSION_ERROR] PyArrow UDTF must return an iterator of "
    "pyarrow.Table or pyarrow.RecordBatch objects."
)

_CONTAINER_TOKENS: tuple[str, ...] = (
    "array",
    "series",
    "dataframe",
    "frame",
    "batch",
    "table",
    "chunked",
)


def _require_pyarrow() -> Any:
    """Import pyarrow or raise the PACKAGE_NOT_INSTALLED shape."""
    try:
        import pyarrow
    except ImportError as error:
        raise ImportError(_PACKAGE_NOT_INSTALLED_MESSAGE) from error
    return pyarrow


def _annotation_text(annotation: Any) -> str:
    """Render one annotation for hint matching (empty string when absent)."""
    if annotation is inspect.Parameter.empty:
        return ""
    return str(annotation)


def _infer_arrow_function_type(user_func: Callable[..., Any]) -> int:
    """Infer SCALAR / SCALAR_ITER / GROUPED_AGG from type hints, Spark-style."""
    try:
        signature = inspect.signature(user_func)
    except (TypeError, ValueError):
        return PandasUDFType.SCALAR
    seen = [_annotation_text(parameter.annotation) for parameter in signature.parameters.values()]
    seen.append(_annotation_text(signature.return_annotation))
    lowered = [text.lower() for text in seen]
    if any("iterator" in text for text in lowered):
        return PandasUDFType.SCALAR_ITER
    returns = lowered[-1]
    if returns and "none" not in returns and not _returns_container(returns):
        return PandasUDFType.GROUPED_AGG
    return PandasUDFType.SCALAR


def _returns_container(returns: str) -> bool:
    """Whether a lowered return annotation names an Arrow/pandas container."""
    return any(token in returns for token in _CONTAINER_TOKENS)


def _series_to_arrow(series: Any) -> Any:
    """Convert one pandas Series to an Arrow array, keeping nulls."""
    import pyarrow as pa

    return pa.Array.from_pandas(series)


def _arrow_to_series(array: Any) -> Any:
    """Convert one Arrow array to a pandas Series with nullable dtypes."""
    from repark.spark.dataframe.udf_bridge import _arrow_array_to_pandas_series

    return _arrow_array_to_pandas_series(array)


def _normalize_arrow_result(result: Any, arrow_type: Any, *, expected_rows: int, name: str) -> Any:
    """Validate one arrow result, cast it to the declared type, return a Series."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if isinstance(result, pa.ChunkedArray):
        result = result.combine_chunks()
    if not isinstance(result, pa.Array):
        raise PySparkException(
            f"arrow_udf {name!r} must return a pyarrow.Array; got {type(result).__name__}"
        )
    if len(result) != expected_rows:
        raise PySparkRuntimeError(
            _LENGTH_MISMATCH_TEMPLATE.format(expected=expected_rows, got=len(result)),
            errorClass="SCHEMA_MISMATCH_FOR_PANDAS_UDF",
            messageParameters={"expected": str(expected_rows), "got": str(len(result))},
        )
    if not result.type.equals(arrow_type):
        try:
            result = pc.cast(result, arrow_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError, TypeError) as error:
            raise PySparkException(
                f"arrow_udf {name!r} failed converting result to declared "
                f"type {arrow_type}: {error}"
            ) from error
    return _arrow_to_series(result)


class _ArrowScalarAdapter:
    """Wrap ``pa.Array`` to ``pa.Array`` as Series to Series for the pandas bridge."""

    def __init__(self, user_func: Callable[..., Any], arrow_type: Any) -> None:
        """Bind the user function, the declared Arrow type, and its display name."""
        self._user_func = user_func
        self._arrow_type = arrow_type
        self.__name__ = getattr(user_func, "__name__", "arrow_udf")

    def __call__(self, *series_args: Any) -> Any:
        """Run the arrow function on one batch and return a Series."""
        arrays = [_series_to_arrow(series) for series in series_args]
        result = self._user_func(*arrays)
        return _normalize_arrow_result(
            result, self._arrow_type, expected_rows=len(arrays[0]), name=self.__name__
        )


class _ArrowBatchFeeder:
    """Convert Series batches to Arrow arrays one at a time, recording lengths."""

    def __init__(self, series_iter: Any) -> None:
        """Bind the Series batch iterator."""
        self._series_iter = iter(series_iter)
        self._lengths: deque[int] = deque()

    def __iter__(self) -> Any:
        """Iterate converted Arrow batches."""
        return self

    def __next__(self) -> Any:
        """Convert the next Series batch and record its row count."""
        item = next(self._series_iter)
        if isinstance(item, tuple):
            self._lengths.append(len(item[0]))
            return tuple(_series_to_arrow(series) for series in item)
        array = _series_to_arrow(item)
        self._lengths.append(len(array))
        return array

    def pop_length(self) -> int | None:
        """Pop the oldest recorded input length, if the user already pulled it."""
        if self._lengths:
            return self._lengths.popleft()
        return None


class _ArrowIterAdapter:
    """Wrap iterator-of-Array to iterator-of-Array for the SCALAR_ITER bridge."""

    def __init__(self, user_func: Callable[..., Any], arrow_type: Any) -> None:
        """Bind the user function, the declared Arrow type, and its display name."""
        self._user_func = user_func
        self._arrow_type = arrow_type
        self.__name__ = getattr(user_func, "__name__", "arrow_udf")

    def __call__(self, series_iter: Any) -> Any:
        """Stream batches through the user iterator, holding at most one batch."""
        return self._stream(_ArrowBatchFeeder(series_iter))

    def _stream(self, feeder: _ArrowBatchFeeder) -> Any:
        """Yield normalized output Series in input order, validating lengths."""
        outputs = self._user_func(iter(feeder))
        if outputs is None:
            raise PySparkException(
                f"arrow_udf {self.__name__!r} (SCALAR_ITER) must return an iterator "
                "of pyarrow.Array (got None)"
            )
        for output in outputs:
            expected_rows = feeder.pop_length()
            if expected_rows is None:
                expected_rows = len(output) if hasattr(output, "__len__") else 0
            yield _normalize_arrow_result(
                output, self._arrow_type, expected_rows=expected_rows, name=self.__name__
            )


class _ArrowGroupedAdapter:
    """Wrap ``pa.Array`` to scalar for the GROUPED_AGG bridge."""

    def __init__(self, user_func: Callable[..., Any]) -> None:
        """Bind the user function and its display name."""
        self._user_func = user_func
        self.__name__ = getattr(user_func, "__name__", "arrow_udf")

    def __call__(self, *series_args: Any) -> Any:
        """Run the arrow function on one group and return its scalar."""
        import pyarrow as pa

        arrays = [_series_to_arrow(series) for series in series_args]
        result = self._user_func(*arrays)
        if isinstance(result, pa.Scalar):
            return result.as_py()
        return result


class _ArrowUdfDecorator:
    """Decorator factory carrying returnType and functionType without closures."""

    def __init__(self, return_type: Any, function_type: Any) -> None:
        """Bind the declared return type and the optional eval-type override."""
        self._return_type = return_type
        self._function_type = function_type

    def __call__(self, user_func: Callable[..., Any]) -> Any:
        """Build the bridge callable for the decorated function."""
        return _build_arrow_udf(user_func, self._return_type, self._function_type)


def _cannot_be_none() -> PySparkTypeError:
    """Build the returnType-missing error in Spark's shape."""
    return PySparkTypeError(
        "[CANNOT_BE_NONE] Argument `returnType` cannot be None.",
        errorClass="CANNOT_BE_NONE",
        messageParameters={"arg_name": "returnType"},
    )


def _build_arrow_udf(user_func: Callable[..., Any], return_type: Any, function_type: Any) -> Any:
    """Validate the declaration, infer the eval type, and wrap for the pandas bridge."""
    if return_type is None:
        raise _cannot_be_none()
    _require_pyarrow()
    if not callable(user_func):
        raise PySparkTypeError(f"arrow_udf func must be callable, got {type(user_func).__name__}")
    if function_type is None:
        resolved_type = _infer_arrow_function_type(user_func)
    else:
        resolved_type = _normalize_pandas_udf_function_type(function_type)
    return_type_sql = _normalize_pandas_udf_return_type_sql(return_type)
    from repark.spark.types import DataType

    arrow_type = _pandas_udf_arrow_type_for_return(DataType.fromDDL(return_type_sql))
    if resolved_type == PandasUDFType.SCALAR_ITER:
        adapter: Any = _ArrowIterAdapter(user_func, arrow_type)
    elif resolved_type == PandasUDFType.GROUPED_AGG:
        adapter = _ArrowGroupedAdapter(user_func)
    else:
        adapter = _ArrowScalarAdapter(user_func, arrow_type)
    return _build_pandas_udf(adapter, return_type_sql, resolved_type)


def arrow_udf(
    f: Any = None,
    returnType: Any = None,  # noqa: N803 — PySpark camelCase
    functionType: Any = None,  # noqa: N803 — PySpark camelCase
) -> Any:
    """Arrow UDF over the pandas bridge (PySpark ``functions.arrow_udf``). pins: fnp-misc-1/C-004"""
    if callable(f) and not _is_pandas_udf_datatype_like(f):
        return _build_arrow_udf(f, returnType, functionType)
    if f is None:
        if returnType is None:
            raise _cannot_be_none()
        return _ArrowUdfDecorator(returnType, functionType)
    if functionType is None and returnType is not None and _is_pandas_udf_function_type(returnType):
        return _ArrowUdfDecorator(f, returnType)
    if functionType is None and returnType is not None:
        raise PySparkTypeError(
            "arrow_udf decorator second positional argument must be functionType "
            f"(SCALAR / PandasUDFType.*), not a second returnType; got {returnType!r}."
        )
    return _ArrowUdfDecorator(f, functionType)


class _ArrowUdtfDecorator:
    """Decorator factory carrying the UDTF returnType without closures."""

    def __init__(self, return_type: Any) -> None:
        """Bind the declared return type."""
        self._return_type = return_type

    def __call__(self, handler_cls: Any) -> Any:
        """Wrap the decorated handler class as an arrow UDTF."""
        return _build_arrow_udtf(handler_cls, self._return_type)


class _ArrowUdtfHandler:
    """Batch-native arrow handler (eval takes whole-column Arrays, yields Tables)."""

    _repark_arrow_udtf: bool = True
    _arrow_handler_cls: Any = None

    def __init__(self) -> None:
        """Instantiate the wrapped arrow handler."""
        self._handler = self._arrow_handler_cls()

    def start(self) -> None:
        """Forward the start hook when the wrapped handler defines one."""
        hook = getattr(self._handler, "start", None)
        if callable(hook):
            hook()

    def terminate(self) -> None:
        """Forward the terminate hook when the wrapped handler defines one."""
        hook = getattr(self._handler, "terminate", None)
        if callable(hook):
            hook()

    def eval(self, *scalar_args: Any) -> Any:
        """Refuse per-row calls; the bridge runs arrow handlers batch-wise."""
        raise PySparkException(
            "arrow UDTF handlers run batch-wise through _eval_batch; per-row eval is not supported"
        )

    def _eval_batch(
        self, field_names: list[str], arrow_schema: Any, surface: str, *arrays: Any
    ) -> list[Any]:
        """Call the arrow eval once and project its Tables onto the declared fields."""
        produced = self._handler.eval(*arrays)
        if produced is None:
            return []
        if inspect.isgenerator(produced) or isinstance(produced, Iterator):
            items = list(produced)
        elif isinstance(produced, list):
            items = produced
        else:
            items = [produced]
        return [_project_arrow_table(item, field_names, arrow_schema, surface) for item in items]


def _project_arrow_table(item: Any, field_names: list[str], arrow_schema: Any, surface: str) -> Any:
    """Project one yielded Table onto the declared field names and types."""
    import pyarrow as pa

    if isinstance(item, pa.RecordBatch):
        item = pa.Table.from_batches([item])
    if not isinstance(item, pa.Table):
        raise PySparkException(_ARROW_YIELD_MESSAGE)
    missing = [name for name in field_names if name not in item.schema.names]
    if missing:
        raise PySparkException(
            f"UDTF {surface} yielded table missing returnType field(s) {missing}; "
            f"declared fields are {field_names}"
        )
    extra = [name for name in item.schema.names if name not in field_names]
    if extra:
        raise PySparkException(
            f"UDTF {surface} yielded table with undeclared column(s) {extra}; "
            f"declared fields are {field_names}"
        )
    try:
        return item.select(field_names).cast(arrow_schema)
    except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError, TypeError) as error:
        raise PySparkException(
            f"UDTF {surface} yielded table failed converting to returnType {arrow_schema}: {error}"
        ) from error


def _build_arrow_udtf(handler_cls: Any, return_type: Any) -> Any:
    """Validate the handler and wrap it as a UserDefinedTableFunction."""
    from repark.spark.udtf import UserDefinedTableFunction, _validate_udtf_handler

    _validate_udtf_handler(handler_cls, return_type)
    _require_pyarrow()
    wrapper = type(
        f"Arrow{getattr(handler_cls, '__name__', 'Handler')}",
        (_ArrowUdtfHandler,),
        {"_arrow_handler_cls": handler_cls},
    )
    return UserDefinedTableFunction(
        wrapper, returnType=return_type, name=getattr(handler_cls, "__name__", "arrow_udtf")
    )


def arrow_udtf(cls: Any = None, *, returnType: Any = None) -> Any:  # noqa: N803 — PySpark camelCase
    """Arrow UDTF over the Python UDTF path. pins: fnp-misc-1/C-005"""
    if cls is None:
        return _ArrowUdtfDecorator(returnType)
    return _build_arrow_udtf(cls, returnType)


def install_into(namespace: dict[str, Any], exported: list[str]) -> None:
    """Copy the arrow surface onto the canonical functions module."""
    for name in ARROW_EXPORTS:
        namespace[name] = globals()[name]
        if name not in exported:
            exported.append(name)
