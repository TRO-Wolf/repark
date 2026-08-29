"""Python and pandas UDF markers and validation helpers.

Decorators preserve Spark evaluation-type tags and declared return types. DataFrame execution
owns the Arrow bridge; unsupported composition and unsafe type fallbacks fail loudly.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from repark.errors import (
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark.column import Column
from repark.spark.functions import col
from repark.spark.udtf import UserDefinedTableFunction


class PandasUDFType:
    """Eval-type tags mirroring ``pyspark.sql.functions.PandasUDFType`` (int values).

    Values match PySpark 4.1.2: SCALAR=200, GROUPED_MAP=201, GROUPED_AGG=202,
    SCALAR_ITER=204. Repark supports scalar, iterator, grouped-aggregate, and windowed
    grouped-aggregate forms. Grouped-map and ``functionType=WINDOW`` forms are refused.
    """

    SCALAR = 200
    GROUPED_MAP = 201
    GROUPED_AGG = 202
    SCALAR_ITER = 204


def _is_pandas_udf_datatype_like(value: Any) -> bool:
    """True when ``value`` is a returnType (str DDL or :class:`~repark.types.DataType`)."""
    if isinstance(value, str):
        return True
    from repark.spark.types import DataType

    return isinstance(value, DataType)


def _is_pandas_udf_function_type(value: Any) -> bool:
    """True when ``value`` looks like a PandasUDFType / eval-type tag (not a returnType)."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value in {
            PandasUDFType.SCALAR,
            PandasUDFType.SCALAR_ITER,
            PandasUDFType.GROUPED_MAP,
            PandasUDFType.GROUPED_AGG,
        }
    if isinstance(value, str):
        # Include WINDOW so positional ``@pandas_udf("long", "WINDOW")`` routes to the
        return value.upper() in {
            "SCALAR",
            "SCALAR_ITER",
            "GROUPED_MAP",
            "GROUPED_AGG",
            "GROUPED_AGGREGATE",
            "WINDOW",
        }
    return False


def _pandas_udf_refuse_fail_open_string_leaves(data_type: Any, arrow_type: Any) -> None:
    """Refuse DataType leaves that map to Arrow string without being string-like.

    Top-level and nested (array/map/struct-in-array) variant / interval / time markers
    fail-open to ``pa.string()`` via ``repark_type_to_arrow`` — walk leaves so
    ``array<variant>`` / ``map<string,time>`` / ``array<struct<a:variant>>`` cannot
    silently declare string payloads.
    """
    import pyarrow as pa

    from repark.spark.types import (
        ArrayType,
        CharType,
        MapType,
        StringType,
        StructType,
        VarcharType,
    )

    if isinstance(data_type, ArrayType):
        if (
            pa.types.is_list(arrow_type)
            or pa.types.is_large_list(arrow_type)
            or pa.types.is_fixed_size_list(arrow_type)
        ):
            _pandas_udf_refuse_fail_open_string_leaves(data_type.elementType, arrow_type.value_type)
            return
        raise PySparkTypeError(
            f"pandas_udf returnType {data_type.simpleString()!r} does not map to a concrete "
            "Arrow list type in repark v1 (refusing silent string fallback)."
        )
    if isinstance(data_type, MapType):
        if pa.types.is_map(arrow_type):
            _pandas_udf_refuse_fail_open_string_leaves(data_type.keyType, arrow_type.key_type)
            _pandas_udf_refuse_fail_open_string_leaves(data_type.valueType, arrow_type.item_type)
            return
        raise PySparkTypeError(
            f"pandas_udf returnType {data_type.simpleString()!r} does not map to a concrete "
            "Arrow map type in repark v1 (refusing silent string fallback)."
        )
    if isinstance(data_type, StructType):
        if pa.types.is_struct(arrow_type):
            arrow_fields = list(arrow_type)
            if len(arrow_fields) != len(data_type.fields):
                raise PySparkTypeError(
                    f"pandas_udf returnType {data_type.simpleString()!r} struct field count "
                    "mismatch after Arrow mapping."
                )
            for field, arrow_field in zip(data_type.fields, arrow_fields, strict=True):
                _pandas_udf_refuse_fail_open_string_leaves(field.dataType, arrow_field.type)
            return
        raise PySparkTypeError(
            f"pandas_udf returnType {data_type.simpleString()!r} does not map to a concrete "
            "Arrow struct type in repark v1 (refusing silent string fallback)."
        )
    # Leaf: non-string declaration that collapsed to Arrow string.
    if pa.types.is_string(arrow_type) and not isinstance(
        data_type, (StringType, CharType, VarcharType)
    ):
        raise PySparkTypeError(
            f"pandas_udf returnType {data_type.simpleString()!r} does not map to a concrete "
            "Arrow scalar type in repark v1 (refusing silent string fallback). Use a supported "
            "scalar type (boolean/byte/short/int/long/float/double/string/binary/date/"
            "timestamp/decimal/array/…)."
        )


def _pandas_udf_arrow_type_for_return(data_type: Any) -> Any:
    """Map a scalar return :class:`~repark.types.DataType` to Arrow, refusing fail-open string.

    ``repark_type_to_arrow`` / ``_sql_type_to_arrow`` map unknown markers (variant / interval /
    time / …) to ``pa.string()``. Scalar pandas_udf must not silently declare string when the
    user asked for another type.
    """
    from repark.spark.session import _data_type_to_sql_type, _sql_type_to_arrow
    from repark.spark.types import (
        DataType,
        StructType,
    )

    if not isinstance(data_type, DataType):
        raise PySparkTypeError(
            "pandas_udf returnType must be a DataType or DDL type string, "
            f"got {type(data_type).__name__}"
        )
    if isinstance(data_type, StructType):
        raise UnsupportedOperationException(
            "pandas_udf StructType / struct returnType is not supported in repark v1 "
            "(scalar only). Grouped-map pandas_udf is an M5-class seed."
        )
    try:
        sql_type = _data_type_to_sql_type(data_type)
    except Exception as error:
        raise PySparkTypeError(
            f"pandas_udf returnType {data_type.simpleString()!r} is not a supported "
            f"scalar type: {error}"
        ) from error
    arrow_type = _sql_type_to_arrow(sql_type)
    # Top-level and nested leaves (array/map/struct-in-array of variant|interval|time).
    _pandas_udf_refuse_fail_open_string_leaves(data_type, arrow_type)
    return arrow_type


def _normalize_pandas_udf_return_type_sql(return_type: Any) -> str:
    """Lower ``returnType`` to a logical DDL fragment that preserves Spark type identity.

    Stores :meth:`~repark.types.DataType.simpleString` (not ``_data_type_to_sql_type``), so
    ``timestamp_ntz`` / ``varchar(n)`` / ``char(n)`` survive round-trip through the bridge
    ``DataType.fromDDL`` → :attr:`DataFrame.schema`. Engine cast tokens
    (``TIMESTAMP`` / ``STRING``) collapse those distinctions and must not be stored.

    Parses string DDL fully so field-list forms (``a int, b string`` / ``a: int``) are
    detected as :class:`~repark.types.StructType` and refused, not only
    ``struct…`` prefixes. Unsupported markers that would fail-open to Arrow string are
    refused. Arrow physical mapping still uses ``_data_type_to_sql_type``
    inside :func:`_pandas_udf_arrow_type_for_return` at validation / bridge time.
    """
    from repark.spark.types import DataType, StructType

    if isinstance(return_type, str):
        text = return_type.strip()
        if not text:
            raise PySparkTypeError("pandas_udf returnType must be a non-empty type string")
        if text.lower().startswith("struct"):
            raise UnsupportedOperationException(
                "pandas_udf StructType / struct returnType is not supported in repark v1 "
                "(scalar only). Grouped-map pandas_udf is an M5-class seed."
            )
        try:
            parsed = DataType.fromDDL(text)
        except Exception as error:
            raise PySparkTypeError(
                f"pandas_udf returnType {text!r} is not a valid type: {error}"
            ) from error
        # Field-list DDL (``a int, b string`` / ``a: int``) parses as StructType without a
        if isinstance(parsed, StructType):
            raise UnsupportedOperationException(
                "pandas_udf StructType / struct returnType is not supported in repark v1 "
                "(scalar only). Grouped-map pandas_udf is an M5-class seed."
            )
        _pandas_udf_arrow_type_for_return(parsed)
        # logical simpleString — not engine SQL (TIMESTAMP/STRING collapse NTZ/varchar/char).
        return parsed.simpleString()
    if isinstance(return_type, StructType):
        raise UnsupportedOperationException(
            "pandas_udf StructType returnType is not supported in repark v1 "
            "(scalar only). Grouped-map pandas_udf is an M5-class seed."
        )
    if isinstance(return_type, DataType):
        _pandas_udf_arrow_type_for_return(return_type)
        return return_type.simpleString()
    raise PySparkTypeError(
        "pandas_udf returnType must be a DataType or DDL type string, "
        f"got {type(return_type).__name__}"
    )


def _normalize_pandas_udf_function_type(function_type: Any) -> int:
    """Accept SCALAR, SCALAR_ITER, and GROUPED_AGG; refuse GROUPED_MAP and window forms."""
    if function_type is None:
        return PandasUDFType.SCALAR
    if isinstance(function_type, str):
        key = function_type.upper()
        if key == "SCALAR":
            return PandasUDFType.SCALAR
        if key == "SCALAR_ITER":
            return PandasUDFType.SCALAR_ITER
        if key in {"GROUPED_AGG", "GROUPED_AGGREGATE"}:
            return PandasUDFType.GROUPED_AGG
        if key in {"GROUPED_MAP", "WINDOW"}:
            raise UnsupportedOperationException(
                f"pandas_udf functionType={function_type!r} is not supported in repark v1 "
                "(GROUPED_MAP / window pandas_udf are M6-class seeds). "
                "Supported: SCALAR, SCALAR_ITER, GROUPED_AGG."
            )
        raise PySparkTypeError(f"unknown pandas_udf functionType {function_type!r}")
    if isinstance(function_type, int) and not isinstance(function_type, bool):
        if function_type == PandasUDFType.SCALAR:
            return PandasUDFType.SCALAR
        if function_type == PandasUDFType.SCALAR_ITER:
            return PandasUDFType.SCALAR_ITER
        if function_type == PandasUDFType.GROUPED_AGG:
            return PandasUDFType.GROUPED_AGG
        if function_type == PandasUDFType.GROUPED_MAP:
            raise UnsupportedOperationException(
                f"pandas_udf functionType={function_type!r} is not supported in repark v1 "
                "(GROUPED_MAP / window pandas_udf are M6-class seeds). "
                "Supported: SCALAR, SCALAR_ITER, GROUPED_AGG."
            )
        raise PySparkTypeError(f"unknown pandas_udf functionType {function_type!r}")
    raise PySparkTypeError(
        f"pandas_udf functionType must be int or str, got {type(function_type).__name__}"
    )


class PandasUDFColumn:
    """Marker for a ``pandas_udf`` projection / agg (not a SQL-plan :class:`Column`).

    Produced by calling a :func:`pandas_udf`-decorated function with column arguments.

    * **SCALAR / SCALAR_ITER** — top-level ``select`` / ``withColumn`` only.
    * **GROUPED_AGG** — ``groupBy(...).agg(...)``.
    * **Windowed GROUPED_AGG** — ``.over(Window.partitionBy(...))`` unbounded whole-partition
      with ``orderBy`` and the default frame
      ``ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW``, plus duck-typed
      ``_frame_start`` / ``_frame_end`` integer offsets.

    Mid-expression composition (``udf_col + 1``, ``udf_col > 0`` in filter, nesting under
    ``coalesce``) is refused — the result is a mapInArrow / applyInPandas bridge-node
    output, with the same composition limit as ``mapInArrow``.
    """

    __slots__ = (
        "_alias_name",
        "_function_name",
        "_function_type",
        "_inputs",
        "_return_type_sql",
        "_user_func",
        "_window_spec",
    )

    def __init__(
        self,
        user_func: Callable[..., Any],
        return_type_sql: str,
        inputs: list[Column],
        function_name: str,
        *,
        alias_name: str | None = None,
        function_type: int = PandasUDFType.SCALAR,
        window_spec: Any | None = None,
    ) -> None:
        """Bind the user function, declared return type, eval type, and input Columns.

        ``return_type_sql`` is re-normalized here so a hostile public constructor call
        (or a hand-built marker) cannot skip decorator validation and fail-open to
        an Arrow string.
        """
        self._user_func = user_func
        self._return_type_sql = _normalize_pandas_udf_return_type_sql(return_type_sql)
        self._inputs = list(inputs)
        self._function_name = function_name
        self._alias_name = alias_name
        self._function_type = _normalize_pandas_udf_function_type(function_type)
        self._window_spec = window_spec

    def alias(self, name: str) -> PandasUDFColumn:
        """Set the output column name (PySpark ``Column.alias`` parity for UDF results)."""
        if not isinstance(name, str) or name.strip() == "":
            raise PySparkTypeError("pandas_udf alias name must be a non-empty str")
        return PandasUDFColumn(
            self._user_func,
            self._return_type_sql,
            self._inputs,
            self._function_name,
            alias_name=name,
            function_type=self._function_type,
            window_spec=self._window_spec,
        )

    def over(self, window: Any) -> PandasUDFColumn:
        """Attach a window for GROUPED_AGG with an optional ordered frame.

        Accepted :class:`~repark.window.WindowSpec` forms:

        * ``Window.partitionBy(...)`` only — unbounded whole-partition.
        * ``Window.partitionBy(...).orderBy(...)`` — default frame
          ``ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW``.
        * The same form with duck-typed ``_frame_start`` / ``_frame_end`` integer offsets.

        Requires ``functionType=GROUPED_AGG``. Scalar / SCALAR_ITER refuse.
        """
        from repark.errors import AnalysisException
        from repark.spark.window import WindowSpec

        if not isinstance(window, WindowSpec):
            raise PySparkTypeError(
                f"pandas_udf.over expects a WindowSpec, got {type(window).__name__}"
            )
        if int(self._function_type) != PandasUDFType.GROUPED_AGG:
            raise AnalysisException(
                "pandas_udf.over requires functionType=GROUPED_AGG "
                f"(got functionType={self._function_type!r} for {self._function_name!r}); "
                "SCALAR / SCALAR_ITER are not window forms"
            )
        partition_columns = list(getattr(window, "_partition_columns", []) or [])
        if not partition_columns:
            raise UnsupportedOperationException(
                "windowed pandas_udf requires Window.partitionBy(...) "
                "(global/unpartitioned window is not supported; use groupBy().agg for "
                "global GROUPED_AGG)"
            )
        order_columns = list(getattr(window, "_order_columns", []) or [])
        frame_units = getattr(window, "_frame_units", None)
        if frame_units is not None and str(frame_units).lower() not in {"rows", "row"}:
            raise UnsupportedOperationException(
                "windowed pandas_udf supports only ROWS frames "
                f"(got frame_units={frame_units!r}); RANGE frames are not supported"
            )
        # (None until rowsBetween/rangeBetween sets normalized ints) — presence means
        # value-is-not-None, never hasattr. When orderBy is present and bounds are absent →
        # Spark default UNBOUNDED PRECEDING … CURRENT ROW (start=None, end=0).
        has_frame_attrs = (
            getattr(window, "_frame_start", None) is not None
            or getattr(window, "_frame_end", None) is not None
        )
        if has_frame_attrs and not order_columns:
            raise UnsupportedOperationException(
                "windowed pandas_udf rowsBetween/range frame requires orderBy "
                "(Spark ordered-frame semantics)"
            )
        return PandasUDFColumn(
            self._user_func,
            self._return_type_sql,
            self._inputs,
            self._function_name,
            alias_name=self._alias_name,
            function_type=self._function_type,
            window_spec=window,
        )

    def default_name(self) -> str:
        """Spark-style default projection name ``func(arg, …)`` when no ``.alias`` is set."""
        arg_parts: list[str] = []
        for column in self._inputs:
            if column._projection_name is not None and column._stable_name:
                arg_parts.append(column._projection_name)
            else:
                arg_parts.append(column.spark_display_part())
        return f"{self._function_name}({', '.join(arg_parts)})"

    def output_name(self) -> str:
        """Resolved output field name (alias wins over :meth:`default_name`)."""
        if self._alias_name is not None:
            return self._alias_name
        return self.default_name()

    def _refuse_composition(self, surface: str) -> None:
        """Refuse composition because this marker is not a SQL Column expression."""
        raise UnsupportedOperationException(
            f"pandas_udf result cannot be used in {surface} in repark v1 "
            "(facade projection-rewrite bridge only; not a Column expression in the SQL plan). "
            "Materialize via select/withColumn, then apply further expressions on that column. "
            "Mid-expression embedding is an M5-class seed."
        )

    def __add__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (+)")

    def __radd__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (+)")

    def __sub__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (-)")

    def __rsub__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (-)")

    def __mul__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (*)")

    def __rmul__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (*)")

    def __truediv__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (/)")

    def __rtruediv__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (/)")

    def __mod__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (%)")

    def __rmod__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (%)")

    def __pow__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (**)")

    def __rpow__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (**)")

    def __neg__(self) -> None:
        self._refuse_composition("unary (-)")

    def __eq__(self, _other: Any) -> bool:  # type: ignore[override]
        self._refuse_composition("comparison (==)")
        return False

    def __ne__(self, _other: Any) -> bool:  # type: ignore[override]
        self._refuse_composition("comparison (!=)")
        return False

    def __lt__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (<)")
        return False

    def __le__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (<=)")
        return False

    def __gt__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (>)")
        return False

    def __ge__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (>=)")
        return False

    def __and__(self, _other: Any) -> None:
        self._refuse_composition("logical (&)")

    def __rand__(self, _other: Any) -> None:
        self._refuse_composition("logical (&)")

    def __or__(self, _other: Any) -> None:
        self._refuse_composition("logical (|)")

    def __ror__(self, _other: Any) -> None:
        self._refuse_composition("logical (|)")

    def __invert__(self) -> None:
        self._refuse_composition("logical (~)")

    def cast(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``cast`` — select the marker first, then cast the materialized column."""
        self._refuse_composition("cast")

    def is_null(self) -> None:
        """Refuse ``isNull`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("isNull")

    isNull = is_null  # noqa: N815 — PySpark camelCase alias

    def is_not_null(self) -> None:
        """Refuse ``isNotNull`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("isNotNull")

    isNotNull = is_not_null  # noqa: N815 — PySpark camelCase alias

    def between(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``between`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("between")

    def eqNullSafe(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        """Refuse ``eqNullSafe`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("eqNullSafe")

    def when(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``when`` — a pandas_udf marker cannot open a ``CASE`` arm."""
        self._refuse_composition("when")

    def otherwise(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``otherwise`` — a pandas_udf marker cannot close a ``CASE`` arm."""
        self._refuse_composition("otherwise")

    def asc(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``asc`` — sort on the materialized column instead of the marker."""
        self._refuse_composition("asc")

    def desc(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``desc`` — sort on the materialized column instead of the marker."""
        self._refuse_composition("desc")

    def contains(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``contains`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (contains)")

    def startswith(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``startswith`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (startswith)")

    def endswith(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``endswith`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (endswith)")

    def like(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``like`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (like)")

    def ilike(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``ilike`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (ilike)")

    def rlike(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``rlike`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (rlike)")

    def bitwiseAND(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        """Refuse ``bitwiseAND`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("bitwiseAND")

    def bitwiseOR(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        """Refuse ``bitwiseOR`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("bitwiseOR")

    def bitwiseXOR(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        """Refuse ``bitwiseXOR`` — a pandas_udf marker is not a plan :class:`Column`."""
        self._refuse_composition("bitwiseXOR")

    def __contains__(self, _item: object) -> bool:
        # UOE (composition refuse), not AttributeError / Column's PySparkValueError.
        self._refuse_composition("__contains__ / in")
        return False

    def __bool__(self) -> bool:
        """Raise — a pandas_udf marker has no truth value (parity with :class:`Column.__bool__`).

        Without this guard, Python ``and`` / ``or`` / ``not`` / ``if`` treat the marker as
        always-truthy and silently drop composition.
        """
        raise PySparkValueError(
            "Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
            "'~' for 'not' when building DataFrame boolean expressions."
        )

    __nonzero__ = __bool__


class PandasUDFFunction:
    """Callable from :func:`pandas_udf` — call with columns to build a projection/agg marker."""

    __slots__ = ("__name__", "_function_type", "_return_type_sql", "_user_func")

    def __init__(
        self,
        user_func: Callable[..., Any],
        return_type_sql: str,
        function_type: int = PandasUDFType.SCALAR,
    ) -> None:
        """Wrap a user function with its declared return type and eval type."""
        self._user_func = user_func
        self._return_type_sql = return_type_sql
        self._function_type = function_type
        self.__name__ = getattr(user_func, "__name__", "pandas_udf")

    def __call__(self, *args: Column | str) -> PandasUDFColumn:
        """Bind input columns; returns a :class:`PandasUDFColumn` for select/withColumn/agg."""
        if not args:
            raise PySparkTypeError(
                "pandas_udf requires at least one column argument (zero-arg form is unsupported)"
            )
        inputs: list[Column] = []
        for argument in args:
            if isinstance(argument, Column):
                inputs.append(argument)
            elif isinstance(argument, str):
                inputs.append(col(argument))
            else:
                raise PySparkTypeError(
                    "pandas_udf arguments must be Column or column-name str, "
                    f"got {type(argument).__name__}"
                )
        return PandasUDFColumn(
            self._user_func,
            self._return_type_sql,
            inputs,
            self.__name__,
            function_type=self._function_type,
        )


def _build_pandas_udf(
    user_func: Callable[..., Any],
    return_type: Any,
    function_type: Any,
) -> PandasUDFFunction:
    """Validate eval type + return type and wrap ``user_func`` as a :class:`PandasUDFFunction`."""
    if not callable(user_func):
        raise PySparkTypeError(f"pandas_udf func must be callable, got {type(user_func).__name__}")
    normalized_ft = _normalize_pandas_udf_function_type(function_type)
    return_type_sql = _normalize_pandas_udf_return_type_sql(return_type)
    return PandasUDFFunction(user_func, return_type_sql, function_type=normalized_ft)


def pandas_udf(
    f: Any = None,
    returnType: Any = None,  # noqa: N803 — PySpark camelCase
    functionType: Any = None,  # noqa: N803 — PySpark camelCase
) -> Any:
    """Vectorized pandas UDF decorator (PySpark ``functions.pandas_udf``).

    **Supported:**

    * **SCALAR** (default) — ``Series → Series`` in ``select`` / ``withColumn``.
    * **SCALAR_ITER** — ``Iterator[Series] → Iterator[Series]`` (or multi-arg
      ``Iterator[tuple[Series, …]]``) via the same Arrow bridge with a batch-iterator adapter.
    * **GROUPED_AGG** — ``Series → scalar`` in ``groupBy(...).agg(...)``. Mixed UDF and builtin
      aggregation is refused.

    Usage::

        @pandas_udf("long")
        def double_x(series: pd.Series) -> pd.Series:
            return series * 2

        df.select(double_x(df.x).alias("y"))
        df.withColumn("y", double_x("x"))

        @pandas_udf("long", PandasUDFType.SCALAR_ITER)
        def double_iter(batches):
            for series in batches:
                yield series * 2

        @pandas_udf("double", PandasUDFType.GROUPED_AGG)
        def mean_udf(series: pd.Series) -> float:
            return float(series.mean())

        df.groupBy("k").agg(mean_udf("v").alias("m"))

    SCALAR / SCALAR_ITER implementation is a **facade projection rewrite** over the deferred
    mapInArrow-style bridge (see :meth:`repark.dataframe.DataFrame._select_with_pandas_udfs`)
    — the UDF result is **not** a :class:`~repark.column.Column` expression in the SQL plan.
    Composition mid-expression is refused; materialize via select/withColumn first.

    Multi-UDF SCALAR ``select`` lists run in **one** mapInArrow pass per batch. Requires the
    optional ``pandas`` extra at execution time (import is deferred to the bridge).

    **OUT (loud):** ``GROUPED_MAP`` and window pandas_udf —
    :class:`~repark.errors.UnsupportedOperationException` naming the unsupported type.
    """
    # Direct: pandas_udf(fn, returnType[, functionType])
    if f is not None and callable(f) and not _is_pandas_udf_datatype_like(f):
        if returnType is None:
            raise PySparkTypeError("pandas_udf(func, returnType) requires returnType")
        return _build_pandas_udf(f, returnType, functionType)

    # @pandas_udf("long", PandasUDFType.SCALAR) — second positional is functionType
    if (
        f is not None
        and returnType is not None
        and functionType is None
        and _is_pandas_udf_datatype_like(f)
        and _is_pandas_udf_function_type(returnType)
    ):

        def _decorator_with_ft(func: Callable[..., Any]) -> PandasUDFFunction:
            return _build_pandas_udf(func, f, returnType)

        return _decorator_with_ft

    # @pandas_udf("long", "double") — two datatype positionals is not a legal form; the old
    # keyword fall-through silently took the second as returnType and dropped the first
    # are dual-datatype-looking only because every str is datatype-like, but they must reach
    if (
        f is not None
        and returnType is not None
        and functionType is None
        and _is_pandas_udf_datatype_like(f)
        and not _is_pandas_udf_function_type(f)
        and _is_pandas_udf_datatype_like(returnType)
        and not _is_pandas_udf_function_type(returnType)
    ):
        raise PySparkTypeError(
            "pandas_udf decorator second positional argument must be functionType "
            f"(SCALAR / PandasUDFType.*), not a second returnType; got {returnType!r}. "
            "Use @pandas_udf('long') or @pandas_udf('long', PandasUDFType.SCALAR)."
        )

    # first positional is a functionType tag and the second/kw is returnType. Old keyword
    # fall-through ignored ``f`` and built SCALAR (fail-open). Route through normalize so
    if (
        f is not None
        and returnType is not None
        and functionType is None
        and _is_pandas_udf_function_type(f)
        and _is_pandas_udf_datatype_like(returnType)
    ):

        def _decorator_ft_first(func: Callable[..., Any]) -> PandasUDFFunction:
            return _build_pandas_udf(func, returnType, f)

        return _decorator_ft_first

    # @pandas_udf("long") / @pandas_udf(LongType())
    if f is not None and returnType is None:
        if not _is_pandas_udf_datatype_like(f):
            raise PySparkTypeError(
                "pandas_udf decorator expects a returnType (DataType or str) as the first "
                f"argument, got {type(f).__name__}"
            )

        def _decorator(func: Callable[..., Any]) -> PandasUDFFunction:
            return _build_pandas_udf(func, f, functionType)

        return _decorator

    # @pandas_udf(returnType=..., functionType=...) — keyword / returnType-only form.
    # When the first positional is also a datatype (not a function), refuse rather than
    if returnType is not None:
        if f is not None and _is_pandas_udf_datatype_like(f) and not callable(f):
            raise PySparkTypeError(
                "pandas_udf received two returnType-like values; use a single returnType "
                f"(first={f!r}, returnType={returnType!r})"
            )

        def _decorator_kw(func: Callable[..., Any]) -> PandasUDFFunction:
            return _build_pandas_udf(func, returnType, functionType)

        return _decorator_kw

    raise PySparkTypeError(
        "pandas_udf requires returnType (e.g. @pandas_udf('long') or "
        "pandas_udf(fn, returnType='long'))"
    )


# Classic scalar UDFs call Python once per row; pandas UDFs vectorize by batch.


def _is_python_udf_datatype_like(value: Any) -> bool:
    """True when ``value`` is a returnType (str DDL, repark/pyspark DataType, or duck-typed).

    Accept duck-typed DataType objects (``simpleString`` / ``jsonValue``) so callers that import
    Apache ``StringType()`` instances still validate as returnType.
    """
    if isinstance(value, str):
        return True
    from repark.spark.types import DataType

    if isinstance(value, DataType):
        return True
    # Duck-typed Spark DataType instance (not the class itself).
    if isinstance(value, type):
        return False
    simple = getattr(value, "simpleString", None)
    return callable(simple)


def _python_udf_arrow_type_for_return(data_type: Any) -> Any:
    """Map a Spark :class:`~repark.types.DataType` to a concrete Arrow type for re-ingest.

    Reuses the same fail-open string refuse as :func:`_pandas_udf_arrow_type_for_return`
    (variant / interval / time must not silently declare string).
    """
    from repark.spark.session import _data_type_to_sql_type, _sql_type_to_arrow
    from repark.spark.types import DataType

    if not isinstance(data_type, DataType):
        raise PySparkTypeError(
            f"udf returnType must be a DataType or DDL type string, got {type(data_type).__name__}"
        )
    try:
        sql_type = _data_type_to_sql_type(data_type)
    except Exception as error:
        raise PySparkTypeError(
            f"udf returnType {data_type.simpleString()!r} is not a supported scalar type: {error}"
        ) from error
    try:
        arrow_type = _sql_type_to_arrow(sql_type)
    except Exception as error:
        raise PySparkTypeError(
            f"udf returnType {data_type.simpleString()!r} is not a supported scalar type: {error}"
        ) from error
    # Shared refuse for variant/interval/time string fail-open (nested leaves too).
    _pandas_udf_refuse_fail_open_string_leaves(data_type, arrow_type)
    return arrow_type


def _normalize_python_udf_return_type_sql(return_type: Any) -> str:
    """Lower ``returnType`` to a logical DDL fragment (``DataType.simpleString``).

    Default when omitted is Spark's ``string``. Struct / field-list DDL is allowed for
    classic scalar UDFs (unlike :func:`pandas_udf` scalar which is Series-shaped).
    Accept duck-typed DataType instances via ``simpleString()``.
    """
    from repark.spark.types import DataType, StringType

    if return_type is None:
        return_type = StringType()
    if isinstance(return_type, str):
        text = return_type.strip()
        if not text:
            raise PySparkTypeError("udf returnType must be a non-empty type string")
        try:
            parsed = DataType.fromDDL(text)
        except Exception as error:
            raise PySparkTypeError(
                f"udf returnType {text!r} is not a valid type: {error}"
            ) from error
        _python_udf_arrow_type_for_return(parsed)
        return parsed.simpleString()
    if isinstance(return_type, DataType):
        _python_udf_arrow_type_for_return(return_type)
        return return_type.simpleString()
    # Duck-typed DataType (e.g. pyspark.sql.types.LongType instance).
    simple = getattr(return_type, "simpleString", None)
    if callable(simple) and not isinstance(return_type, type):
        try:
            text = str(simple()).strip()
        except Exception as error:
            raise PySparkTypeError(
                f"udf returnType {type(return_type).__name__} simpleString() failed: {error}"
            ) from error
        if not text:
            raise PySparkTypeError("udf returnType simpleString() must be non-empty")
        try:
            parsed = DataType.fromDDL(text)
        except Exception as error:
            raise PySparkTypeError(
                f"udf returnType {text!r} is not a valid type: {error}"
            ) from error
        _python_udf_arrow_type_for_return(parsed)
        return parsed.simpleString()
    raise PySparkTypeError(
        f"udf returnType must be a DataType or DDL type string, got {type(return_type).__name__}"
    )


class PythonUDFColumn:
    """Marker for a classic scalar ``udf`` projection (not a SQL-plan :class:`Column`).

    Produced by calling a :func:`udf`-decorated / :class:`UserDefinedFunction` with
    column arguments. Top-level ``select`` / ``withColumn`` only — mid-expression
    composition is refused (same class as :class:`PandasUDFColumn`).
    """

    __slots__ = (
        "_alias_name",
        "_function_name",
        "_inputs",
        "_return_type_sql",
        "_user_func",
    )

    def __init__(
        self,
        user_func: Callable[..., Any],
        return_type_sql: str,
        inputs: list[Column],
        function_name: str,
        *,
        alias_name: str | None = None,
    ) -> None:
        """Bind the user function, declared return type, and input Columns."""
        self._user_func = user_func
        # Revalidate every construction path (hostile constructor / post-build mutation).
        self._return_type_sql = _normalize_python_udf_return_type_sql(return_type_sql)
        self._inputs = list(inputs)
        self._function_name = function_name
        self._alias_name = alias_name

    def alias(self, name: str) -> PythonUDFColumn:
        """Set the output column name (PySpark ``Column.alias`` parity for UDF results)."""
        if not isinstance(name, str) or name.strip() == "":
            raise PySparkTypeError("udf alias name must be a non-empty str")
        return PythonUDFColumn(
            self._user_func,
            self._return_type_sql,
            self._inputs,
            self._function_name,
            alias_name=name,
        )

    def default_name(self) -> str:
        """Spark-style default projection name ``func(arg, …)`` when no ``.alias`` is set."""
        arg_parts: list[str] = []
        for column in self._inputs:
            if column._projection_name is not None and column._stable_name:
                arg_parts.append(column._projection_name)
            else:
                arg_parts.append(column.spark_display_part())
        return f"{self._function_name}({', '.join(arg_parts)})"

    def output_name(self) -> str:
        """Resolved output field name (alias wins over :meth:`default_name`)."""
        if self._alias_name is not None:
            return self._alias_name
        return self.default_name()

    def _refuse_composition(self, surface: str) -> None:
        """Refuse composition because this marker is not a SQL Column expression."""
        raise UnsupportedOperationException(
            f"udf result cannot be used in {surface} in repark v1 "
            "(facade projection-rewrite bridge only; not a Column expression in the SQL plan). "
            "Materialize via select/withColumn, then apply further expressions on that column. "
            "Mid-expression embedding is a follow-on seed."
        )

    def __add__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (+)")

    def __radd__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (+)")

    def __sub__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (-)")

    def __rsub__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (-)")

    def __mul__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (*)")

    def __rmul__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (*)")

    def __truediv__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (/)")

    def __rtruediv__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (/)")

    def __mod__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (%)")

    def __rmod__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (%)")

    def __pow__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (**)")

    def __rpow__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (**)")

    def __neg__(self) -> None:
        self._refuse_composition("unary (-)")

    def __eq__(self, _other: Any) -> bool:  # type: ignore[override]
        self._refuse_composition("comparison (==)")
        return False

    def __ne__(self, _other: Any) -> bool:  # type: ignore[override]
        self._refuse_composition("comparison (!=)")
        return False

    def __lt__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (<)")
        return False

    def __le__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (<=)")
        return False

    def __gt__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (>)")
        return False

    def __ge__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (>=)")
        return False

    def __and__(self, _other: Any) -> None:
        self._refuse_composition("logical (&)")

    def __rand__(self, _other: Any) -> None:
        self._refuse_composition("logical (&)")

    def __or__(self, _other: Any) -> None:
        self._refuse_composition("logical (|)")

    def __ror__(self, _other: Any) -> None:
        self._refuse_composition("logical (|)")

    def __invert__(self) -> None:
        self._refuse_composition("logical (~)")

    def cast(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``cast`` — select the marker first, then cast the materialized column."""
        self._refuse_composition("cast")

    def over(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``over`` — a classic scalar ``udf`` has no windowed form on repark."""
        self._refuse_composition("window .over")

    def is_null(self) -> None:
        """Refuse ``isNull`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("isNull")

    isNull = is_null  # noqa: N815 — PySpark camelCase alias

    def is_not_null(self) -> None:
        """Refuse ``isNotNull`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("isNotNull")

    isNotNull = is_not_null  # noqa: N815 — PySpark camelCase alias

    def between(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``between`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("between")

    def eqNullSafe(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        """Refuse ``eqNullSafe`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("eqNullSafe")

    def when(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``when`` — a udf marker cannot open a ``CASE`` arm."""
        self._refuse_composition("when")

    def otherwise(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``otherwise`` — a udf marker cannot close a ``CASE`` arm."""
        self._refuse_composition("otherwise")

    def asc(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``asc`` — sort on the materialized column instead of the marker."""
        self._refuse_composition("asc")

    def desc(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``desc`` — sort on the materialized column instead of the marker."""
        self._refuse_composition("desc")

    def contains(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``contains`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (contains)")

    def startswith(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``startswith`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (startswith)")

    def endswith(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``endswith`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (endswith)")

    def like(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``like`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (like)")

    def ilike(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``ilike`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (ilike)")

    def rlike(self, *_args: Any, **_kwargs: Any) -> None:
        """Refuse ``rlike`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("string predicate (rlike)")

    def bitwiseAND(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        """Refuse ``bitwiseAND`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("bitwiseAND")

    def bitwiseOR(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        """Refuse ``bitwiseOR`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("bitwiseOR")

    def bitwiseXOR(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        """Refuse ``bitwiseXOR`` — a udf marker is not a plan :class:`Column`."""
        self._refuse_composition("bitwiseXOR")

    def __contains__(self, _item: object) -> bool:
        self._refuse_composition("__contains__ / in")
        return False

    def __bool__(self) -> bool:
        """Raise — a udf marker has no truth value (parity with :class:`Column.__bool__`)."""
        raise PySparkValueError(
            "Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
            "'~' for 'not' when building DataFrame boolean expressions."
        )

    __nonzero__ = __bool__


class UserDefinedFunction:
    """Callable from :func:`udf` / :meth:`UDFRegistration.register` (PySpark name).

    Call with column arguments to build a :class:`PythonUDFColumn` for select/withColumn.
    **Cost:** each row invokes the Python function once (per-row scalar UDF — not vectorized).
    Prefer :func:`pandas_udf` for Series-batch throughput.

    ``deterministic`` defaults to ``True`` (Spark parity); :meth:`asNondeterministic`
    flips it to ``False`` (accepted flag; repark has no Spark codegen path that
    consults it for fold/cache; the flag is surface-honest only).
    """

    __slots__ = ("__name__", "_deterministic", "_return_type_sql", "_user_func")

    def __init__(
        self,
        user_func: Callable[..., Any],
        return_type_sql: str,
        *,
        name: str | None = None,
        deterministic: bool = True,
    ) -> None:
        """Wrap a user function with its declared return type."""
        if not callable(user_func):
            raise PySparkTypeError(f"udf func must be callable, got {type(user_func).__name__}")
        # Defense in depth — direct ``UserDefinedFunction(udtf_obj, …)`` must not half-wire.
        _refuse_udtf_as_scalar_udf(user_func, surface="UserDefinedFunction")
        self._user_func = user_func
        self._return_type_sql = _normalize_python_udf_return_type_sql(return_type_sql)
        self.__name__ = name if name is not None else getattr(user_func, "__name__", "udf")
        self._deterministic = bool(deterministic)

    @property
    def deterministic(self) -> bool:
        """Whether the UDF is marked deterministic (Spark ``UserDefinedFunction.deterministic``)."""
        return self._deterministic

    def __call__(self, *args: Column | str) -> PythonUDFColumn:
        """Bind input columns; returns a :class:`PythonUDFColumn` for select/withColumn."""
        if not args:
            raise PySparkTypeError(
                "udf requires at least one column argument (zero-arg form is unsupported)"
            )
        inputs: list[Column] = []
        for argument in args:
            if isinstance(argument, Column):
                inputs.append(argument)
            elif isinstance(argument, str):
                inputs.append(col(argument))
            else:
                raise PySparkTypeError(
                    "udf arguments must be Column or column-name str, "
                    f"got {type(argument).__name__}"
                )
        return PythonUDFColumn(
            self._user_func,
            self._return_type_sql,
            inputs,
            self.__name__,
        )

    def asNondeterministic(self) -> UserDefinedFunction:  # noqa: N802 — PySpark camelCase
        """Mark the UDF nondeterministic (Spark parity flag; no codegen path in repark)."""
        self._deterministic = False
        return self


def _refuse_udtf_as_scalar_udf(user_func: Any, *, surface: str) -> None:
    """Refuse wrapping a table UDTF as a classic scalar UDF.

    ``UserDefinedTableFunction`` is callable (a scalar-argument call produces a DataFrame in
    the FROM path). Without this gate ``F.udf(udtf_obj)`` /
    ``spark.udf.register(name, udtf_obj)``
    would half-wire a table function as a scalar UDF.
    """
    if isinstance(user_func, UserDefinedTableFunction):
        raise PySparkTypeError(
            f"{surface} does not accept UserDefinedTableFunction (table UDTF). "
            "Use spark.udtf.register / @udtf for table functions (U12 scalar-arg "
            "core via mapInArrow), or pass a scalar Python callable to F.udf / "
            "spark.udf.register."
        )


def _build_python_udf(
    user_func: Callable[..., Any],
    return_type: Any,
    *,
    name: str | None = None,
) -> UserDefinedFunction:
    """Construct a :class:`UserDefinedFunction` with validated return type."""
    if not callable(user_func):
        raise PySparkTypeError(f"udf func must be callable, got {type(user_func).__name__}")
    _refuse_udtf_as_scalar_udf(user_func, surface="F.udf / spark.udf.register")
    return_type_sql = _normalize_python_udf_return_type_sql(return_type)
    return UserDefinedFunction(user_func, return_type_sql, name=name)


def udf(
    f: Callable[..., Any] | Any | None = None,
    returnType: Any = None,  # noqa: N803 — PySpark camelCase
    *,
    useArrow: bool | None = None,  # noqa: N803 — PySpark camelCase (accepted, ignored)
) -> UserDefinedFunction | Callable[[Callable[..., Any]], UserDefinedFunction]:
    """Classic scalar Python UDF decorator (PySpark ``functions.udf``).

    **Per-row cost (honest):** each input row is a separate Python call. Arrow batches
    stream through the mapInArrow bridge; inside each batch the facade walks rows and
    invokes ``f`` once per row. Prefer :func:`pandas_udf` for vectorized Series→Series
    throughput on the same bridge.

    Forms::

        @udf("long")
        def double(x: int | None) -> int | None:
            return None if x is None else x * 2

        @udf(returnType=LongType())
        def double2(x: int | None) -> int | None:
            return None if x is None else x * 2

        double = udf(lambda x: x * 2 if x is not None else None, "long")

        df.select(double("a"))
        df.withColumn("b", double(col("a")))

    Null semantics: SQL NULL arrives as Python ``None``; return ``None`` for NULL.
    ``returnType`` defaults to ``string`` when omitted (Spark contract).
    ``useArrow`` is accepted for PySpark signature parity and ignored (repark always
    uses the Arrow mapInArrow bridge).
    """
    from repark.spark.types import StringType

    _ = useArrow  # PySpark parity; repark bridge is always Arrow

    # Direct: udf(fn, returnType)
    if f is not None and callable(f) and not _is_python_udf_datatype_like(f):
        resolved = returnType if returnType is not None else StringType()
        return _build_python_udf(f, resolved)

    # @udf("long") / @udf(LongType()) — first positional is returnType
    if f is not None and _is_python_udf_datatype_like(f) and returnType is None:

        def _decorator_type(func: Callable[..., Any]) -> UserDefinedFunction:
            return _build_python_udf(func, f)

        return _decorator_type

    # @udf / @udf() — default StringType
    if f is None and returnType is None:

        def _decorator_default(func: Callable[..., Any]) -> UserDefinedFunction:
            return _build_python_udf(func, StringType())

        return _decorator_default

    # @udf(returnType=...) / udf(returnType=...)
    if f is None and returnType is not None:

        def _decorator_kw(func: Callable[..., Any]) -> UserDefinedFunction:
            return _build_python_udf(func, returnType)

        return _decorator_kw

    # udf(fn, returnType=...) already handled; dual-datatype positionals refuse
    if (
        f is not None
        and _is_python_udf_datatype_like(f)
        and returnType is not None
        and _is_python_udf_datatype_like(returnType)
    ):
        raise PySparkTypeError(
            "udf decorator second positional argument must not be a second returnType; "
            "use @udf('long') or udf(fn, returnType='long')."
        )

    raise PySparkTypeError(
        "udf requires a callable and optional returnType "
        "(e.g. @udf('long') or udf(fn, returnType='long') or @udf(returnType='long'))"
    )
