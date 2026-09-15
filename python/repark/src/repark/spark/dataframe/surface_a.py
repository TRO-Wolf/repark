"""Surface-a DataFrame bodies bound on ``DataFrame`` from ``core.py`` (DF-SURFACE-A-1)."""

from __future__ import annotations

import warnings
from types import MethodType
from typing import TYPE_CHECKING, Any

from repark.errors import AnalysisException, PySparkTypeError, PySparkValueError
from repark.spark import functions as F  # noqa: N812
from repark.spark._integral import (
    _attached_error_class,
    _attached_message_parameters,
    _attached_sql_state,
)
from repark.spark.column import Column
from repark.spark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    CharType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    VarcharType,
)

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame
    from repark.spark.session import SparkSession

_NUMERIC_TYPES = (
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
)

_STRING_TYPES = (StringType, CharType, VarcharType)

_ATOMIC_TYPES = (
    *_NUMERIC_TYPES,
    BooleanType,
    DateType,
    TimestampType,
    TimestampNTZType,
    BinaryType,
)

_CONTAINER_TYPES = (StructType, ArrayType, MapType)

_TEMPORAL_PAIRS = frozenset(
    {
        (DateType, TimestampType),
        (DateType, TimestampNTZType),
        (TimestampType, DateType),
        (TimestampType, TimestampNTZType),
        (TimestampNTZType, DateType),
        (TimestampNTZType, TimestampType),
    }
)


def _nullable_column_message(path: str) -> str:
    return (
        f"[NULLABLE_COLUMN_OR_FIELD] Column or field `{path}` is nullable "
        "while it's required to be non-nullable. SQLSTATE: 42000"
    )


def _type_label(data_type: DataType) -> str:
    return data_type.simpleString().upper()


def _decimal_widens(source: DecimalType, target: DecimalType) -> bool:
    return (target.precision - target.scale) >= (
        source.precision - source.scale
    ) and target.scale >= source.scale


def _store_assignment_ok(source: DataType, target: DataType) -> bool:
    if isinstance(source, NullType):
        return True
    if source == target:
        return True
    if isinstance(source, DecimalType) and isinstance(target, DecimalType):
        return _decimal_widens(source, target)
    if isinstance(source, _NUMERIC_TYPES) and isinstance(target, _NUMERIC_TYPES):
        return True
    if isinstance(source, _ATOMIC_TYPES) and isinstance(target, _STRING_TYPES):
        return True
    if isinstance(source, _STRING_TYPES) and isinstance(target, _STRING_TYPES):
        return True
    return (type(source), type(target)) in _TEMPORAL_PAIRS


def _invalid_column_type(source: DataType, target: DataType, path: str) -> AnalysisException:
    return AnalysisException(
        f"[INVALID_COLUMN_OR_FIELD_DATA_TYPE] Column or field `{path}` is of type "
        f'"{_type_label(source)}" while it\'s required to be '
        f'"{_type_label(target)}". SQLSTATE: 42000'
    )


def _typed_null(data_type: DataType) -> Column:
    if isinstance(data_type, StructType):
        args: list[Any] = []
        for field in data_type.fields:
            args += [field.name, _typed_null(field.dataType)]
        return F.named_struct(*args)
    if isinstance(data_type, ArrayType):
        return F.array(_typed_null(data_type.elementType))
    if isinstance(data_type, MapType):
        return F.create_map(_typed_null(data_type.keyType), _typed_null(data_type.valueType))
    return F.lit(None).cast(data_type)


def _null_of(data_type: DataType) -> Column:
    if isinstance(data_type, _CONTAINER_TYPES):
        return F.when(F.lit(False), _typed_null(data_type)).otherwise(F.lit(None))
    return F.lit(None).cast(data_type)


def _reconcile_struct(expr: Column, source: StructType, target: StructType, path: str) -> Column:
    by_fold: dict[str, StructField] = {}
    for field in source.fields:
        by_fold.setdefault(field.name.casefold(), field)
    args: list[Any] = []
    for field in target.fields:
        inner_path = f"{path}.{field.name}"
        found = by_fold.get(field.name.casefold())
        if found is None:
            if not field.nullable:
                raise AnalysisException(_nullable_column_message(inner_path))
            value = _null_of(field.dataType)
        else:
            if found.nullable and not field.nullable:
                raise AnalysisException(_nullable_column_message(inner_path))
            value = _reconcile(
                expr.getField(found.name), found.dataType, field.dataType, inner_path
            )
        args += [field.name, value]
    return F.named_struct(*args)


def _reconcile(expr: Column, source: DataType, target: DataType, path: str) -> Column:
    if source == target:
        return expr
    if isinstance(source, StructType) and isinstance(target, StructType):
        return _reconcile_struct(expr, source, target, path)
    if isinstance(source, ArrayType) and isinstance(target, ArrayType):
        return F.transform(
            expr,
            lambda x: _reconcile(x, source.elementType, target.elementType, f"{path}.element"),
        )
    if isinstance(source, MapType) and isinstance(target, MapType):
        out = expr
        if source.keyType != target.keyType:
            out = F.transform_keys(
                out,
                lambda k, v: _reconcile(k, source.keyType, target.keyType, f"{path}.key"),
            )
        if source.valueType != target.valueType:
            out = F.transform_values(
                out,
                lambda k, v: _reconcile(v, source.valueType, target.valueType, f"{path}.value"),
            )
        return out
    if not _store_assignment_ok(source, target):
        raise _invalid_column_type(source, target, path)
    return expr.cast(target)


def to(frame: DataFrame, schema: Any) -> DataFrame:
    """Reconcile the frame to ``schema`` (PySpark ``DataFrame.to``). pins: df-surface-a-1/C-001."""
    frame._ensure_alive()
    if not isinstance(schema, StructType):
        raise PySparkTypeError(
            message="[NOT_STRUCT] Argument `schema` should be a StructType, "
            f"got {type(schema).__name__}.",
            errorClass="NOT_STRUCT",
            messageParameters={
                "arg_name": "schema",
                "arg_type": type(schema).__name__,
            },
        )
    source_fields: dict[str, StructField] = {}
    for field in frame.schema.fields:
        source_fields.setdefault(field.name.casefold(), field)
    projected = []
    for field in schema.fields:
        source = source_fields.get(field.name.casefold())
        if source is None:
            if not field.nullable:
                raise AnalysisException(_nullable_column_message(field.name))
            expr = _null_of(field.dataType)
        else:
            if source.nullable and not field.nullable:
                raise AnalysisException(_nullable_column_message(field.name))
            expr = _reconcile(frame[source.name], source.dataType, field.dataType, field.name)
        projected.append(expr.alias(field.name))
    return frame.select(*projected)


def _raise_unresolved_column(name: str, names: list[str]) -> None:
    proposal = ", ".join(f"`{column}`" for column in names)
    parameters = {"objectName": f"`{name}`", "proposal": proposal}
    error = AnalysisException(
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter "
        f"with name `{name}` cannot be resolved. Did you mean one of the following? "
        f"[{proposal}]. SQLSTATE: 42703"
    )
    error._spark_error_class = "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    error._spark_message_parameters = parameters
    error._spark_sql_state = "42703"
    error.getErrorClass = MethodType(_attached_error_class, error)
    error.getCondition = MethodType(_attached_error_class, error)
    error.getMessageParameters = MethodType(_attached_message_parameters, error)
    error.getSqlState = MethodType(_attached_sql_state, error)
    raise error


def withMetadata(frame: DataFrame, columnName: str, metadata: dict) -> DataFrame:  # noqa: N802 N803
    """Replace one field's metadata (PySpark ``DataFrame.withMetadata``). pins: C-002."""
    frame._ensure_alive()
    if not isinstance(metadata, dict):
        raise PySparkTypeError(
            message="[NOT_DICT] Argument `metadata` should be a dict, "
            f"got {type(metadata).__name__}.",
            errorClass="NOT_DICT",
            messageParameters={
                "arg_name": "metadata",
                "arg_type": type(metadata).__name__,
            },
        )
    names = frame.columns
    folded = columnName.casefold()
    if columnName not in names and not any(name.casefold() == folded for name in names):
        _raise_unresolved_column(columnName, names)
    resolved = frame._resolve_getitem_column_name(columnName)
    projected = [frame[name].alias(name) if name == resolved else frame[name] for name in names]
    return frame.select(*projected)


def registerTempTable(frame: DataFrame, name: str) -> None:  # noqa: N802
    """Deprecated alias for ``createOrReplaceTempView`` (PySpark). pins: C-003."""
    frame._ensure_alive()
    warnings.warn(
        "Deprecated in 2.0, use createOrReplaceTempView instead.",
        FutureWarning,
        stacklevel=2,
    )
    frame.create_or_replace_temp_view(name)


def localCheckpoint(  # noqa: N802
    frame: DataFrame,
    eager: bool = True,
    storageLevel: Any = None,  # noqa: N803
) -> DataFrame:
    """Truncate lineage by materializing to a MemTable (PySpark ``localCheckpoint``)."""
    _ = storageLevel
    frame._ensure_alive()
    frame._checkpoint_lazy = True
    frame._persist_requested = False
    frame._storage_level = None
    if eager:
        frame._materialize_cache_if_needed()
    return frame


def checkpoint(frame: DataFrame, eager: bool = True) -> DataFrame:
    """Materialize like ``localCheckpoint`` (DF-CHECKPOINT-1). pins: C-003."""
    return localCheckpoint(frame._identity_child(), eager=eager)


def sparkSession(frame: DataFrame) -> SparkSession:  # noqa: N802
    """Return the owning session (PySpark ``DataFrame.sparkSession``). pins: df-surface-a-1/C-003"""
    frame._ensure_alive()
    owner = frame._alive_token.get("facade_session")
    if owner is not None:
        return owner
    from repark.spark.session import SparkSession as _SparkSession

    return _SparkSession.getActiveSession()


def isStreaming(frame: DataFrame) -> bool:  # noqa: N802
    """Whether this is a streaming DataFrame; always ``False`` (batch-only)."""
    return False


def isLocal(frame: DataFrame) -> bool:  # noqa: N802
    """Whether the plan is local; always ``False`` (R-4). pins: C-004."""
    frame._ensure_alive()
    return False


def executionInfo(frame: DataFrame) -> Any:  # noqa: N802
    """Classic-mode refusal (PySpark ``DataFrame.executionInfo``). pins: df-surface-a-1/C-005"""
    frame._ensure_alive()
    raise PySparkValueError(
        message="[CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF] Calling property or member "
        "'queryExecution' is not supported in PySpark Classic, please use Spark Connect instead.",
        errorClass="CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF",
        messageParameters={"member": "queryExecution"},
    )
