"""Surface-a DataFrame bodies bound on ``DataFrame`` from ``core.py`` (DF-SURFACE-A-1)."""

from __future__ import annotations

import hashlib
import re
import warnings
from typing import TYPE_CHECKING, Any

from repark.errors import AnalysisException, PySparkTypeError, PySparkValueError
from repark.spark.functions import lit
from repark.spark.types import (
    ArrayType,
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

_SCRATCH_NAME_RE = re.compile(r"__repark_[A-Za-z]+_[0-9a-f]{6,}")

_COMMON_EXPR_RE = re.compile(r"__common_expr_\d+")

_ALIAS_RE = re.compile(
    r"\b(?:AS|as) ([A-Za-z_][\w$]*|`[^`]*`|\"[^\"]*\")(?=\s*[,\]\)\n]|$)",
    re.MULTILINE,
)

_PLAN_TYPE_TOKENS = frozenset(
    {
        "Binary",
        "BinaryView",
        "Boolean",
        "Date32",
        "Date64",
        "Decimal128",
        "Decimal256",
        "Dictionary",
        "Duration",
        "FixedSizeBinary",
        "Float16",
        "Float32",
        "Float64",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "Interval",
        "LargeBinary",
        "LargeList",
        "LargeListView",
        "LargeUtf8",
        "List",
        "ListView",
        "Map",
        "Null",
        "RunEndEncoded",
        "Struct",
        "Time32",
        "Time64",
        "Timestamp",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Union",
        "Utf8",
        "Utf8View",
    }
)


def _nullable_column_message(path: str) -> str:
    return (
        f"[NULLABLE_COLUMN_OR_FIELD] Column or field `{path}` is nullable "
        "while it's required to be non-nullable. SQLSTATE: 42000"
    )


def _type_label(data_type: DataType) -> str:
    return data_type.simpleString().upper()


def _store_assignment_ok(source: DataType, target: DataType, path: str) -> bool:
    if isinstance(source, NullType):
        return True
    if source == target:
        return True
    if isinstance(source, _NUMERIC_TYPES) and isinstance(target, _NUMERIC_TYPES):
        return not (isinstance(source, DecimalType) and isinstance(target, DecimalType))
    if isinstance(source, _STRING_TYPES) and isinstance(target, _STRING_TYPES):
        return True
    if (type(source), type(target)) in _TEMPORAL_PAIRS:
        return True
    if isinstance(source, StructType) and isinstance(target, StructType):
        _require_struct_assignable(source, target, path)
        return True
    if isinstance(source, ArrayType) and isinstance(target, ArrayType):
        return _store_assignment_ok(source.elementType, target.elementType, path)
    if isinstance(source, MapType) and isinstance(target, MapType):
        return _store_assignment_ok(source.keyType, target.keyType, path) and _store_assignment_ok(
            source.valueType, target.valueType, path
        )
    return False


def _require_struct_assignable(source: StructType, target: StructType, path: str) -> None:
    by_fold: dict[str, StructField] = {}
    for field in source.fields:
        by_fold.setdefault(field.name.casefold(), field)
    for field in target.fields:
        nested_path = f"{path}.{field.name}"
        found = by_fold.get(field.name.casefold())
        if found is None:
            if not field.nullable:
                raise AnalysisException(_nullable_column_message(nested_path))
            continue
        _require_store_assignment(found, field, nested_path)


def _require_store_assignment(source: StructField, target: StructField, path: str) -> None:
    if source.nullable and not target.nullable:
        raise AnalysisException(_nullable_column_message(path))
    if not _store_assignment_ok(source.dataType, target.dataType, path):
        raise AnalysisException(
            f"[INVALID_COLUMN_OR_FIELD_DATA_TYPE] Column or field `{path}` is of type "
            f'"{_type_label(source.dataType)}" while it\'s required to be '
            f'"{_type_label(target.dataType)}". SQLSTATE: 42000'
        )


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
            projected.append(lit(None).cast(field.dataType).alias(field.name))
            continue
        _require_store_assignment(source, field, field.name)
        projected.append(frame[source.name].cast(field.dataType).alias(field.name))
    child = frame.select(*projected)
    child._schema_override = schema
    return child


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
    resolved = frame._resolve_getitem_column_name(columnName)
    projected = [
        frame[name].alias(name, metadata=metadata) if name == resolved else frame[name]
        for name in frame.columns
    ]
    child = frame.select(*projected)
    child._schema_override = StructType(
        [
            StructField(
                field.name,
                field.dataType,
                field.nullable,
                dict(metadata) if field.name == resolved else dict(field.metadata),
            )
            for field in frame.schema.fields
        ]
    )
    return child


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
    from repark.spark.session import SparkSession as _SparkSession

    return _SparkSession.getActiveSession()


def isStreaming(frame: DataFrame) -> bool:  # noqa: N802
    """Whether this is a streaming DataFrame; always ``False`` (batch-only)."""
    return False


def isLocal(frame: DataFrame) -> bool:  # noqa: N802
    """Whether the plan is local; always ``False`` (R-4). pins: C-004."""
    frame._ensure_alive()
    return False


def _matching_brace(text: str, open_index: int) -> int:
    closers = {"{": "}", "[": "]", "(": ")"}
    stack = [closers[text[open_index]]]
    index = open_index + 1
    while stack and index < len(text):
        char = text[index]
        if char in closers:
            stack.append(closers[char])
        elif char == stack[-1]:
            stack.pop()
        index += 1
    return index


def _inner_list_items(block: str) -> list[str]:
    items: list[str] = []
    depth = 0
    item_start: int | None = None
    for index, char in enumerate(block):
        if char == "[":
            depth += 1
            if depth == 2:
                item_start = index + 1
        elif char == "]":
            if depth == 2 and item_start is not None:
                segment = block[item_start:index]
                items.extend(part.strip() for part in segment.split(", ") if part.strip())
                item_start = None
            depth -= 1
    return items


def _file_group_paths(plan_text: str) -> list[str]:
    paths: list[str] = []
    marker = "file_groups={"
    index = 0
    while (start := plan_text.find(marker, index)) != -1:
        open_index = start + len(marker) - 1
        end = _matching_brace(plan_text, open_index)
        paths.extend(_inner_list_items(plan_text[open_index + 1 : end - 1]))
        index = end
    return paths


def _file_uri(path: str) -> str:
    if "://" in path:
        return path
    return "file://" + (path if path.startswith("/") else f"/{path}")


def inputFiles(frame: DataFrame) -> list[str]:  # noqa: N802
    """Absolute ``file:`` URIs scanned by a file-based read. pins: C-004."""
    frame._ensure_alive()
    paths = _file_group_paths(frame._explain_text("simple"))
    return [_file_uri(path) for path in dict.fromkeys(paths)]


def executionInfo(frame: DataFrame) -> Any:  # noqa: N802
    """Classic-mode refusal (PySpark ``DataFrame.executionInfo``). pins: df-surface-a-1/C-005"""
    frame._ensure_alive()
    raise PySparkValueError(
        message="[CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF] Calling property or member "
        "'queryExecution' is not supported in PySpark Classic, please use Spark Connect instead.",
        errorClass="CLASSIC_OPERATION_NOT_SUPPORTED_ON_DF",
        messageParameters={"member": "queryExecution"},
    )


def _alias_replacement(match: re.Match[str]) -> str:
    token = match.group(1)
    if token in _PLAN_TYPE_TOKENS:
        return match.group(0)
    return match.group(0)[: -len(token)] + "_"


def _normalized_plan_text(plan_text: str) -> str:
    text = _SCRATCH_NAME_RE.sub("__repark", plan_text)
    text = _COMMON_EXPR_RE.sub("__common_expr", text)
    return _ALIAS_RE.sub(_alias_replacement, text)


def semanticHash(frame: DataFrame) -> int:  # noqa: N802
    """Plan-text fingerprint with projection aliases normalized. pins: C-005."""
    frame._ensure_alive()
    normalized = _normalized_plan_text(frame._explain_text("extended"))
    digest = hashlib.sha256(normalized.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "little", signed=True)
