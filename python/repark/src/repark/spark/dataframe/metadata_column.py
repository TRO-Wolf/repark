"""`DataFrame.metadataColumn` and the hidden file-source `_metadata` binding.

The struct itself is built in Rust (`crates/repark-core/src/file_metadata.rs`,
projection-driven per-file UNION augmentation); this module holds names, argument
shapes, and API plumbing only: the `NOT_STR` check, the eager frame/name validation
with Spark's `UNRESOLVED_COLUMN` (and shadowed `MISSING_ATTRIBUTES`) conditions,
the `select`-string funnel, and the `MetadataColumn.getField` dot naming the oracle pins.
"""

from __future__ import annotations

from types import MethodType
from typing import Any

from repark import _native
from repark.errors import AnalysisException, PySparkTypeError
from repark.spark.column import Column

_HIDDEN_NAMES = ("_metadata", "__metadata")

_FILE_SOURCE_METADATA: dict[str, bool | str] = {
    "__file_source_metadata_col": True,
    "__metadata_col": "_metadata",
}


def _conditioned(
    error: AnalysisException, error_class: str, params: dict[str, str], sql_state: str
) -> AnalysisException:
    """Attach Spark's structured-error surface to a facade-raised error."""
    error._spark_error_class = error_class
    error._spark_message_parameters = dict(params)
    error._spark_sql_state = sql_state
    error.getCondition = MethodType(lambda self: self._spark_error_class, error)
    error.getErrorClass = MethodType(lambda self: self._spark_error_class, error)
    error.getMessageParameters = MethodType(
        lambda self: dict(self._spark_message_parameters), error
    )
    error.getSqlState = MethodType(lambda self: self._spark_sql_state, error)
    return error


def _raise_without_suggestion(name: str) -> Any:
    """Raise Spark's `UNRESOLVED_COLUMN.WITHOUT_SUGGESTION` for a name."""
    message = (
        "[UNRESOLVED_COLUMN.WITHOUT_SUGGESTION] A column, variable, or function "
        f"parameter with name `{name}` cannot be resolved.  SQLSTATE: 42703"
    )
    raise _conditioned(
        AnalysisException(message),
        "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
        {"objectName": f"`{name}`"},
        "42703",
    )


def _raise_with_suggestion(name: str, proposal: str) -> Any:
    """Raise Spark's `UNRESOLVED_COLUMN.WITH_SUGGESTION` proposing one name."""
    message = (
        "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function "
        f"parameter with name `{name}` cannot be resolved. Did you mean one of the "
        f"following? [`{proposal}`]. SQLSTATE: 42703"
    )
    raise _conditioned(
        AnalysisException(message),
        "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        {"objectName": f"`{name}`", "proposal": f"`{proposal}`"},
        "42703",
    )


def _hidden_name(frame: Any) -> str | None:
    """Return the hidden metadata name this frame answers, if any."""
    return frame._plan().file_metadata_hidden_name()


class MetadataColumn(Column):
    """A `metadataColumn` result bound to its frame's hidden metadata attribute."""

    def __init__(self, frame: Any, hidden: str) -> None:
        """Bind the hidden struct column of a file-source frame."""
        self._metadata_hidden = hidden
        super().__init__(
            _native.PyColumn.column(hidden),
            spark_display=hidden,
            projection_name=hidden,
            stable_name=True,
            has_free_attribute=True,
            sql_expr=f'"{hidden}"',
            alias_metadata=dict(_FILE_SOURCE_METADATA),
        )

    def getField(self, name: str) -> Column:  # noqa: N802 — PySpark method name
        """Return the hidden struct field with Spark's dotted projection name."""
        dotted = f"{self._metadata_hidden}.{name}"
        return Column(
            _native.metadata_field(self._metadata_hidden, name),
            spark_display=dotted,
            projection_name=dotted,
            stable_name=False,
            has_free_attribute=True,
            sql_expr=f'"{self._metadata_hidden}"."{name}"',
        )


def metadataColumn(frame: Any, name: str) -> Column:  # noqa: N802 — PySpark method name
    """Return the file-source metadata column bound to this frame (PySpark shape).

    The name resolves against the hidden metadata namespace only: a regular column
    name, or any unknown name, raises `UNRESOLVED_COLUMN.WITH_SUGGESTION` proposing
    the hidden name. Frames without file provenance raise `WITHOUT_SUGGESTION`.
    """
    if not isinstance(name, str):
        raise PySparkTypeError(
            message=(f"[NOT_STR] Argument `colName` should be a str, got {type(name).__name__}."),
            errorClass="NOT_STR",
            messageParameters={"arg_name": "colName", "arg_type": type(name).__name__},
        )
    hidden = _hidden_name(frame)
    if hidden is None:
        _raise_without_suggestion(name)
    assert hidden is not None
    if hidden == "__metadata":
        listed = ", ".join(f'"{field}"' for field in frame.columns)
        message = (
            "[MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT] "
            f'Resolved attribute(s) "__metadata" missing from {listed} '
            "in operator !Project [__metadata].  SQLSTATE: XX000;"
        )
        raise _conditioned(
            AnalysisException(message),
            "MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT",
            {},
            "XX000",
        )
    if name != hidden:
        _raise_with_suggestion(name, hidden)
    return MetadataColumn(frame, hidden)


def bind_if_file_metadata(frame: Any, item: str) -> Column:
    """Resolve a select-string, routing hidden `_metadata` references to Rust."""
    head, dot, _ = item.partition(".")
    if (
        item[:1] in ('"', "`")
        or head not in _HIDDEN_NAMES
        or head in frame.columns
        or head.casefold() in {name.casefold() for name in frame.columns}
    ):
        return frame._bind_schema_column(item)
    hidden = _hidden_name(frame)
    if hidden is None:
        _raise_without_suggestion(item)
    assert hidden is not None
    if not dot:
        return Column(
            _native.PyColumn.column(hidden),
            spark_display=hidden,
            projection_name=hidden,
            stable_name=True,
            has_free_attribute=True,
            sql_expr=f'"{hidden}"',
            alias_metadata=dict(_FILE_SOURCE_METADATA),
        )
    field = item.partition(".")[2]
    if "." in field:
        return frame._bind_schema_column(item)
    return Column(
        _native.metadata_field(hidden, field),
        spark_display=item,
        projection_name=field,
        stable_name=True,
        has_free_attribute=True,
        sql_expr=f'"{hidden}"."{field}"',
    )
