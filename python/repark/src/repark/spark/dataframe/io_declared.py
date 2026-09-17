"""Declared IO refusals for the orc writer, the xml names, and the jdbc names."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, NoReturn

from repark.errors import AnalysisException, PySparkNotImplementedError
from repark.spark.dataframe.streaming_batch import _raise_analysis

if TYPE_CHECKING:
    from repark.spark.dataframe.core import DataFrame
    from repark.spark.dataframe.writer_readwriter import DataFrameWriter
    from repark.spark.session.reader import DataFrameReader

_JDBC_SAVE_MODES = ("append", "overwrite", "error", "errorifexists", "ignore", "default")

_XML_ROW_TAG_MESSAGE = (
    "[XML_ROW_TAG_MISSING] `rowTag` option is required for reading/writing files "
    "in XML format. SQLSTATE: 42KDF"
)

_INVALID_SAVE_MODE_MESSAGE = (
    '[INVALID_SAVE_MODE] The specified save mode "{mode}" is invalid. '
    'Valid save modes include "append", "overwrite", "ignore", "error", '
    '"errorifexists", and "default". SQLSTATE: 42000'
)


def _refuse(feature: str) -> NoReturn:
    """Raise Spark's ``NOT_IMPLEMENTED`` refusal for one IO feature."""
    raise PySparkNotImplementedError(
        f"[NOT_IMPLEMENTED] {feature} is not implemented.",
        errorClass="NOT_IMPLEMENTED",
        messageParameters={"feature": feature},
    )


def _refuse_missing_row_tag() -> NoReturn:
    """Raise Spark's ``XML_ROW_TAG_MISSING`` refusal."""
    _raise_analysis(
        _XML_ROW_TAG_MESSAGE,
        "XML_ROW_TAG_MISSING",
        message_parameters={"rowTag": "`rowTag`"},
        sql_state="42KDF",
    )


def _refuse_invalid_jdbc_mode(mode: str) -> NoReturn:
    """Raise Spark's ``INVALID_SAVE_MODE`` refusal for one JDBC write mode."""
    _raise_analysis(
        _INVALID_SAVE_MODE_MESSAGE.format(mode=mode),
        "INVALID_SAVE_MODE",
        message_parameters={"mode": f'"{mode}"'},
        sql_state="42000",
    )


def _row_tag_provided(row_tag: Any, options: Mapping[str, str]) -> bool:
    """Whether a ``rowTag`` came from the argument or the option map (case-insensitive)."""
    if row_tag is not None:
        return True
    return any(key.lower() == "rowtag" for key in options)


def _jdbc_alias(camel: str, camel_value: Any, snake: str, snake_value: Any) -> Any:
    """Resolve one jdbc parameter's keyword spellings; passing both raises ``TypeError``."""
    if camel_value is not None and snake_value is not None:
        raise TypeError(
            f"jdbc received both {camel} and {snake}; pass one spelling of the parameter"
        )
    return camel_value if camel_value is not None else snake_value


def _is_postgres_url(url: str) -> bool:
    """Whether a JDBC URL names PostgreSQL (the driver the native connector serves).

    libpq accepts ``postgres://`` as an alias of ``postgresql://``; the check is
    case-insensitive after stripping leading whitespace and the caller's string is
    forwarded untouched. ``jdbc:postgres://`` names no documented connector scheme
    and refuses.
    """
    lowered = str(url).lstrip().lower()
    return lowered.startswith(("jdbc:postgresql://", "postgresql://", "postgres://"))


def reader_xml(
    reader: DataFrameReader,
    path: Any,
    rowTag: Any = None,  # noqa: N803 — PySpark param name
    schema: Any = None,
    **options: Any,
) -> DataFrame:
    """Refuse XML reads after Spark's ``rowTag`` check. pins: io-declared-1/C-002"""
    _ = path, schema, options
    if not _row_tag_provided(rowTag, reader._options):
        _refuse_missing_row_tag()
    _refuse("xml")


def reader_jdbc(
    reader: DataFrameReader,
    url: str,
    table: str | None = None,
    column: str | None = None,
    lowerBound: int | None = None,  # noqa: N803 — PySpark param name
    upperBound: int | None = None,  # noqa: N803 — PySpark param name
    numPartitions: int | None = None,  # noqa: N803 — PySpark param name
    predicates: list[str] | None = None,
    properties: dict[str, str] | None = None,
    *,
    lower_bound: int | None = None,
    upper_bound: int | None = None,
    num_partitions: int | None = None,
    connection_properties: dict[str, str] | None = None,
) -> DataFrame:
    """Read PostgreSQL via the native connector; other drivers refuse.

    pins: io-declared-1/C-003, C-007
    """
    from repark.errors import IllegalArgumentException

    resolved_lower = _jdbc_alias("lowerBound", lowerBound, "lower_bound", lower_bound)
    resolved_upper = _jdbc_alias("upperBound", upperBound, "upper_bound", upper_bound)
    resolved_num = _jdbc_alias("numPartitions", numPartitions, "num_partitions", num_partitions)
    resolved_props = _jdbc_alias(
        "properties", properties, "connection_properties", connection_properties
    )
    props = dict(resolved_props or {})
    dbtable = table
    if dbtable is None:
        for key, value in props.items():
            if key.lower() == "dbtable" and value:
                dbtable = value
                break
    if dbtable is None:
        raise IllegalArgumentException(
            "jdbc requires a table name (or dbtable option) — "
            "use spark.read.jdbc(url, table, properties=...) "
            "or format('postgres').option('dbtable', ...).load()"
        )
    range_parts = [column, resolved_lower, resolved_upper, resolved_num]
    range_set = sum(part is not None for part in range_parts)
    if predicates is not None and range_set > 0:
        raise IllegalArgumentException(
            "jdbc predicates[] cannot be combined with partitionColumn/lowerBound/"
            "upperBound/numPartitions (Spark JDBC mutual exclusion)"
        )
    if range_set not in (0, 4):
        raise IllegalArgumentException(
            "jdbc range partitioning requires column, lowerBound, upperBound, and "
            "numPartitions together (Spark JDBC parity)"
        )
    if predicates is not None and len(predicates) == 0:
        raise IllegalArgumentException("jdbc predicates[] must be non-empty when supplied")
    if not _is_postgres_url(url):
        _refuse("jdbc")
    return reader._session.read_postgres(
        url=url,
        dbtable=dbtable,
        query=None,
        properties=props,
        partition_column=column,
        lower_bound=resolved_lower,
        upper_bound=resolved_upper,
        num_partitions=resolved_num,
        predicates=predicates,
    )


def refuse_reader_load_format(reader: DataFrameReader, source_format: str) -> NoReturn:
    """Refuse a declared reader format at ``load``. pins: io-declared-1/C-002"""
    _ = source_format
    if not _row_tag_provided(None, reader._options):
        _refuse_missing_row_tag()
    _refuse("xml")


def writer_orc(
    writer: DataFrameWriter,
    path: Any,
    mode: str | None = None,
    partitionBy: Any = None,  # noqa: N803 — PySpark param name
    compression: str | None = None,
) -> None:
    """Refuse ORC writes. pins: io-declared-1/C-001"""
    _ = writer, path, mode, partitionBy, compression
    _refuse("orc")


def writer_xml(
    writer: DataFrameWriter,
    path: Any,
    rowTag: Any = None,  # noqa: N803 — PySpark param name
    mode: str | None = None,
    **options: Any,
) -> None:
    """Refuse XML writes after Spark's ``rowTag`` check. pins: io-declared-1/C-002"""
    _ = path, mode, options
    if not _row_tag_provided(rowTag, writer._options):
        _refuse_missing_row_tag()
    _refuse("xml")


def writer_jdbc(
    writer: DataFrameWriter,
    url: str,
    table: str,
    mode: str | None = None,
    properties: dict[str, str] | None = None,
) -> None:
    """Refuse JDBC writes after Spark's save-mode check. pins: io-declared-1/C-003, C-008"""
    _ = writer, url, table, properties
    lowered_mode = mode.lower() if isinstance(mode, str) else mode
    if mode is not None and lowered_mode not in _JDBC_SAVE_MODES:
        _refuse_invalid_jdbc_mode(mode)
    _refuse("jdbc")


def refuse_writer_save_format(writer: DataFrameWriter) -> NoReturn:
    """Refuse a non-path write format at ``save``. pins: io-declared-1/C-001, C-002"""
    source_format = (writer._format or "").strip().lower()
    if source_format == "orc":
        _refuse("orc")
    if source_format == "xml":
        if not _row_tag_provided(None, writer._options):
            _refuse_missing_row_tag()
        _refuse("xml")
    shown = (writer._format or "")[:64]
    raise AnalysisException(
        f"DATA_SOURCE_NOT_FOUND: Failed to find the data source: {shown!r}. "
        "repark path writes support format('parquet'|'csv'|'json') via COPY TO "
        "(orc/other formats are not supported)."
    )
